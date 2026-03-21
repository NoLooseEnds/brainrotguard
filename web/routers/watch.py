"""Watch, pending, status polling, and heartbeat routes."""

import asyncio
import hashlib
import logging
import re
import shutil
import time
from urllib.parse import urljoin, urlsplit

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from web.shared import templates, limiter
from web.deps import get_child_store, get_extractor
from web.helpers import (
    VIDEO_ID_RE, HeartbeatRequest,
    _HEARTBEAT_MIN_INTERVAL, _HEARTBEAT_EVICT_AGE,
    base_ctx, resolve_video_category,
    get_time_limit_info, get_category_time_info,
    get_schedule_info, get_next_start_time,
    format_audio_language_priority,
    format_player_mode,
    format_quality_preference,
    get_external_origin,
)
from web.cache import invalidate_catalog_cache
from i18n import category_label

router = APIRouter()
logger = logging.getLogger(__name__)
_PLAYBACK_CACHE_TTL = 300
_PLAYBACK_SEGMENT_CACHE_TTL = 7200
_ALLOWED_MEDIA_HEADER_NAMES = {
    "accept-ranges",
    "cache-control",
    "content-length",
    "content-range",
    "content-type",
    "etag",
    "last-modified",
}
_MEDIA_PROXY_HOSTS = ("googlevideo.com",)
_SUBTITLE_PROXY_HOSTS = ("youtube.com", "googlevideo.com")
_HLS_URI_ATTR_RE = re.compile(r'URI="([^"]+)"')
_HLS_LANGUAGE_ATTR_RE = re.compile(r'LANGUAGE="([^"]+)"')
_HLS_DEFAULT_ATTR_RE = re.compile(r'DEFAULT=(YES|NO)')
_HLS_AUTOSELECT_ATTR_RE = re.compile(r'AUTOSELECT=(YES|NO)')
_HLS_SUBTITLES_ATTR_RE = re.compile(r',SUBTITLES="[^"]+"')
_UPSTREAM_ORIGIN = "https://www.youtube.com"
_UPSTREAM_REFERER = "https://www.youtube.com/"
_FALLBACK_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)


def _playback_cache_key(video_id: str, audio_priority: str) -> str:
    return f"{video_id}:{audio_priority}"


def _get_cached_playback(state, cache_key: str) -> dict | None:
    cache = getattr(state, "playback_cache", None) or {}
    cached = cache.get(cache_key)
    if not cached:
        return None
    if (time.monotonic() - cached["cached_at"]) > _PLAYBACK_CACHE_TTL:
        cache.pop(cache_key, None)
        return None
    return cached["payload"]


def _set_cached_playback(state, cache_key: str, payload: dict) -> dict:
    cache = getattr(state, "playback_cache", None)
    if cache is None:
        state.playback_cache = {}
        cache = state.playback_cache
    cache[cache_key] = {"cached_at": time.monotonic(), "payload": payload}
    return payload


def _is_allowed_media_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except Exception:
        return False
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not hostname:
        return False
    return any(hostname == suffix or hostname.endswith(f".{suffix}") for suffix in _MEDIA_PROXY_HOSTS)


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _is_allowed_subtitle_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except Exception:
        return False
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not hostname:
        return False
    return any(hostname == suffix or hostname.endswith(f".{suffix}") for suffix in _SUBTITLE_PROXY_HOSTS)


def _select_stream(playback: dict, stream_id: str = "") -> dict | None:
    target_id = stream_id or playback.get("selected_stream_id", "")
    for stream in playback.get("streams", []):
        if stream.get("stream_id") == target_id:
            return stream
    return playback.get("streams", [None])[0]


def _select_hls_audio_track(playback: dict, stream_id: str = "") -> dict | None:
    target_id = stream_id or playback.get("selected_stream_id", "")
    for track in playback.get("audio_tracks", []):
        if track.get("stream_id") == target_id:
            return track
    return playback.get("audio_tracks", [None])[0]


def _select_hls_video_variant(playback: dict, variant_id: str = "") -> dict | None:
    target_id = variant_id or playback.get("video_variants", [{}])[0].get("format_id", "")
    for variant in playback.get("video_variants", []):
        if variant.get("format_id") == target_id:
            return variant
    return playback.get("video_variants", [None])[0]


def _sanitize_playback(playback: dict) -> dict | None:
    if playback.get("mode") == "hls":
        master_manifest_url = playback.get("master_manifest_url", "")
        if not _is_allowed_media_url(master_manifest_url):
            return None
        audio_tracks = []
        for track in playback.get("audio_tracks", []):
            candidate = dict(track)
            if candidate.get("stream_id") and _is_allowed_media_url(candidate.get("url", "")):
                audio_tracks.append(candidate)
        video_variants = []
        for variant in playback.get("video_variants", []):
            candidate = dict(variant)
            if candidate.get("format_id") and _is_allowed_media_url(candidate.get("url", "")):
                video_variants.append(candidate)
        if not audio_tracks or not video_variants:
            return None
        sanitized = dict(playback)
        sanitized["master_manifest_url"] = master_manifest_url
        sanitized["audio_tracks"] = audio_tracks
        sanitized["video_variants"] = video_variants
        selected_audio = _select_hls_audio_track(sanitized)
        if not selected_audio:
            return None
        sanitized["selected_stream_id"] = selected_audio.get("stream_id", "")
        sanitized["selected_language"] = selected_audio.get("language", "")
        language_options = []
        for option in playback.get("language_options", []):
            option_track = None
            for track in audio_tracks:
                if track.get("stream_id") == option.get("stream_id"):
                    option_track = track
                    break
            if not option_track and option.get("code"):
                for track in audio_tracks:
                    if track.get("language") == option.get("code"):
                        option_track = track
                        break
            if not option_track:
                continue
            language_options.append({
                **option,
                "stream_id": option_track.get("stream_id", ""),
                "selected": option_track.get("stream_id") == sanitized["selected_stream_id"],
            })
        sanitized["language_options"] = language_options
        sanitized["subtitle_options"] = [
            dict(option)
            for option in playback.get("subtitle_options", [])
            if option.get("code") and _is_allowed_subtitle_url(option.get("url", ""))
        ]
        return sanitized

    streams = []
    for stream in playback.get("streams", []):
        candidate = dict(stream)
        if candidate.get("mode") == "mux":
            if not _ffmpeg_available():
                continue
            if not (_is_allowed_media_url(candidate.get("video_url", "")) and _is_allowed_media_url(candidate.get("audio_url", ""))):
                continue
            streams.append(candidate)
            continue
        if _is_allowed_media_url(candidate.get("url", "")):
            streams.append(candidate)
    if not streams:
        return None
    sanitized = dict(playback)
    sanitized["streams"] = streams
    selected = _select_stream(sanitized)
    if not selected:
        return None
    sanitized["selected_stream_id"] = selected.get("stream_id", "")
    sanitized["selected_language"] = selected.get("language", "")
    language_options = []
    for option in playback.get("language_options", []):
        option_stream = None
        for stream in streams:
            if stream.get("stream_id") == option.get("stream_id"):
                option_stream = stream
                break
        if not option_stream and option.get("code"):
            for stream in streams:
                if stream.get("language") == option.get("code"):
                    option_stream = stream
                    break
        if not option_stream:
            continue
        language_options.append({
            **option,
            "stream_id": option_stream.get("stream_id", ""),
            "selected": option_stream.get("stream_id") == sanitized["selected_stream_id"],
        })
    sanitized["language_options"] = language_options
    sanitized["subtitle_options"] = [
        dict(option)
        for option in playback.get("subtitle_options", [])
        if option.get("code") and _is_allowed_subtitle_url(option.get("url", ""))
    ]
    return sanitized


async def _get_playback(request: Request, video_id: str, child_store, force_refresh: bool = False) -> dict | None:
    raw_priority = child_store.get_setting("audio_language_priority", "")
    audio_priority = format_audio_language_priority(raw_priority)
    cache_key = _playback_cache_key(video_id, audio_priority)
    state = request.app.state
    if not force_refresh:
        cached = _get_cached_playback(state, cache_key)
        if cached:
            return cached
    extractor = get_extractor(request)
    playback = await extractor.extract_playback(video_id, audio_priority=audio_priority)
    if not playback:
        return None
    sanitized = _sanitize_playback(playback)
    if not sanitized:
        return None
    sanitized["audio_priority"] = audio_priority
    return _set_cached_playback(state, cache_key, sanitized)


async def _close_proxy_resources(upstream: httpx.Response, client: httpx.AsyncClient) -> None:
    await upstream.aclose()
    await client.aclose()


def _build_upstream_headers(
    request: Request | None,
    *,
    accept: str | None = None,
    range_header: str | None = None,
) -> dict:
    headers = {
        "Origin": _UPSTREAM_ORIGIN,
        "Referer": _UPSTREAM_REFERER,
        "User-Agent": (request.headers.get("user-agent") if request else "") or _FALLBACK_USER_AGENT,
        "Accept-Language": (request.headers.get("accept-language") if request else "") or "en-US,en;q=0.9",
    }
    if accept:
        headers["Accept"] = accept
    if range_header:
        headers["Range"] = range_header
    return headers


def _prepare_playback_for_template(state, video_id: str, playback: dict | None) -> dict | None:
    """Attach local subtitle proxy URLs for template use without mutating cache payloads."""
    if not playback:
        return None
    prepared = dict(playback)
    subtitle_options = []
    for option in playback.get("subtitle_options", []):
        url = option.get("url", "")
        if not _is_allowed_subtitle_url(url):
            continue
        token = _cache_segment_url(state, video_id, url)
        subtitle_options.append({
            **option,
            "proxy_url": f"/api/watch-subtitles/{video_id}/{token}",
        })
    prepared["subtitle_options"] = subtitle_options
    return prepared


async def _terminate_process(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def _mux_stream_response(stream: dict) -> StreamingResponse | JSONResponse:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return JSONResponse({"error": "ffmpeg_unavailable"}, status_code=503)
    cmd = [
        ffmpeg,
        "-nostdin",
        "-loglevel", "error",
        "-i", stream["video_url"],
        "-i", stream["audio_url"],
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "copy",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "-f", "mp4",
        "pipe:1",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def iterator():
        try:
            while True:
                chunk = await proc.stdout.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
            stderr = await proc.stderr.read()
            if proc.returncode not in (None, 0):
                logger.error("ffmpeg mux failed for %s: %s", stream.get("stream_id"), stderr.decode(errors="ignore"))
        finally:
            await _terminate_process(proc)

    return StreamingResponse(
        iterator(),
        status_code=200,
        media_type="video/mp4",
        headers={"Cache-Control": "no-store"},
    )


def _authorize_watch_media_request(request: Request, video_id: str):
    if not VIDEO_ID_RE.match(video_id):
        logger.info("Rejected watch media request for invalid video id: %s", video_id)
        return None, None, None, JSONResponse({"error": "invalid"}, status_code=400)
    if request.session.get("watching") != video_id:
        logger.info(
            "Rejected watch media request for %s: not_watching (session watching=%r)",
            video_id,
            request.session.get("watching"),
        )
        return None, None, None, JSONResponse({"error": "not_watching"}, status_code=400)

    state = request.app.state
    wl_cfg = state.wl_config
    cs = get_child_store(request)
    video = cs.get_video(video_id)
    if not video or video["status"] != "approved":
        logger.info("Rejected watch media request for %s: not_approved", video_id)
        return state, cs, None, JSONResponse({"error": "not_approved"}, status_code=404)

    schedule_info = get_schedule_info(store=cs, wl_cfg=wl_cfg)
    if schedule_info and not schedule_info["allowed"]:
        logger.info("Rejected watch media request for %s: outside_schedule", video_id)
        return state, cs, video, JSONResponse({"error": "outside_schedule"}, status_code=403)

    video_cat = resolve_video_category(video, store=cs)
    cat_info = get_category_time_info(store=cs, wl_cfg=wl_cfg)
    if cat_info:
        cat_budget = cat_info["categories"].get(video_cat, {})
        if cat_budget.get("exceeded"):
            logger.info("Rejected watch media request for %s: category_times_up", video_id)
            return state, cs, video, JSONResponse({"error": "times_up"}, status_code=403)
    else:
        time_info = get_time_limit_info(store=cs, wl_cfg=wl_cfg)
        if time_info and time_info["exceeded"]:
            logger.info("Rejected watch media request for %s: times_up", video_id)
            return state, cs, video, JSONResponse({"error": "times_up"}, status_code=403)

    return state, cs, video, None


def _cleanup_segment_cache(state, now: float) -> None:
    cache = getattr(state, "playback_segment_cache", None) or {}
    stale_tokens = [token for token, item in cache.items() if (now - item.get("cached_at", 0.0)) > _PLAYBACK_SEGMENT_CACHE_TTL]
    for token in stale_tokens:
        cache.pop(token, None)
    state.playback_segment_last_cleanup = now


def _cache_segment_url(state, video_id: str, url: str) -> str:
    cache = getattr(state, "playback_segment_cache", None)
    if cache is None:
        state.playback_segment_cache = {}
        cache = state.playback_segment_cache
    now = time.monotonic()
    if (now - getattr(state, "playback_segment_last_cleanup", 0.0)) > 300:
        _cleanup_segment_cache(state, now)
    token = hashlib.sha256(f"{video_id}\0{url}".encode("utf-8")).hexdigest()[:32]
    cache[token] = {"video_id": video_id, "url": url, "cached_at": now}
    return token


def _resolve_segment_url(state, video_id: str, token: str) -> str | None:
    cache = getattr(state, "playback_segment_cache", None) or {}
    item = cache.get(token)
    if not item:
        return None
    if item.get("video_id") != video_id:
        return None
    now = time.monotonic()
    if (now - item.get("cached_at", 0.0)) > _PLAYBACK_SEGMENT_CACHE_TTL:
        cache.pop(token, None)
        return None
    item["cached_at"] = now
    return item.get("url")


def _build_hls_segment_url(state, video_id: str, base_url: str, uri: str) -> str:
    absolute_url = urljoin(base_url, uri)
    if not _is_allowed_media_url(absolute_url):
        raise ValueError(f"disallowed_hls_uri:{absolute_url}")
    token = _cache_segment_url(state, video_id, absolute_url)
    return f"/api/watch-hls/{video_id}/segment/{token}"


def _build_hls_resource_url(state, video_id: str, base_url: str, uri: str) -> str:
    absolute_url = urljoin(base_url, uri)
    if not _is_allowed_media_url(absolute_url):
        raise ValueError(f"disallowed_hls_uri:{absolute_url}")
    token = _cache_segment_url(state, video_id, absolute_url)
    return f"/api/watch-hls/{video_id}/resource/{token}"


def _rewrite_hls_playlist(state, video_id: str, playlist_url: str, body: str) -> str:
    rewritten_lines = []
    trailing_newline = body.endswith("\n")
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            rewritten_lines.append(raw_line)
            continue
        if stripped.startswith("#"):
            def _replace_uri(match: re.Match[str]) -> str:
                rewritten = _build_hls_segment_url(state, video_id, playlist_url, match.group(1))
                return f'URI="{rewritten}"'

            rewritten_lines.append(_HLS_URI_ATTR_RE.sub(_replace_uri, raw_line))
            continue
        rewritten_lines.append(_build_hls_segment_url(state, video_id, playlist_url, stripped))
    rewritten = "\n".join(rewritten_lines)
    if trailing_newline:
        rewritten += "\n"
    return rewritten


def _rewrite_hls_manifest_resource(state, video_id: str, playlist_url: str, body: str) -> str:
    return _rewrite_hls_manifest_resource_with_audio(state, video_id, playlist_url, body, selected_language="")


def _rewrite_hls_manifest_resource_with_audio(state, video_id: str, playlist_url: str, body: str, selected_language: str) -> str:
    rewritten_lines = []
    trailing_newline = body.endswith("\n")
    preferred_lang = (selected_language or "").strip().lower()
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            rewritten_lines.append(raw_line)
            continue
        if stripped.startswith("#"):
            updated_line = raw_line
            if stripped.startswith("#EXT-X-MEDIA:") and ("TYPE=SUBTITLES" in stripped or "TYPE=CLOSED-CAPTIONS" in stripped):
                continue
            if stripped.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in stripped:
                lang_match = _HLS_LANGUAGE_ATTR_RE.search(raw_line)
                lang = (lang_match.group(1).split("-", 1)[0].lower() if lang_match else "")
                is_selected = bool(preferred_lang and lang == preferred_lang)
                if _HLS_DEFAULT_ATTR_RE.search(updated_line):
                    updated_line = _HLS_DEFAULT_ATTR_RE.sub(f'DEFAULT={"YES" if is_selected else "NO"}', updated_line)
                elif is_selected:
                    updated_line = updated_line + ',DEFAULT=YES'
                else:
                    updated_line = updated_line + ',DEFAULT=NO'
                if _HLS_AUTOSELECT_ATTR_RE.search(updated_line):
                    updated_line = _HLS_AUTOSELECT_ATTR_RE.sub('AUTOSELECT=YES', updated_line)
                else:
                    updated_line = updated_line + ',AUTOSELECT=YES'
            elif stripped.startswith("#EXT-X-STREAM-INF:"):
                updated_line = _HLS_SUBTITLES_ATTR_RE.sub("", updated_line)

            def _replace_uri(match: re.Match[str]) -> str:
                rewritten = _build_hls_resource_url(state, video_id, playlist_url, match.group(1))
                return f'URI="{rewritten}"'

            rewritten_lines.append(_HLS_URI_ATTR_RE.sub(_replace_uri, updated_line))
            continue
        rewritten_lines.append(_build_hls_resource_url(state, video_id, playlist_url, stripped))
    rewritten = "\n".join(rewritten_lines)
    if trailing_newline:
        rewritten += "\n"
    return rewritten


def _hls_master_playlist(video_id: str, playback: dict, selected_audio: dict) -> str:
    lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for track in playback.get("audio_tracks", []):
        lang = track.get("language") or "und"
        name = (track.get("label") or lang).replace('"', "'")
        uri = f"/api/watch-hls/{video_id}/audio.m3u8?stream={track.get('stream_id', '')}"
        lines.append(
            '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",'
            f'NAME="{name}",LANGUAGE="{lang}",'
            f'DEFAULT={"YES" if track.get("stream_id") == selected_audio.get("stream_id") else "NO"},'
            'AUTOSELECT=YES,'
            f'URI="{uri}"'
        )
    for variant in playback.get("video_variants", []):
        bandwidth = int(max((variant.get("tbr") or 0) * 1000, 1))
        attrs = [f"BANDWIDTH={bandwidth}", 'AUDIO="audio"']
        width = int(variant.get("width") or 0)
        height = int(variant.get("height") or 0)
        if width and height:
            attrs.append(f"RESOLUTION={width}x{height}")
        codecs = [codec for codec in (variant.get("vcodec"), selected_audio.get("acodec")) if codec and codec != "none"]
        if codecs:
            attrs.append(f'CODECS="{",".join(codecs)}"')
        lines.append(f"#EXT-X-STREAM-INF:{','.join(attrs)}")
        lines.append(f"/api/watch-hls/{video_id}/video.m3u8?variant={variant.get('format_id', '')}")
    return "\n".join(lines) + "\n"


async def _fetch_upstream_playlist(request: Request, url: str) -> tuple[int, dict, str] | None:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
        ) as client:
            response = await client.get(
                url,
                headers=_build_upstream_headers(
                    request,
                    accept="application/vnd.apple.mpegurl, application/x-mpegURL, text/plain, */*",
                ),
            )
            response.raise_for_status()
            return response.status_code, dict(response.headers), response.text
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch HLS playlist %s: %s", url, exc)
        return None


async def _fetch_hls_playlist_body(request: Request, url: str) -> str | None:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
        ) as client:
            response = await client.get(
                url,
                headers=_build_upstream_headers(
                    request,
                    accept="application/vnd.apple.mpegurl, application/x-mpegURL, text/plain, */*",
                ),
            )
            response.raise_for_status()
            return response.text
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch HLS playlist %s: %s", url, exc)
        return None


async def _open_proxy_stream(
    request: Request,
    url: str,
    headers: dict | None = None,
) -> tuple[httpx.AsyncClient, httpx.Response]:
    client = httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0),
    )
    try:
        merged_headers = _build_upstream_headers(request)
        if headers:
            merged_headers.update(headers)
        upstream = await client.send(client.build_request("GET", url, headers=merged_headers), stream=True)
        return client, upstream
    except Exception:
        await client.aclose()
        raise


@router.get("/pending/{video_id}", response_class=HTMLResponse)
async def pending_video(request: Request, video_id: str):
    """Waiting screen with polling."""
    if not VIDEO_ID_RE.match(video_id):
        return RedirectResponse(url="/", status_code=303)
    cs = get_child_store(request)
    video = cs.get_video(video_id)

    if not video:
        return RedirectResponse(url="/", status_code=303)

    if video["status"] == "approved":
        return RedirectResponse(url=f"/watch/{video_id}", status_code=303)
    elif video["status"] == "denied":
        return templates.TemplateResponse(request, "denied.html", {
            **base_ctx(request),
            "video": video,
        })
    else:
        w_cfg = request.app.state.web_config
        poll_interval = w_cfg.poll_interval if w_cfg else 3000
        return templates.TemplateResponse(request, "pending.html", {
            **base_ctx(request),
            "video": video,
            "poll_interval": poll_interval,
        })


@router.get("/watch/{video_id}", response_class=HTMLResponse)
async def watch_video(request: Request, video_id: str):
    """Play approved video (embed)."""
    if not VIDEO_ID_RE.match(video_id):
        return RedirectResponse(url="/", status_code=303)
    state = request.app.state
    wl_cfg = state.wl_config
    cs = get_child_store(request)
    video = cs.get_video(video_id)

    if not video:
        # Video not in DB -- auto-approve if channel is allowlisted
        extractor = get_extractor(request)
        metadata = await extractor.extract_metadata(video_id)
        if not metadata:
            return RedirectResponse(url="/", status_code=303)
        if not cs.is_channel_allowed(metadata['channel_name'],
                                     channel_id=metadata.get('channel_id') or ""):
            return RedirectResponse(url="/", status_code=303)
        cs.add_video(
            video_id=metadata['video_id'],
            title=metadata['title'],
            channel_name=metadata['channel_name'],
            thumbnail_url=metadata.get('thumbnail_url'),
            duration=metadata.get('duration'),
            channel_id=metadata.get('channel_id'),
            is_short=metadata.get('is_short', False),
            yt_view_count=metadata.get('view_count'),
        )
        cs.update_status(video_id, "approved")
        invalidate_catalog_cache(state)
        video = cs.get_video(video_id)

    if not video or video["status"] != "approved":
        return RedirectResponse(url="/", status_code=303)

    video_cat = resolve_video_category(video, store=cs)
    locale = getattr(request.app.state, "locale", "en")
    cat_label = category_label(video_cat, locale)
    cat_info = get_category_time_info(store=cs, wl_cfg=wl_cfg)
    base = base_ctx(request)
    time_info = None
    if cat_info:
        cat_budget = cat_info["categories"].get(video_cat, {})
        if cat_budget.get("exceeded"):
            available = []
            for c, info in cat_info["categories"].items():
                if not info["exceeded"] and c != video_cat:
                    c_label = category_label(c, locale)
                    available.append({"name": c, "label": c_label, "remaining_min": info["remaining_min"]})
            return templates.TemplateResponse(request, "timesup.html", {
                **base,
                "time_info": cat_budget,
                "category": cat_label,
                "available_categories": available,
                "next_start": get_next_start_time(store=cs, wl_cfg=wl_cfg),
            })
        if cat_budget.get("limit_min", 0) > 0:
            time_info = cat_budget
    else:
        time_info = get_time_limit_info(store=cs, wl_cfg=wl_cfg)
        if time_info and time_info["exceeded"]:
            return templates.TemplateResponse(request, "timesup.html", {
                **base,
                "time_info": time_info,
                "next_start": get_next_start_time(store=cs, wl_cfg=wl_cfg),
            })

    schedule_info = get_schedule_info(store=cs, wl_cfg=wl_cfg)
    if schedule_info and not schedule_info["allowed"]:
        return templates.TemplateResponse(request, "outsidehours.html", {
            **base,
            "schedule_info": schedule_info,
        })

    cs.record_view(video_id)
    request.session["watching"] = video_id

    embed_url = f"https://www.youtube-nocookie.com/embed/{video_id}?enablejsapi=1"
    player_mode = format_player_mode(cs.get_setting("player_mode", ""))
    quality_preference = format_quality_preference(cs.get_setting("quality_preference", ""))
    playback = None
    if player_mode != "embed":
        playback = await _get_playback(request, video_id, cs)
        playback = _prepare_playback_for_template(state, video_id, playback)

    return templates.TemplateResponse(request, "watch.html", {
        **base,
        "video": video,
        "embed_url": embed_url,
        "player_origin": get_external_origin(request),
        "time_info": time_info,
        "schedule_info": schedule_info,
        "video_cat": video_cat,
        "cat_label": cat_label,
        "is_short": bool(video.get("is_short")),
        "profile_id": cs.profile_id,
        "player_mode": player_mode,
        "quality_preference": quality_preference,
        "playback": playback,
    })


@router.get("/api/status/{video_id}")
@limiter.limit("30/minute")
async def api_status(request: Request, video_id: str):
    """JSON status endpoint for polling."""
    if not VIDEO_ID_RE.match(video_id):
        return JSONResponse({"status": "not_found"})

    vs = request.app.state.video_store
    profile_id = request.session.get("child_id", "default")
    video = vs.get_video(video_id, profile_id=profile_id) if vs else None

    if not video:
        return JSONResponse({"status": "not_found"})

    return JSONResponse({"status": video["status"]})


@router.get("/api/watch-preload/{video_id}")
@limiter.limit("60/minute")
async def watch_preload(request: Request, video_id: str):
    """Warm playback cache for likely-next videos so watch pages open faster."""
    if not VIDEO_ID_RE.match(video_id):
        return JSONResponse({"error": "invalid"}, status_code=400)
    cs = get_child_store(request)
    video = cs.get_video(video_id)
    if not video or video["status"] != "approved":
        return JSONResponse({"ok": False}, status_code=200)
    if format_player_mode(cs.get_setting("player_mode", "")) != "embed":
        await _get_playback(request, video_id, cs)
    return JSONResponse({"ok": True})


@router.post("/api/watch-heartbeat")
@limiter.limit("30/minute")
async def watch_heartbeat(request: Request, body: HeartbeatRequest):
    """Log playback seconds and return remaining budget."""
    vid = body.video_id
    seconds = min(max(body.seconds, 0), 60)  # clamp 0-60

    if not VIDEO_ID_RE.match(vid):
        return JSONResponse({"error": "invalid"}, status_code=400)

    # Verify heartbeat matches the video currently being watched in this session
    if request.session.get("watching") != vid:
        return JSONResponse({"error": "not_watching"}, status_code=400)

    # Verify the video exists and is approved before accepting heartbeat
    state = request.app.state
    wl_cfg = state.wl_config
    cs = get_child_store(request)
    video = cs.get_video(vid)
    if not video or video["status"] != "approved":
        return JSONResponse({"error": "not_approved"}, status_code=400)

    # Check schedule window
    schedule_info = get_schedule_info(store=cs, wl_cfg=wl_cfg)
    if schedule_info and not schedule_info["allowed"]:
        return JSONResponse({"error": "outside_schedule"}, status_code=403)

    # Clamp seconds to 0 if heartbeat arrives faster than expected interval
    now = time.monotonic()
    last_hb = state.last_heartbeat
    profile_id = cs.profile_id
    hb_key = (vid, profile_id)
    last = last_hb.get(hb_key, 0.0)
    if last and (now - last) < _HEARTBEAT_MIN_INTERVAL:
        seconds = 0
    last_hb[hb_key] = now

    # Periodic cleanup: evict stale entries to prevent unbounded growth
    if now - state.heartbeat_last_cleanup > _HEARTBEAT_EVICT_AGE:
        state.heartbeat_last_cleanup = now
        stale = [k for k, t in last_hb.items() if now - t > _HEARTBEAT_EVICT_AGE]
        for k in stale:
            del last_hb[k]

    if seconds > 0:
        cs.record_watch_seconds(vid, seconds)
    if body.position_seconds is not None:
        cs.update_playback_position(vid, body.position_seconds)

    # Per-category time limit check
    video_cat = resolve_video_category(video, store=cs) if video else "fun"
    cat_info = get_category_time_info(store=cs, wl_cfg=wl_cfg)
    remaining = -1
    time_limit_cb = state.time_limit_notify_cb
    if cat_info:
        cat_budget = cat_info["categories"].get(video_cat, {})
        if cat_budget.get("limit_min", 0) > 0:
            remaining = cat_budget.get("remaining_sec", -1)
        if cat_budget.get("exceeded") and time_limit_cb:
            await time_limit_cb(cat_budget["used_min"], cat_budget["limit_min"], video_cat, profile_id)
    else:
        time_info = get_time_limit_info(store=cs, wl_cfg=wl_cfg)
        remaining = time_info["remaining_sec"] if time_info else -1
        if time_info and time_info["exceeded"] and time_limit_cb:
            await time_limit_cb(time_info["used_min"], time_info["limit_min"], "", profile_id)

    return JSONResponse({"remaining": remaining})


@router.get("/api/watch-media/{video_id}")
async def watch_media(request: Request, video_id: str, stream: str = Query("", max_length=80)):
    """Proxy the selected video stream through the app to keep playback session-bound."""
    state, cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    range_header = request.headers.get("range")
    for attempt in range(2):
        playback = await _get_playback(request, video_id, cs, force_refresh=bool(attempt))
        if not playback:
            return JSONResponse({"error": "playback_unavailable"}, status_code=404)
        if playback.get("mode") == "hls":
            return JSONResponse({"error": "adaptive_only"}, status_code=409)
        selected_stream = _select_stream(playback, stream)
        if not selected_stream:
            return JSONResponse({"error": "stream_not_found"}, status_code=404)
        if selected_stream.get("mode") == "mux":
            return await _mux_stream_response(selected_stream)

        headers = {}
        if range_header:
            headers["Range"] = range_header
        try:
            client, upstream = await _open_proxy_stream(request, selected_stream["url"], headers=headers)
        except httpx.HTTPError:
            if attempt == 0:
                continue
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

        if upstream.status_code in (403, 404) and attempt == 0:
            await upstream.aclose()
            await client.aclose()
            continue
        if upstream.status_code >= 400:
            await upstream.aclose()
            await client.aclose()
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

        response_headers = {
            name: value
            for name, value in upstream.headers.items()
            if name.lower() in _ALLOWED_MEDIA_HEADER_NAMES
        }
        response_headers["Cache-Control"] = "no-store"
        return StreamingResponse(
            upstream.aiter_bytes(),
            status_code=upstream.status_code,
            headers=response_headers,
            background=BackgroundTask(_close_proxy_resources, upstream, client),
        )

    return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)


@router.get("/api/watch-hls/{video_id}/master.m3u8")
async def watch_hls_master(request: Request, video_id: str, audio: str = Query("", max_length=80)):
    """Serve a local HLS master playlist that points to app-proxied media playlists."""
    state, cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    playback = await _get_playback(request, video_id, cs)
    if not playback or playback.get("mode") != "hls":
        return JSONResponse({"error": "playback_unavailable"}, status_code=404)
    selected_audio = _select_hls_audio_track(playback, audio)
    if not selected_audio:
        return JSONResponse({"error": "stream_not_found"}, status_code=404)
    master_url = playback.get("master_manifest_url", "")
    fetched = await _fetch_upstream_playlist(request, master_url)
    if not fetched:
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
    _status_code, _headers, body = fetched
    try:
        body = _rewrite_hls_manifest_resource_with_audio(
            state,
            video_id,
            master_url,
            body,
            selected_audio.get("language", ""),
        )
    except ValueError as exc:
        logger.warning("Rejected HLS master playlist for %s: %s", video_id, exc)
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
    return Response(
        content=body,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/api/watch-hls/{video_id}/audio.m3u8")
async def watch_hls_audio_playlist(request: Request, video_id: str, stream: str = Query("", max_length=80)):
    """Serve a rewritten local HLS audio media playlist."""
    state, cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    for attempt in range(2):
        playback = await _get_playback(request, video_id, cs, force_refresh=bool(attempt))
        if not playback or playback.get("mode") != "hls":
            return JSONResponse({"error": "playback_unavailable"}, status_code=404)
        selected_audio = _select_hls_audio_track(playback, stream)
        if not selected_audio:
            return JSONResponse({"error": "stream_not_found"}, status_code=404)
        body = await _fetch_hls_playlist_body(request, selected_audio.get("url", ""))
        if body is None:
            if attempt == 0:
                continue
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        try:
            rewritten = _rewrite_hls_playlist(state, video_id, selected_audio.get("url", ""), body)
        except ValueError as exc:
            logger.warning("Rejected HLS audio playlist for %s: %s", video_id, exc)
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)


@router.get("/api/watch-hls/{video_id}/video.m3u8")
async def watch_hls_video_playlist(request: Request, video_id: str, variant: str = Query("", max_length=80)):
    """Serve a rewritten local HLS video media playlist."""
    state, cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    for attempt in range(2):
        playback = await _get_playback(request, video_id, cs, force_refresh=bool(attempt))
        if not playback or playback.get("mode") != "hls":
            return JSONResponse({"error": "playback_unavailable"}, status_code=404)
        selected_variant = _select_hls_video_variant(playback, variant)
        if not selected_variant:
            return JSONResponse({"error": "stream_not_found"}, status_code=404)
        body = await _fetch_hls_playlist_body(request, selected_variant.get("url", ""))
        if body is None:
            if attempt == 0:
                continue
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        try:
            rewritten = _rewrite_hls_playlist(state, video_id, selected_variant.get("url", ""), body)
        except ValueError as exc:
            logger.warning("Rejected HLS video playlist for %s: %s", video_id, exc)
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)


@router.get("/api/watch-hls/{video_id}/resource/{token}")
async def watch_hls_resource(request: Request, video_id: str, token: str):
    """Proxy a playlist/segment resource from the original HLS manifest graph."""
    state, _cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    upstream_url = _resolve_segment_url(state, video_id, token)
    if not upstream_url or not _is_allowed_media_url(upstream_url):
        return JSONResponse({"error": "segment_not_found"}, status_code=404)

    if upstream_url.endswith(".m3u8") or "/manifest/" in upstream_url:
        fetched = await _fetch_upstream_playlist(request, upstream_url)
        if not fetched:
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        _status_code, _headers, body = fetched
        try:
            rewritten = _rewrite_hls_manifest_resource(state, video_id, upstream_url, body)
        except ValueError as exc:
            logger.warning("Rejected HLS nested playlist for %s: %s", video_id, exc)
            return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    range_header = request.headers.get("range")
    headers = {}
    if range_header:
        headers["Range"] = range_header
    try:
        client, upstream = await _open_proxy_stream(request, upstream_url, headers=headers)
    except httpx.HTTPError:
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

    response_headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() in _ALLOWED_MEDIA_HEADER_NAMES
    }
    response_headers["Cache-Control"] = "no-store"
    return StreamingResponse(
        upstream.aiter_bytes(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_proxy_resources, upstream, client),
    )


@router.get("/api/watch-hls/{video_id}/segment/{token}")
async def watch_hls_segment(request: Request, video_id: str, token: str):
    """Proxy a single HLS segment or sidecar resource through the app."""
    state, _cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    upstream_url = _resolve_segment_url(state, video_id, token)
    if not upstream_url or not _is_allowed_media_url(upstream_url):
        return JSONResponse({"error": "segment_not_found"}, status_code=404)

    range_header = request.headers.get("range")
    headers = {}
    if range_header:
        headers["Range"] = range_header
    try:
        client, upstream = await _open_proxy_stream(request, upstream_url, headers=headers)
    except httpx.HTTPError:
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        return JSONResponse({"error": "stream_fetch_failed"}, status_code=502)

    response_headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() in _ALLOWED_MEDIA_HEADER_NAMES
    }
    response_headers["Cache-Control"] = "no-store"
    return StreamingResponse(
        upstream.aiter_bytes(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_proxy_resources, upstream, client),
    )


@router.get("/api/watch-subtitles/{video_id}/{token}")
async def watch_subtitles(request: Request, video_id: str, token: str):
    """Proxy WebVTT subtitles through the app so caption URLs stay session-bound."""
    state, _cs, _video, error = _authorize_watch_media_request(request, video_id)
    if error:
        return error

    upstream_url = _resolve_segment_url(state, video_id, token)
    if not upstream_url or not _is_allowed_subtitle_url(upstream_url):
        return JSONResponse({"error": "subtitle_not_found"}, status_code=404)

    try:
        client, upstream = await _open_proxy_stream(request, upstream_url)
    except httpx.HTTPError:
        return JSONResponse({"error": "subtitle_fetch_failed"}, status_code=502)

    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        return JSONResponse({"error": "subtitle_fetch_failed"}, status_code=502)

    response_headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() in _ALLOWED_MEDIA_HEADER_NAMES
    }
    response_headers["Cache-Control"] = "no-store"
    return StreamingResponse(
        upstream.aiter_bytes(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_proxy_resources, upstream, client),
    )
