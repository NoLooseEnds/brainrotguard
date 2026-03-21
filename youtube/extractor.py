from __future__ import annotations

import asyncio
import logging
import re
from copy import deepcopy
from typing import Optional, Protocol, runtime_checkable
from urllib.parse import parse_qs, urlparse
import yt_dlp

logger = logging.getLogger(__name__)

# Allowlisted YouTube thumbnail CDN hostnames (single source of truth)
THUMB_ALLOWED_HOSTS = frozenset({
    "i.ytimg.com", "i1.ytimg.com", "i2.ytimg.com", "i3.ytimg.com",
    "i4.ytimg.com", "i9.ytimg.com", "img.youtube.com",
})

# Regex to extract video ID from various YouTube URL formats
YOUTUBE_URL_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)([a-zA-Z0-9_-]{11})'
)

_VIDEO_ID_RE = re.compile(r'^[a-zA-Z0-9_-]{11}$')

_SHORTS_PATH_RE = re.compile(r'/shorts/')
_AUDIO_PRIORITY_DEFAULT = ("no", "sv", "en")
_LANGUAGE_ALIASES = {
    "nb": "no",
    "nn": "no",
    "nob": "no",
    "nno": "no",
    "nor": "no",
    "norsk": "no",
    "norwegian": "no",
    "sv-se": "sv",
    "swe": "sv",
    "swedish": "sv",
    "svenska": "sv",
    "en-us": "en",
    "en-gb": "en",
    "eng": "en",
    "english": "en",
}


def _is_short_url(url: Optional[str]) -> bool:
    """Check if a YouTube URL indicates a Short (contains /shorts/ in path)."""
    return bool(url and _SHORTS_PATH_RE.search(url))


def _safe_thumbnail(url: Optional[str], video_id: str) -> str:
    """Return the thumbnail URL if it's from an allowlisted host, else use ytimg fallback."""
    if url:
        try:
            parsed = urlparse(url)
            if parsed.scheme == "https" and parsed.hostname in THUMB_ALLOWED_HOSTS:
                return url
        except Exception:
            pass
    if video_id and _VIDEO_ID_RE.match(video_id):
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return ""


def extract_video_id(url_or_id: str) -> Optional[str]:
    """Extract YouTube video ID from URL or return as-is if already an ID."""
    url_or_id = url_or_id.strip()
    match = YOUTUBE_URL_PATTERN.search(url_or_id)
    if match:
        return match.group(1)
    # Check if it's already a valid video ID (11 chars, alphanumeric + _ -)
    if re.match(r'^[a-zA-Z0-9_-]{11}$', url_or_id):
        return url_or_id
    return None


def normalize_audio_language(value: Optional[str]) -> str:
    """Normalize user-entered or yt-dlp language values to a stable match key."""
    if not value:
        return ""
    normalized = re.sub(r"[^a-z0-9_-]+", "", value.strip().lower().replace("_", "-"))
    if not normalized:
        return ""
    normalized = _LANGUAGE_ALIASES.get(normalized, normalized)
    if normalized in _LANGUAGE_ALIASES:
        normalized = _LANGUAGE_ALIASES[normalized]
    base = normalized.split("-", 1)[0]
    return _LANGUAGE_ALIASES.get(base, base)


def parse_audio_language_priority(raw: Optional[str], default: tuple[str, ...] = _AUDIO_PRIORITY_DEFAULT) -> list[str]:
    """Parse a comma-separated language priority list into normalized unique codes."""
    values = [normalize_audio_language(part) for part in (raw or "").split(",")]
    seen = set()
    parsed = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            parsed.append(value)
    if parsed:
        return parsed
    return [normalize_audio_language(code) for code in default if normalize_audio_language(code)]


def _language_matches(preferred: str, actual: Optional[str]) -> bool:
    """Return True when a stream language satisfies a preferred language bucket."""
    actual_norm = normalize_audio_language(actual)
    return bool(preferred and actual_norm and preferred == actual_norm)


def _pick_preferred_stream(streams: list[dict], priorities: list[str]) -> Optional[dict]:
    """Select the best available stream, preferring configured languages first."""
    ranked_priorities = priorities or list(_AUDIO_PRIORITY_DEFAULT)
    for preferred in ranked_priorities:
        candidates = [stream for stream in streams if _language_matches(preferred, stream.get("language"))]
        if candidates:
            return candidates[0]
    english_candidates = [stream for stream in streams if _language_matches("en", stream.get("language"))]
    if english_candidates:
        return english_candidates[0]
    return streams[0] if streams else None


def _build_subtitle_options(info: dict) -> list[dict]:
    """Extract browser-playable subtitle tracks from yt-dlp metadata."""
    picked: dict[str, tuple[tuple[int, int, str], dict]] = {}
    for source_key, is_auto in (("subtitles", False), ("automatic_captions", True)):
        source = info.get(source_key) or {}
        if not isinstance(source, dict):
            continue
        for language_key, entries in source.items():
            normalized = normalize_audio_language(language_key) or (language_key or "").strip().lower()
            if not normalized or not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                url = entry.get("url")
                ext = (entry.get("ext") or "").strip().lower()
                if not url or ext != "vtt":
                    continue
                parsed_url = urlparse(url)
                if parse_qs(parsed_url.query).get("tlang"):
                    # Skip translated auto-caption variants. They explode the option list and are prone to rate limits.
                    continue
                label = entry.get("name") or normalized.upper()
                option = {
                    "code": normalized,
                    "label": f"{label} (auto)" if is_auto else label,
                    "url": url,
                    "ext": ext,
                    "auto": is_auto,
                }
                # Prefer manual subtitles, then VTT, then lexicographically stable URL.
                score = (1 if is_auto else 0, 0 if ext == "vtt" else 1, url)
                existing = picked.get(normalized)
                if existing is None or score < existing[0]:
                    picked[normalized] = (score, option)
    return [
        picked[key][1]
        for key in sorted(picked)
    ]


def _container_name(ext: str, acodec: str = "") -> str:
    """Normalize format/container naming so audio/video compatibility stays simple."""
    if ext in ("mp4", "m4a") or acodec.startswith("mp4a"):
        return "mp4"
    if ext == "webm" or acodec in ("opus", "vorbis"):
        return "webm"
    return ext


def _audio_is_compatible(video_stream: dict, audio_stream: dict) -> bool:
    """Return True when ffmpeg can stream-copy the chosen audio with the chosen video container."""
    return video_stream.get("container") == audio_stream.get("container")


def _build_hls_playback_payload(video_id: str, formats: list[dict], raw_priority: Optional[str]) -> Optional[dict]:
    """Build adaptive HLS playback info when yt-dlp exposes HLS video variants + audio tracks."""
    raw_hls_audio_tracks = []
    raw_hls_video_variants = []
    for fmt in formats:
        if not isinstance(fmt, dict) or (fmt.get("protocol") or "").lower() != "m3u8_native":
            continue
        url = fmt.get("url")
        if not url:
            continue
        language = normalize_audio_language(fmt.get("language"))
        base = {
            "format_id": str(fmt.get("format_id") or ""),
            "label": fmt.get("format_note") or fmt.get("format") or (fmt.get("ext") or "").upper(),
            "url": url,
            "manifest_url": fmt.get("manifest_url") or url,
            "ext": (fmt.get("ext") or "").lower(),
            "tbr": fmt.get("tbr") or 0,
            "width": fmt.get("width") or 0,
            "height": fmt.get("height") or 0,
            "vcodec": fmt.get("vcodec") or "",
            "acodec": fmt.get("acodec") or "",
        }
        if fmt.get("vcodec") not in (None, "none") and not language:
            raw_hls_video_variants.append(base)
        elif language:
            raw_hls_audio_tracks.append({
                **base,
                "language": language,
                "stream_id": f"{base['format_id']}:{language}",
            })

    if not raw_hls_video_variants or not raw_hls_audio_tracks:
        return None

    hls_audio_tracks_by_language: dict[str, dict] = {}
    for track in sorted(
        raw_hls_audio_tracks,
        key=lambda item: (
            -(item.get("tbr") or 0),
            item.get("format_id") or "",
        ),
    ):
        lang = track.get("language") or "und"
        hls_audio_tracks_by_language.setdefault(lang, track)
    hls_audio_tracks = sorted(
        hls_audio_tracks_by_language.values(),
        key=lambda item: (
            item.get("language") or "zzz",
            item.get("format_id") or "",
        ),
    )
    hls_video_variants = sorted(
        raw_hls_video_variants,
        key=lambda item: (
            item.get("tbr") or 0,
            item.get("height") or 0,
            item.get("format_id") or "",
        ),
    )

    priority = parse_audio_language_priority(raw_priority)
    selected_audio = _pick_preferred_stream(hls_audio_tracks, priority)
    language_map: dict[str, dict] = {}
    for track in hls_audio_tracks:
        lang = track.get("language") or "und"
        language_map.setdefault(lang, {
            "code": lang,
            "label": track.get("label") or lang,
            "stream_id": track.get("stream_id") or "",
            "selected": bool(selected_audio and selected_audio.get("language") == track.get("language")),
        })

    return {
        "video_id": video_id,
        "mode": "hls",
        "master_manifest_url": selected_audio.get("manifest_url") if selected_audio else (hls_video_variants[0].get("manifest_url") if hls_video_variants else ""),
        "priority": priority,
        "selected_stream_id": selected_audio.get("stream_id") if selected_audio else "",
        "selected_language": selected_audio.get("language") if selected_audio else "",
        "audio_tracks": hls_audio_tracks,
        "video_variants": hls_video_variants,
        "language_options": sorted(
            language_map.values(),
            key=lambda option: (
                priority.index(option["code"]) if option["code"] in priority else len(priority),
                option["code"],
            ),
        ),
        "subtitle_options": [],
    }


def _build_playback_payload(video_id: str, info: dict, raw_priority: Optional[str] = None) -> Optional[dict]:
    """Convert a yt-dlp info dict into a compact playback manifest for the web UI."""
    subtitle_options = _build_subtitle_options(info)
    formats = info.get("formats") or []
    hls_payload = _build_hls_playback_payload(video_id, formats, raw_priority)
    if hls_payload:
        hls_payload["subtitle_options"] = subtitle_options
        return hls_payload

    direct_streams: list[dict] = []
    audio_streams: list[dict] = []
    video_streams: list[dict] = []
    for fmt in formats:
        if not isinstance(fmt, dict):
            continue
        url = fmt.get("url")
        if not url:
            continue
        protocol = (fmt.get("protocol") or "").lower()
        if protocol not in ("http", "https"):
            continue
        ext = (fmt.get("ext") or "").lower()
        acodec = fmt.get("acodec") or "none"
        vcodec = fmt.get("vcodec") or "none"
        language = normalize_audio_language(fmt.get("language"))
        container = _container_name(ext, acodec=acodec)
        base_stream = {
            "format_id": str(fmt.get("format_id") or ""),
            "language": language,
            "label": fmt.get("format_note") or fmt.get("format") or ext.upper(),
            "width": fmt.get("width") or 0,
            "height": fmt.get("height") or 0,
            "ext": ext,
            "container": container,
            "url": url,
            "abr": fmt.get("abr") or fmt.get("tbr") or 0,
            "acodec": acodec,
            "vcodec": vcodec,
        }
        if acodec != "none" and vcodec != "none" and ext in ("mp4", "webm"):
            direct_streams.append({
                **base_stream,
                "stream_id": f"{base_stream['format_id'] or ext}:{language or 'und'}",
                "mode": "direct",
            })
        elif acodec != "none" and vcodec == "none" and ext in ("m4a", "mp4", "webm"):
            audio_streams.append(base_stream)
        elif vcodec != "none" and acodec == "none" and ext in ("mp4", "webm"):
            video_streams.append(base_stream)

    streams: list[dict] = []
    if video_streams and audio_streams:
        ranked_video_streams = sorted(
            video_streams,
            key=lambda item: (
                -sum(1 for audio in audio_streams if _audio_is_compatible(item, audio)),
                0 if item.get("container") == "mp4" else 1,
                -(item.get("height") or 0),
                -(item.get("width") or 0),
                item.get("format_id") or "",
            ),
        )
        best_video = ranked_video_streams[0]
        audio_by_language: dict[str, dict] = {}
        for audio in sorted(
            audio_streams,
            key=lambda item: (
                0 if _audio_is_compatible(best_video, item) else 1,
                -(item.get("abr") or 0),
                item.get("format_id") or "",
            ),
        ):
            if not _audio_is_compatible(best_video, audio):
                continue
            lang = audio.get("language") or "und"
            audio_by_language.setdefault(lang, audio)
        for lang, audio in audio_by_language.items():
            streams.append({
                "stream_id": f"{best_video.get('format_id') or best_video.get('ext')}+{audio.get('format_id') or audio.get('ext')}:{lang}",
                "format_id": f"{best_video.get('format_id') or ''}+{audio.get('format_id') or ''}",
                "language": lang,
                "label": audio.get("label") or lang,
                "width": best_video.get("width") or 0,
                "height": best_video.get("height") or 0,
                "ext": best_video.get("ext") or "",
                "container": best_video.get("container") or "",
                "mode": "mux",
                "video_url": best_video.get("url") or "",
                "audio_url": audio.get("url") or "",
                "video_format_id": best_video.get("format_id") or "",
                "audio_format_id": audio.get("format_id") or "",
            })

    if not streams:
        streams = direct_streams

    if not streams:
        return None

    streams.sort(key=lambda item: (
        -(item.get("height") or 0),
        -(item.get("width") or 0),
        item.get("language") or "zzz",
        item.get("stream_id") or "",
    ))
    priority = parse_audio_language_priority(raw_priority)
    selected = _pick_preferred_stream(streams, priority)
    language_map: dict[str, dict] = {}
    for stream in streams:
        lang = stream.get("language") or "und"
        language_map.setdefault(lang, {
            "code": lang,
            "label": stream.get("label") or lang,
            "stream_id": stream.get("stream_id") or "",
            "selected": bool(selected and selected.get("language") == stream.get("language")),
        })
    language_options = sorted(
        language_map.values(),
        key=lambda option: (
            priority.index(option["code"]) if option["code"] in priority else len(priority),
            option["code"],
        ),
    )

    return {
        "video_id": video_id,
        "mode": streams[0].get("mode", "direct"),
        "title": info.get("title", "Unknown"),
        "duration": info.get("duration"),
        "priority": priority,
        "selected_stream_id": selected.get("stream_id") if selected else "",
        "selected_language": selected.get("language") if selected else "",
        "streams": streams,
        "language_options": language_options,
        "subtitle_options": subtitle_options,
    }

_YDL_TIMEOUT = 30  # default; overridden by configure_timeout()


def configure_timeout(seconds: int):
    """Set yt-dlp timeout from config."""
    global _YDL_TIMEOUT
    _YDL_TIMEOUT = seconds


def _ydl_opts() -> dict:
    """Common yt-dlp options - no download, just metadata."""
    return {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'skip_download': True,
        'ignore_no_formats_error': True,
        'socket_timeout': _YDL_TIMEOUT,
    }


def _ydl_playback_opts() -> dict:
    """yt-dlp options tuned for multi-audio playback discovery."""
    opts = deepcopy(_ydl_opts())
    opts['extractor_args'] = {
        'youtube': {
            'player_client': ['ios', 'android_vr'],
            'formats': ['duplicate', 'missing_pot'],
        }
    }
    return opts


async def extract_metadata(video_id: str) -> Optional[dict]:
    """Extract metadata for a single YouTube video."""
    def _extract():
        try:
            with yt_dlp.YoutubeDL(_ydl_opts()) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                if not info:
                    return None
                return {
                    'video_id': video_id,
                    'title': info.get('title', 'Unknown'),
                    'channel_name': info.get('channel', info.get('uploader', 'Unknown')),
                    'channel_id': info.get('channel_id'),
                    'thumbnail_url': _safe_thumbnail(info.get('thumbnail'), video_id),
                    'duration': info.get('duration'),
                    'view_count': info.get('view_count'),
                    'is_short': _is_short_url(info.get('webpage_url')),
                }
        except Exception as e:
            logger.error(f"Failed to extract metadata for {video_id}: {e}")
            return None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_extract), timeout=_YDL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"Metadata extraction timed out for {video_id}")
        return None


async def extract_playback(video_id: str, audio_priority: Optional[str] = None) -> Optional[dict]:
    """Extract playable progressive streams for local HTML5 playback."""
    def _extract():
        try:
            with yt_dlp.YoutubeDL(_ydl_playback_opts()) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                if not info:
                    return None
                return _build_playback_payload(video_id, info, raw_priority=audio_priority)
        except Exception as e:
            logger.error(f"Failed to extract playback info for {video_id}: {e}")
            return None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_extract), timeout=_YDL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"Playback extraction timed out for {video_id}")
        return None

async def search(query: str, max_results: int = 10) -> list[dict]:
    """Search YouTube via yt-dlp ytsearch."""
    def _search():
        try:
            opts = _ydl_opts()
            opts['extract_flat'] = True
            with yt_dlp.YoutubeDL(opts) as ydl:
                results = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
                if not results or 'entries' not in results:
                    return []
                videos = []
                for entry in results['entries']:
                    if not entry:
                        continue
                    vid_id = entry.get('id')
                    if not vid_id or not _VIDEO_ID_RE.match(vid_id):
                        continue  # skip channels/playlists mixed into search results
                    videos.append({
                        'video_id': vid_id,
                        'title': entry.get('title', 'Unknown'),
                        'channel_name': entry.get('channel', entry.get('uploader', 'Unknown')),
                        'thumbnail_url': _safe_thumbnail(entry.get('thumbnail'), vid_id),
                        'duration': entry.get('duration'),
                        'view_count': entry.get('view_count'),
                        'is_short': _is_short_url(entry.get('url')),
                    })
                return videos
        except Exception as e:
            logger.error(f"Search failed for '{query}': {e}")
            return []
    try:
        return await asyncio.wait_for(asyncio.to_thread(_search), timeout=_YDL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"Search timed out for '{query}'")
        return []

async def resolve_channel_handle(handle: str) -> Optional[dict]:
    """Resolve a @handle to channel name, ID, and handle. Returns dict or None."""
    clean = handle.lstrip("@")
    url = f"https://www.youtube.com/@{clean}"
    def _resolve():
        try:
            opts = _ydl_opts()
            opts['extract_flat'] = True
            opts['playlistend'] = 1
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None
                return {
                    'channel_name': info.get('channel', info.get('uploader', clean)),
                    'channel_id': info.get('channel_id') or info.get('id'),
                    'handle': f"@{clean}",
                }
        except Exception as e:
            logger.debug(f"Handle resolve failed for '@{clean}': {e}")
            return None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_resolve), timeout=_YDL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"Handle resolve timed out for '@{clean}'")
        return None


async def resolve_handle_from_channel_id(channel_id: str) -> Optional[str]:
    """Resolve a channel_id to its @handle. Returns '@handle' string or None."""
    def _resolve():
        try:
            opts = _ydl_opts()
            opts['extract_flat'] = True
            opts['playlistend'] = 1
            url = f"https://www.youtube.com/channel/{channel_id}/videos"
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None
                uploader_id = info.get('uploader_id', '')
                if uploader_id and uploader_id.startswith('@'):
                    return uploader_id
                channel_url = info.get('channel_url', '') or info.get('uploader_url', '')
                if '/@' in channel_url:
                    return '@' + channel_url.split('/@', 1)[1].split('/')[0]
                return None
        except Exception as e:
            logger.debug(f"Handle resolve failed for channel {channel_id}: {e}")
            return None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_resolve), timeout=_YDL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"Handle resolve timed out for channel {channel_id}")
        return None


def _resolve_channel_id(channel_name: str) -> Optional[str]:
    """Resolve a channel display name to a YouTube channel ID via search."""
    from urllib.parse import quote
    try:
        opts = _ydl_opts()
        opts['extract_flat'] = 'in_playlist'
        opts['playlistend'] = 5
        url = f"https://www.youtube.com/results?search_query={quote(channel_name)}&sp=EgIQAg%3D%3D"
        with yt_dlp.YoutubeDL(opts) as ydl:
            results = ydl.extract_info(url, download=False)
            for entry in (results or {}).get('entries', []):
                if not entry:
                    continue
                entry_name = entry.get('channel', entry.get('title', ''))
                if entry_name.lower() == channel_name.lower():
                    return entry.get('id') or entry.get('channel_id')
    except Exception as e:
        logger.debug(f"Channel ID resolve failed for '{channel_name}': {e}")
    return None


def _fetch_from_channel_page(channel_id: str, channel_name: str, max_results: int) -> list[dict]:
    """Fetch videos directly from a channel's uploads tab."""
    try:
        opts = _ydl_opts()
        opts['extract_flat'] = True
        opts['playlistend'] = max_results
        url = f"https://www.youtube.com/channel/{channel_id}/videos"
        with yt_dlp.YoutubeDL(opts) as ydl:
            results = ydl.extract_info(url, download=False)
            if not results or 'entries' not in results:
                return []
            # Channel name from playlist metadata (entries lack it in flat mode)
            resolved_name = results.get('channel', results.get('uploader', channel_name))
            videos = []
            for entry in results['entries']:
                if not entry:
                    continue
                vid_id = entry.get('id')
                if not vid_id or not _VIDEO_ID_RE.match(vid_id):
                    continue
                videos.append({
                    'video_id': vid_id,
                    'title': entry.get('title', 'Unknown'),
                    'channel_name': resolved_name,
                    'thumbnail_url': _safe_thumbnail(entry.get('thumbnail'), vid_id),
                    'duration': entry.get('duration'),
                    'timestamp': entry.get('timestamp'),
                    'view_count': entry.get('view_count'),
                    'is_short': _is_short_url(entry.get('url')),
                })
            return videos
    except Exception as e:
        logger.debug(f"Channel page fetch failed for '{channel_id}': {e}")
        return []


async def fetch_channel_videos(channel_name: str, max_results: int = 10, channel_id: Optional[str] = None) -> list[dict]:
    """Fetch recent videos from a YouTube channel.

    If channel_id is provided, fetches directly from the uploads tab.
    Otherwise resolves via search first. Falls back to ytsearch with name filtering.
    """
    def _fetch():
        # Try direct channel page approach first
        cid = channel_id or _resolve_channel_id(channel_name)
        if cid:
            videos = _fetch_from_channel_page(cid, channel_name, max_results)
            if videos:
                return videos

        # Fallback: search and filter by exact channel name
        try:
            fetch_count = max_results * 3
            opts = _ydl_opts()
            opts['extract_flat'] = True
            with yt_dlp.YoutubeDL(opts) as ydl:
                results = ydl.extract_info(f"ytsearch{fetch_count}:{channel_name}", download=False)
                if not results or 'entries' not in results:
                    return []
                videos = []
                for entry in results['entries']:
                    if not entry:
                        continue
                    vid_id = entry.get('id')
                    if not vid_id:
                        continue
                    entry_channel = entry.get('channel', entry.get('uploader', ''))
                    if entry_channel.lower() != channel_name.lower():
                        continue
                    videos.append({
                        'video_id': vid_id,
                        'title': entry.get('title', 'Unknown'),
                        'channel_name': entry_channel,
                        'thumbnail_url': _safe_thumbnail(entry.get('thumbnail'), vid_id),
                        'duration': entry.get('duration'),
                        'timestamp': entry.get('timestamp'),
                        })
                    if len(videos) >= max_results:
                        break
                return videos
        except Exception as e:
            logger.error(f"Channel fetch failed for '{channel_name}': {e}")
            return []
    try:
        return await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=_YDL_TIMEOUT * 2)
    except asyncio.TimeoutError:
        logger.error(f"Channel fetch timed out for '{channel_name}'")
        return []


def _fetch_from_channel_shorts(channel_id: str, channel_name: str, max_results: int) -> list[dict]:
    """Fetch Shorts directly from a channel's /shorts tab."""
    try:
        opts = _ydl_opts()
        opts['extract_flat'] = True
        opts['playlistend'] = max_results
        url = f"https://www.youtube.com/channel/{channel_id}/shorts"
        with yt_dlp.YoutubeDL(opts) as ydl:
            results = ydl.extract_info(url, download=False)
            if not results or 'entries' not in results:
                return []
            resolved_name = results.get('channel', results.get('uploader', channel_name))
            videos = []
            for entry in results['entries']:
                if not entry:
                    continue
                vid_id = entry.get('id')
                if not vid_id or not _VIDEO_ID_RE.match(vid_id):
                    continue
                videos.append({
                    'video_id': vid_id,
                    'title': entry.get('title', 'Unknown'),
                    'channel_name': resolved_name,
                    'thumbnail_url': _safe_thumbnail(entry.get('thumbnail'), vid_id),
                    'duration': entry.get('duration'),
                    'timestamp': entry.get('timestamp'),
                    'view_count': entry.get('view_count'),
                    'is_short': True,
                })
            return videos
    except Exception as e:
        logger.debug(f"Channel shorts fetch failed for '{channel_id}': {e}")
        return []


async def fetch_channel_shorts(channel_name: str, max_results: int = 50, channel_id: Optional[str] = None) -> list[dict]:
    """Fetch recent Shorts from a YouTube channel's /shorts tab."""
    if not channel_id:
        return []
    def _fetch():
        return _fetch_from_channel_shorts(channel_id, channel_name, max_results)
    try:
        return await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=_YDL_TIMEOUT * 2)
    except asyncio.TimeoutError:
        logger.error(f"Channel shorts fetch timed out for '{channel_name}'")
        return []


def format_duration(seconds) -> str:
    """Format seconds into human readable duration like '5:23' or '1:02:15'."""
    if not seconds:
        return "?"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# ---------------------------------------------------------------------------
# Class wrapper + Protocol for dependency injection / mocking
# ---------------------------------------------------------------------------

@runtime_checkable
class YouTubeExtractorProtocol(Protocol):
    """Protocol for YouTube metadata extraction — use for type hints and test mocks."""

    async def extract_metadata(self, video_id: str) -> Optional[dict]: ...
    async def extract_playback(self, video_id: str, audio_priority: Optional[str] = None) -> Optional[dict]: ...
    async def search(self, query: str, max_results: int = 10) -> list[dict]: ...
    async def fetch_channel_videos(self, channel_name: str, max_results: int = 10,
                                    channel_id: Optional[str] = None) -> list[dict]: ...
    async def fetch_channel_shorts(self, channel_name: str, max_results: int = 50,
                                    channel_id: Optional[str] = None) -> list[dict]: ...
    async def resolve_channel_handle(self, handle: str) -> Optional[dict]: ...
    async def resolve_handle_from_channel_id(self, channel_id: str) -> Optional[str]: ...


class YouTubeExtractor:
    """Concrete implementation wrapping yt-dlp — satisfies YouTubeExtractorProtocol.

    Thin namespace wrapper that delegates to the module-level functions.
    Timeout is configured globally via configure_timeout().
    """

    async def extract_metadata(self, video_id: str) -> Optional[dict]:
        return await extract_metadata(video_id)

    async def extract_playback(self, video_id: str, audio_priority: Optional[str] = None) -> Optional[dict]:
        return await extract_playback(video_id, audio_priority=audio_priority)

    async def search(self, query: str, max_results: int = 10) -> list[dict]:
        return await search(query, max_results=max_results)

    async def fetch_channel_videos(self, channel_name: str, max_results: int = 10,
                                    channel_id: Optional[str] = None) -> list[dict]:
        return await fetch_channel_videos(channel_name, max_results=max_results,
                                           channel_id=channel_id)

    async def fetch_channel_shorts(self, channel_name: str, max_results: int = 50,
                                    channel_id: Optional[str] = None) -> list[dict]:
        return await fetch_channel_shorts(channel_name, max_results=max_results,
                                           channel_id=channel_id)

    async def resolve_channel_handle(self, handle: str) -> Optional[dict]:
        return await resolve_channel_handle(handle)

    async def resolve_handle_from_channel_id(self, channel_id: str) -> Optional[str]:
        return await resolve_handle_from_channel_id(channel_id)
