"""Profile management routes: avatar customization."""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from data.child_store import ChildStore
from web.shared import limiter
from web.helpers import (
    AVATAR_ICONS,
    AVATAR_COLORS,
    AudioPreferenceRequest,
    PlaybackPreferenceRequest,
    format_audio_language_priority,
    format_player_mode,
    format_quality_preference,
)

router = APIRouter()


@router.post("/api/avatar")
@limiter.limit("30/minute")
async def update_avatar(request: Request):
    """Update the current profile's avatar icon and/or color."""
    child_id = request.session.get("child_id")
    vs = request.app.state.video_store
    if not child_id or not vs:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    icon = body.get("icon", "")
    color = body.get("color", "")

    if icon and icon not in AVATAR_ICONS:
        return JSONResponse({"error": "invalid icon"}, status_code=400)
    if color and color not in AVATAR_COLORS:
        return JSONResponse({"error": "invalid color"}, status_code=400)

    vs.update_profile_avatar(
        child_id,
        icon=icon if icon else None,
        color=color if color else None,
    )

    if icon:
        request.session["avatar_icon"] = icon
    if color:
        request.session["avatar_color"] = color

    return JSONResponse({"ok": True})


@router.post("/api/audio-preferences")
@limiter.limit("30/minute")
async def update_audio_preferences(request: Request, body: AudioPreferenceRequest):
    """Update the current profile's preferred audio language order."""
    child_id = request.session.get("child_id")
    vs = request.app.state.video_store
    if not child_id or not vs:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    child_store = ChildStore(vs, child_id)
    formatted = format_audio_language_priority(body.priority)
    child_store.set_setting("audio_language_priority", formatted)
    return JSONResponse({"ok": True, "priority": formatted})


@router.post("/api/playback-preferences")
@limiter.limit("30/minute")
async def update_playback_preferences(request: Request, body: PlaybackPreferenceRequest):
    """Update the current profile's playback-related preferences."""
    child_id = request.session.get("child_id")
    vs = request.app.state.video_store
    if not child_id or not vs:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    child_store = ChildStore(vs, child_id)
    priority = format_audio_language_priority(body.priority)
    player_mode = format_player_mode(body.player_mode)
    quality_preference = format_quality_preference(body.quality_preference)
    child_store.set_setting("audio_language_priority", priority)
    child_store.set_setting("player_mode", player_mode)
    child_store.set_setting("quality_preference", quality_preference)
    return JSONResponse({
        "ok": True,
        "priority": priority,
        "player_mode": player_mode,
        "quality_preference": quality_preference,
    })
