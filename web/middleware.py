"""HTTP middleware: security headers + PIN-based profile authentication."""

import logging

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

# API paths safe to access without PIN auth
_API_AUTH_EXEMPT = ("/api/status/", "/api/yt-iframe-api.js", "/api/yt-widget-api.js")
_ROOT_AUTH_EXEMPT = ("/manifest.webmanifest", "/service-worker.js")


class SecurityHeadersMiddleware:
    """Add security headers to all responses."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend([
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-frame-options", b"DENY"),
                    (b"referrer-policy", b"strict-origin-when-cross-origin"),
                    (
                        b"content-security-policy",
                        (
                            "default-src 'self'; "
                            "manifest-src 'self'; "
                            "script-src 'self' 'unsafe-inline'; "
                            "style-src 'self' 'unsafe-inline'; "
                            "img-src 'self' https://ko-fi.com https://i.ytimg.com https://i1.ytimg.com https://i2.ytimg.com "
                            "https://i3.ytimg.com https://i4.ytimg.com https://i9.ytimg.com https://img.youtube.com; "
                            "frame-src https://www.youtube-nocookie.com; "
                            "connect-src 'self'; "
                            "media-src 'self' blob: https://*.googlevideo.com; "
                            "worker-src 'self'; "
                            "object-src 'none'; "
                            "base-uri 'self'"
                        ).encode("latin-1"),
                    ),
                ])
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_headers)


class PinAuthMiddleware:
    """Require profile-based authentication when any profile has a PIN."""

    def __init__(self, app: ASGIApp, pin: str = ""):
        self.app = app
        self.pin = pin  # legacy single-PIN (used for backwards compat check)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)

        # Allow unauthenticated access to login, static assets, and specific read-only APIs
        if request.url.path.startswith(("/login", "/static")):
            await self.app(scope, receive, send)
            return
        if request.url.path in _ROOT_AUTH_EXEMPT:
            await self.app(scope, receive, send)
            return
        if request.url.path.startswith(_API_AUTH_EXEMPT):
            await self.app(scope, receive, send)
            return

        # Profile-based auth: check if child_id is in session
        if request.session.get("child_id"):
            await self.app(scope, receive, send)
            return

        # Auto-login: if only one profile and it has no PIN, set session directly
        vs = getattr(request.app.state, "video_store", None)
        profiles = []
        if vs:
            profiles = vs.get_profiles()
            if len(profiles) == 1 and not profiles[0]["pin"]:
                request.session["child_id"] = profiles[0]["id"]
                request.session["child_name"] = profiles[0]["display_name"]
                request.session["avatar_icon"] = profiles[0].get("avatar_icon") or ""
                request.session["avatar_color"] = profiles[0].get("avatar_color") or ""
                await self.app(scope, receive, send)
                return
            if not profiles:
                # No profiles at all — shouldn't happen after bootstrap, but handle gracefully
                await self.app(scope, receive, send)
                return

        # Legacy: if no profiles exist but PIN auth is disabled
        if not self.pin and (not vs or not profiles):
            await self.app(scope, receive, send)
            return

        # Return JSON 401 for API endpoints instead of redirect
        if request.url.path.startswith("/api/"):
            response: Response = JSONResponse({"error": "unauthorized"}, status_code=401)
        else:
            response = RedirectResponse(url="/login", status_code=303)
        await response(scope, receive, send)
