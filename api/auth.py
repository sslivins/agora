import hmac
from typing import Optional

from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import APIKeyHeader
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from api.config import Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

SESSION_COOKIE = "agora_session"
SESSION_MAX_AGE = 86400  # 24 hours


class WebAuthRequired(Exception):
    """Raised when web UI session auth is needed — triggers redirect to /login."""
    pass


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def _serializer(settings: Settings) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(settings.secret_key)


def get_session_user(request: Request, settings: Settings) -> Optional[str]:
    """Extract username from signed session cookie, or None."""
    cookie = request.cookies.get(SESSION_COOKIE)
    if not cookie:
        return None
    try:
        data = _serializer(settings).loads(cookie, max_age=SESSION_MAX_AGE)
        return data.get("username")
    except (BadSignature, SignatureExpired):
        return None


async def require_auth(
    request: Request,
    api_key: Optional[str] = Depends(api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    """Dependency for API routes: accepts API key header OR session cookie."""
    if api_key:
        # Check CMS-pushed key override first, fall back to boot config
        effective_key = settings.api_key
        override_path = settings.persist_dir / "api_key"
        try:
            override = override_path.read_text().strip()
            if override:
                effective_key = override
        except (FileNotFoundError, OSError):
            pass
        if hmac.compare_digest(api_key, effective_key):
            return "api_key"
    user = get_session_user(request, settings)
    if user:
        return user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )


async def require_web_auth(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> str:
    """Dependency for web UI routes: requires session cookie, raises WebAuthRequired."""
    user = get_session_user(request, settings)
    if user:
        return user
    raise WebAuthRequired()


def create_session(response: Response, username: str, settings: Settings) -> None:
    """Set a signed session cookie on the response."""
    token = _serializer(settings).dumps({"username": username})
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="strict",
    )


def clear_session(response: Response) -> None:
    """Remove the session cookie."""
    response.delete_cookie(SESSION_COOKIE)
