from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class PlaybackMode(str, enum.Enum):
    PLAY = "play"
    STOP = "stop"
    SPLASH = "splash"


class DesiredState(BaseModel):
    mode: PlaybackMode = PlaybackMode.SPLASH
    asset: Optional[str] = None
    loop: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CurrentState(BaseModel):
    mode: PlaybackMode = PlaybackMode.SPLASH
    asset: Optional[str] = None
    loop: bool = False
    started_at: Optional[datetime] = None
    pipeline_state: str = "NULL"
    error: Optional[str] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AssetInfo(BaseModel):
    name: str
    size: int
    modified_at: datetime
    asset_type: str  # "video", "image", or "splash"


class PlayRequest(BaseModel):
    asset: str
    loop: bool = False


class HealthResponse(BaseModel):
    status: str = "ok"
    device_name: str
    version: str = ""
    uptime_seconds: float


class StatusResponse(BaseModel):
    device_name: str
    current_state: CurrentState
    desired_state: DesiredState
    asset_count: int
    schedule_hash: str = ""
