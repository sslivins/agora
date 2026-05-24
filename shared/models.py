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
    loop_count: Optional[int] = None  # None = infinite, N = play exactly N times
    expected_checksum: Optional[str] = None  # SHA-256 from CMS schedule
    url: Optional[str] = None  # Webpage URL for Cage+Chromium rendering
    asset_type: Optional[str] = None  # "video", "image", "webpage", "stream"
    # Wall-clock anchor for slideshow playback (agora#226). When set, the
    # player uses ``(now - schedule_anchor_at) mod cycle_duration`` to
    # decide which slide should be on screen, instead of starting from
    # slide 0 on every fresh dispatch. cms_client populates this from
    # the active schedule's ``start_time`` (in the schedule's timezone,
    # normalized to UTC) so the anchor is the same on every device
    # watching the same schedule -- giving free multi-display sync. Old
    # players ignore the field; new players fall back to the manifest's
    # ``started_at`` (and then to legacy timer-chain) when unset.
    schedule_anchor_at: Optional[datetime] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PortStatus(BaseModel):
    """Connection status of a single HDMI port.

    ``connected`` is tri-state: ``True`` (display attached),
    ``False`` (nothing attached), or ``None`` (status not determinable).
    """
    name: str
    connected: Optional[bool] = None


class CurrentState(BaseModel):
    mode: PlaybackMode = PlaybackMode.SPLASH
    asset: Optional[str] = None
    loop: bool = False
    loop_count: Optional[int] = None
    loops_completed: int = 0
    started_at: Optional[datetime] = None
    playback_position_ms: Optional[int] = None
    pipeline_state: str = "NULL"
    display_connected: Optional[bool] = None
    display_ports: Optional[list[PortStatus]] = None
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
