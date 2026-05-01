from dataclasses import dataclass
from typing import Optional
import asyncio


@dataclass
class RecentBackfillState:
    first_bid_ts: Optional[int] = None
    first_ask_ts: Optional[int] = None
    last_backfill_completed_sync_ts: Optional[int] = None
    backfill_task: Optional[asyncio.Task] = None


@dataclass
class RealtimeMonitorState:
    last_bar_monotonic: Optional[float] = None
    last_bar_time_ts: Optional[int] = None
    last_bar_stream: Optional[str] = None
    last_ok_telegram_monotonic: Optional[float] = None
    last_stall_warning_monotonic: Optional[float] = None
    last_restore_monotonic: Optional[float] = None
