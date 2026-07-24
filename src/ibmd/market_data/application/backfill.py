from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.market_data import MarketDataContractV1

from .service import MarketDataServiceError, MarketDataShadowService


@dataclass(frozen=True)
class HistoryBackfillResult:
    instrument_id: str
    con_id: int
    local_symbol: str
    start_utc: str
    end_utc: str
    chunk_count: int
    fragment_count: int
    changed_bar_count: int


def iter_history_chunks(
    *,
    start_utc: str,
    end_utc: str,
    chunk_seconds: int,
    bar_duration_seconds: int,
) -> tuple[tuple[str, str], ...]:
    start = parse_utc(start_utc)
    end = parse_utc(end_utc)
    chunk = int(chunk_seconds)
    duration = int(bar_duration_seconds)
    if end <= start:
        raise MarketDataServiceError("history backfill interval must be positive")
    if chunk <= 0 or chunk > 86_400:
        raise MarketDataServiceError(
            f"history chunk_seconds must be in 1..86400: {chunk_seconds}"
        )
    if duration <= 0:
        raise MarketDataServiceError(
            f"bar_duration_seconds must be positive: {bar_duration_seconds}"
        )
    for label, value in (("start", start), ("end", end)):
        if int(value.timestamp()) % duration != 0:
            raise MarketDataServiceError(
                f"history {label}_utc is not aligned to {duration}s: "
                f"{format_utc(value)}"
            )

    values: list[tuple[str, str]] = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(seconds=chunk), end)
        values.append((format_utc(current), format_utc(chunk_end)))
        current = chunk_end
    return tuple(values)


async def backfill_contract_history(
    *,
    service: MarketDataShadowService,
    contract: MarketDataContractV1,
    start_utc: str,
    end_utc: str,
) -> HistoryBackfillResult:
    if contract.instrument_id != service.config.instrument_id:
        raise MarketDataServiceError(
            "history contract belongs to another instrument: "
            f"expected={service.config.instrument_id}, "
            f"actual={contract.instrument_id}"
        )
    chunks = iter_history_chunks(
        start_utc=start_utc,
        end_utc=end_utc,
        chunk_seconds=service.config.history_chunk_seconds,
        bar_duration_seconds=service.config.bar_duration_seconds,
    )
    fragment_count = 0
    changed_count = 0
    try:
        for index, (chunk_start, chunk_end) in enumerate(chunks):
            start = parse_utc(chunk_start)
            end = parse_utc(chunk_end)
            lookback_seconds = int((end - start).total_seconds())
            fragments = await service.reader.fetch_recent_history(
                contract=contract,
                end_at_utc=chunk_end,
                lookback_seconds=lookback_seconds,
                bar_duration_seconds=service.config.bar_duration_seconds,
                use_rth=service.config.use_rth,
            )
            fragment_count += len(fragments)
            changed = await service.submit_fragments(fragments)
            changed_count += len(changed)
            if (
                index + 1 < len(chunks)
                and service.config.history_chunk_spacing_seconds > 0.0
            ):
                await asyncio.sleep(
                    service.config.history_chunk_spacing_seconds
                )
        service.refresh_health(observed_at_utc=format_utc(service.clock()))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await service._record_source_failure(exc)
        raise MarketDataServiceError(
            "history range backfill failed: "
            f"contract={contract.local_symbol}, "
            f"start={start_utc}, end={end_utc}, "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    return HistoryBackfillResult(
        instrument_id=contract.instrument_id,
        con_id=contract.con_id,
        local_symbol=contract.local_symbol,
        start_utc=format_utc(parse_utc(start_utc)),
        end_utc=format_utc(parse_utc(end_utc)),
        chunk_count=len(chunks),
        fragment_count=fragment_count,
        changed_bar_count=changed_count,
    )
