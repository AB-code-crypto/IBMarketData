from __future__ import annotations

import hashlib

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.market_data import (
    MarketBarV1,
    MarketSideBarObservationV1,
    QuoteSide,
)


class MarketBarAssemblyError(ValueError):
    pass


def deterministic_market_bar_id(
    *,
    instrument_id: str,
    con_id: int,
    bar_start_utc: str,
    bar_duration_seconds: int,
) -> str:
    canonical = (
        f"{instrument_id}\x1f{int(con_id)}\x1f"
        f"{format_utc(parse_utc(bar_start_utc))}\x1f"
        f"{int(bar_duration_seconds)}"
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    return f"market_bar_{digest}"


def assemble_complete_market_bar(
    *,
    bid: MarketSideBarObservationV1,
    ask: MarketSideBarObservationV1,
    revision: int,
    first_published_at_utc: str | None = None,
) -> MarketBarV1:
    if bid.side != QuoteSide.BID or ask.side != QuoteSide.ASK:
        raise MarketBarAssemblyError(
            "assembler requires BID and ASK fragments in explicit roles"
        )
    if bid.natural_key != ask.natural_key:
        raise MarketBarAssemblyError(
            "BID and ASK fragments do not describe the same market bar"
        )
    if bid.local_symbol != ask.local_symbol:
        raise MarketBarAssemblyError(
            "BID and ASK fragments have different contract identity"
        )
    if bid.source_generation_id != ask.source_generation_id:
        raise MarketBarAssemblyError(
            "BID and ASK fragments belong to different source generations"
        )
    if bid.source_session_id != ask.source_session_id:
        raise MarketBarAssemblyError(
            "BID and ASK fragments belong to different IB sessions"
        )
    if bid.source_kind != ask.source_kind:
        raise MarketBarAssemblyError(
            "BID and ASK fragments have different source kinds"
        )

    published = max(
        parse_utc(bid.observed_at_utc),
        parse_utc(ask.observed_at_utc),
    )
    first = (
        published
        if first_published_at_utc is None
        else parse_utc(first_published_at_utc)
    )
    if published < first:
        published = first

    return MarketBarV1(
        bar_id=deterministic_market_bar_id(
            instrument_id=bid.instrument_id,
            con_id=bid.con_id,
            bar_start_utc=bid.bar_start_utc,
            bar_duration_seconds=bid.bar_duration_seconds,
        ),
        instrument_id=bid.instrument_id,
        con_id=bid.con_id,
        local_symbol=bid.local_symbol,
        bar_start_utc=bid.bar_start_utc,
        bar_end_utc=bid.bar_end_utc,
        bar_duration_seconds=bid.bar_duration_seconds,
        bid_open=bid.open_price,
        bid_high=bid.high_price,
        bid_low=bid.low_price,
        bid_close=bid.close_price,
        ask_open=ask.open_price,
        ask_high=ask.high_price,
        ask_low=ask.low_price,
        ask_close=ask.close_price,
        source_kind=bid.source_kind,
        first_published_at_utc=format_utc(first),
        published_at_utc=format_utc(published),
        revision=revision,
        complete=True,
        volume=None,
        average=None,
        bar_count=None,
    )
