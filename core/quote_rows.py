from datetime import timezone

from core.time_utils import build_ct_time_fields_from_utc_dt, format_utc


def _empty_quote_row(*, bar_time_ts, bar_time, bar_time_ts_ct, bar_time_ct, contract_name):
    return {
        "bar_time_ts": bar_time_ts,
        "bar_time": bar_time,
        "bar_time_ts_ct": bar_time_ts_ct,
        "bar_time_ct": bar_time_ct,
        "contract": contract_name,
        "ask_open": None,
        "bid_open": None,
        "ask_high": None,
        "bid_high": None,
        "ask_low": None,
        "bid_low": None,
        "ask_close": None,
        "bid_close": None,
        "volume": None,
        "average": None,
        "bar_count": None,
    }


def _get_or_create_quote_row(rows_by_ts, *, bar, contract_name):
    dt = bar.date.astimezone(timezone.utc)
    bar_time_ts = int(dt.timestamp())
    bar_time_ts_ct, bar_time_ct = build_ct_time_fields_from_utc_dt(dt)

    if bar_time_ts not in rows_by_ts:
        rows_by_ts[bar_time_ts] = _empty_quote_row(
            bar_time_ts=bar_time_ts,
            bar_time=format_utc(dt),
            bar_time_ts_ct=bar_time_ts_ct,
            bar_time_ct=bar_time_ct,
            contract_name=contract_name,
        )

    return bar_time_ts, rows_by_ts[bar_time_ts]


def build_quote_rows(bid_bars, ask_bars, contract_name):
    rows_by_ts = {}

    for bar in ask_bars:
        _, row = _get_or_create_quote_row(
            rows_by_ts,
            bar=bar,
            contract_name=contract_name,
        )
        row["ask_open"] = bar.open
        row["ask_high"] = bar.high
        row["ask_low"] = bar.low
        row["ask_close"] = bar.close

    for bar in bid_bars:
        _, row = _get_or_create_quote_row(
            rows_by_ts,
            bar=bar,
            contract_name=contract_name,
        )
        row["bid_open"] = bar.open
        row["bid_high"] = bar.high
        row["bid_low"] = bar.low
        row["bid_close"] = bar.close

    rows = []

    for bar_time_ts in sorted(rows_by_ts.keys()):
        row = rows_by_ts[bar_time_ts]
        rows.append(
            (
                row["bar_time_ts"],
                row["bar_time"],
                row["bar_time_ts_ct"],
                row["bar_time_ct"],
                row["contract"],
                row["ask_open"],
                row["bid_open"],
                row["ask_high"],
                row["bid_high"],
                row["ask_low"],
                row["bid_low"],
                row["ask_close"],
                row["bid_close"],
                row["volume"],
                row["average"],
                row["bar_count"],
            )
        )

    return rows
