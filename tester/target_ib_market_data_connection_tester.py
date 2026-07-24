from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ib_async.ib import StartupFetchNONE

from ibmd.ib_gateway.ib_async_market_data import (
    IBAsyncMarketDataReader,
    IBMarketDataConnectionSettings,
)
from ibmd.public_contracts.market_data import MarketDataContractV1
from scripts.import_legacy_market_data import _market_data_lock

ACCOUNT_ID = "U0000000"


def contract() -> MarketDataContractV1:
    return MarketDataContractV1(
        instrument_id="MNQ",
        sec_type="FUT",
        symbol="MNQ",
        exchange="CME",
        currency="USD",
        trading_class="MNQ",
        multiplier=2.0,
        con_id=793356225,
        local_symbol="MNQU6",
        last_trade_date="20260918",
    )


class ReadOnlyMarketDataStartupTest(unittest.IsolatedAsyncioTestCase):
    async def test_connection_disables_unrelated_startup_fetches(self) -> None:
        class FakeIB:
            def __init__(self) -> None:
                self.connected = False
                self.connect_kwargs = None

            async def connectAsync(self, **kwargs):
                self.connect_kwargs = dict(kwargs)
                self.connected = True

            def isConnected(self):
                return self.connected

            def managedAccounts(self):
                return [ACCOUNT_ID]

            async def reqHistoricalDataAsync(self, *args, **kwargs):
                _ = args, kwargs
                return []

            def disconnect(self):
                self.connected = False

        fake = FakeIB()
        reader = IBAsyncMarketDataReader(
            IBMarketDataConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=280,
                account_id=ACCOUNT_ID,
                historical_request_spacing_seconds=0.0,
            ),
            ib_factory=lambda: fake,
            clock=lambda: datetime(
                2026,
                7,
                23,
                10,
                1,
                tzinfo=timezone.utc,
            ),
        )
        await reader.fetch_recent_history(
            contract=contract(),
            end_at_utc="2026-07-23T10:01:00Z",
            lookback_seconds=60,
            bar_duration_seconds=5,
            use_rth=False,
        )

        self.assertIsNotNone(fake.connect_kwargs)
        self.assertEqual(fake.connect_kwargs["account"], ACCOUNT_ID)
        self.assertIs(fake.connect_kwargs["readonly"], True)
        self.assertEqual(
            fake.connect_kwargs["fetchFields"],
            StartupFetchNONE,
        )
        await reader.close()


class LegacyImporterLockTest(unittest.TestCase):
    def test_apply_lock_does_not_require_IB_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "IBMD_DEPLOYMENT_ID": "shadow-mnq-account1",
                "IBMD_DATA_ROOT": temp_dir,
            }
            with patch.dict(os.environ, environment, clear=True):
                lock = _market_data_lock()
                with lock:
                    self.assertTrue(
                        (
                            Path(temp_dir)
                            / "runtime"
                            / "locks"
                            / "market_data.lock"
                        ).is_file()
                    )


if __name__ == "__main__":
    unittest.main()
