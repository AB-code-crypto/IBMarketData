import sys
from pathlib import Path

# Чтобы скрипт можно было запускать как:
# python scripts/rebuild_job_db.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ib_job_data.rebuild_mid_price import rebuild_instrument_mid_price_features


TARGET = "MNQ"


if __name__ == "__main__":
    rebuild_instrument_mid_price_features(TARGET)
