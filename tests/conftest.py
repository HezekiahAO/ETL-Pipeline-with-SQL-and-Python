import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


@pytest.fixture(scope="session")
def test_url():
    """Integration tests DROP and recreate all pipeline schemas, so they need a disposable database."""
    url = os.getenv("ETL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set ETL_TEST_DATABASE_URL to a disposable database to run integration tests")
    return url
