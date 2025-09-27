# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import zipfile
from pathlib import Path

import psycopg
import pytest
from py_load_faers.config import (
    AppSettings,
    DatabaseSettings,
    DownloaderSettings,
    ProcessingSettings,
)
from py_load_faers.engine import FaersLoaderEngine
from py_load_faers.postgres.loader import PostgresLoader
from pytest_mock import MockerFixture

pytestmark = pytest.mark.integration


@pytest.fixture
def realistic_xml_zip(tmp_path: Path) -> Path:
    """Creates a zip file containing the realistic FAERS XML test data."""
    xml_file_path = Path(__file__).parent / "test_data/realistic_faers.xml"
    xml_content = xml_file_path.read_text()

    zip_path = tmp_path / "faers_xml_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("faers_2025q1.xml", xml_content)

    return zip_path


def test_full_xml_load_via_parquet_staging(
    db_settings: DatabaseSettings, realistic_xml_zip: Path, mocker: MockerFixture, clean_db
) -> None:
    """
    This integration test verifies the end-to-end XML loading process when
    using Parquet as the intermediate staging format. The final data in the
    database should be identical to a CSV-based load.
    """
    mocker.patch(
        "py_load_faers.engine.download_quarter",
        return_value=(realistic_xml_zip, "dummy_checksum_parquet"),
    )

    # Key change for this test: Set staging_format to 'parquet'
    config = AppSettings(
        db=db_settings,
        downloader=DownloaderSettings(
            download_dir=str(realistic_xml_zip.parent), retries=3, timeout=60
        ),
        processing=ProcessingSettings(staging_format="parquet", chunk_size=500000),
    )

    db_loader = PostgresLoader(config.db)
    db_loader.connect()
    assert db_loader.conn is not None

    engine = FaersLoaderEngine(config, db_loader)
    engine.run_load(quarter="2025q1")

    # The assertions should be identical to the CSV test, as the final state
    # of the database should not depend on the intermediate format.
    with db_loader.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Verify the final state of the data
        cur.execute("SELECT * FROM demo ORDER BY primaryid")
        final_demo_records = cur.fetchall()
        assert (
            len(final_demo_records) == 1
        ), "Should only be one record in the demo table after deduplication"

        loaded_case = final_demo_records[0]
        assert loaded_case["caseid"] == "102"
        assert loaded_case["primaryid"] == "V4"
        assert loaded_case["reporter_country"] == "CA"

        # 2. Verify that case 101 is completely gone
        cur.execute("SELECT COUNT(*) FROM demo WHERE caseid = '101'")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 0, "Case 101 should have been deleted due to nullification"

        # 3. Verify data in a child table
        cur.execute("SELECT * FROM drug WHERE primaryid = 'V4'")
        drug_records = cur.fetchall()
        assert len(drug_records) == 1
        assert drug_records[0]["drugname"] == "DRUG B"

        # 4. Verify the load history metadata
        cur.execute("SELECT * FROM _faers_load_history")
        meta_res = cur.fetchone()
        assert meta_res is not None, "Load history metadata should exist"
        assert meta_res["status"] == "SUCCESS"
        assert meta_res["quarter"] == "2025q1"
        # The engine should report the logical deletion of the nullified case (case 101)
        # The deletion happens based on caseid, so it's 1 logical case.
        assert (
            meta_res["rows_deleted"] == 0
        ), "Nullification now happens before DB insertion, so no rows are deleted from DB"

    if db_loader.conn:
        db_loader.conn.close()
