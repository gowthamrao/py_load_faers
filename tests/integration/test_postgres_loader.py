# -*- coding: utf-8 -*-
# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Integration tests for the PostgresLoader.
"""
import zipfile
from pathlib import Path

import pytest
from psycopg.rows import dict_row
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from py_load_faers.cli import app
from py_load_faers.config import DatabaseSettings
from py_load_faers.postgres.loader import PostgresLoader

# Mark all tests in this module as 'integration'
pytestmark = pytest.mark.integration

SAMPLE_DEMO_DATA = """primaryid$caseid$fda_dt
1001$1$20250101
1002$2$20250102
"""

SAMPLE_DRUG_DATA = """primaryid$caseid$drug_seq$drugname
1001$1$1$Aspirin
1001$1$2$Lisinopril
1002$2$1$Metformin
"""


@pytest.fixture
def sample_faers_zip(tmp_path: Path) -> Path:
    """Creates a sample FAERS quarter zip file for testing."""
    zip_path = tmp_path / "faers_ascii_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("DEMO25Q1.txt", SAMPLE_DEMO_DATA)
        zf.writestr("DRUG25Q1.txt", SAMPLE_DRUG_DATA)
    return zip_path


def test_postgres_loader_initialize_schema(
    db_settings: DatabaseSettings,
    clean_db,
) -> None:
    """
    Test that the database schema is correctly initialized by the test fixtures.
    """
    loader = PostgresLoader(db_settings)

    # The `clean_db` fixture should have already initialized the schema.
    # We just need to connect and verify.
    loader.connect()
    assert loader.conn is not None

    # Verify that the tables were created
    expected_tables = {
        "demo",
        "drug",
        "reac",
        "outc",
        "rpsr",
        "ther",
        "indi",
        "_faers_load_history",
    }

    with loader.conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """
        )
        tables_in_db = {row["table_name"] for row in cur.fetchall()}

    assert expected_tables.issubset(tables_in_db)

    # Verify a column in a table to be extra sure
    with loader.conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'demo' AND column_name = 'caseid'
        """
        )
        assert cur.fetchone() is not None

    loader.conn.close()


def test_run_command_end_to_end(
    db_settings: DatabaseSettings,
    sample_faers_zip: Path,
    mocker: MockerFixture,
    clean_db,
) -> None:
    """
    Test the full end-to-end 'run' command, from download to database load.
    """
    # Mock the downloader where it's used: in the engine module.
    mocker.patch(
        "py_load_faers.engine.download_quarter",
        return_value=(sample_faers_zip, "dummy"),
    )

    # Use CliRunner to invoke the CLI commands
    runner = CliRunner()

    # Set up environment variables for the db connection
    env = {
        "PY_LOAD_FAERS_DB__HOST": db_settings.host,
        "PY_LOAD_FAERS_DB__PORT": str(db_settings.port),
        "PY_LOAD_FAERS_DB__USER": db_settings.user,
        "PY_LOAD_FAERS_DB__PASSWORD": db_settings.password,
        "PY_LOAD_FAERS_DB__DBNAME": db_settings.dbname,
    }

    # 1. Initialize the schema using the new db-init command
    result_init = runner.invoke(app, ["db-init"], env=env)
    assert result_init.exit_code == 0

    # 2. Run the ETL process
    result_run = runner.invoke(app, ["run", "--quarter", "2025q1"], env=env)
    assert result_run.exit_code == 0

    # 3. Verify the data was loaded correctly
    loader = PostgresLoader(db_settings)
    loader.connect()
    assert loader.conn is not None

    with loader.conn.cursor(row_factory=dict_row) as cur:
        # Check demo table
        cur.execute("SELECT COUNT(*) FROM demo")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 2
        cur.execute("SELECT caseid FROM demo WHERE primaryid = '1001'")
        case_res = cur.fetchone()
        assert case_res is not None
        assert case_res["caseid"] == "1"

        # Check drug table
        cur.execute("SELECT COUNT(*) FROM drug")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 3
        cur.execute("SELECT drugname FROM drug WHERE primaryid = '1002'")
        drug_res = cur.fetchone()
        assert drug_res is not None
        assert drug_res["drugname"] == "METFORMIN"
    loader.conn.close()


@pytest.fixture
def realistic_faers_zip(tmp_path: Path) -> Path:
    """Creates a sample FAERS quarter zip file from the realistic XML file."""
    zip_path = tmp_path / "faers_xml_2025q2.zip"
    xml_content = Path("tests/integration/test_data/realistic_faers.xml").read_bytes()
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("realistic_faers.xml", xml_content)
    return zip_path


def test_data_quality_check_passes_and_fails(
    db_settings: DatabaseSettings,
    realistic_faers_zip: Path,
    mocker: MockerFixture,
    clean_db,
) -> None:
    """
    Tests the data quality check command.
    1. Loads good data and asserts the check passes.
    2. Injects a duplicate and asserts the check fails.
    """
    mocker.patch(
        "py_load_faers.engine.download_quarter",
        return_value=(realistic_faers_zip, "dummy"),
    )
    mocker.patch("py_load_faers.engine.find_latest_quarter", return_value="2025q2")

    env = {
        "PY_LOAD_FAERS_DB__HOST": db_settings.host,
        "PY_LOAD_FAERS_DB__PORT": str(db_settings.port),
        "PY_LOAD_FAERS_DB__USER": db_settings.user,
        "PY_LOAD_FAERS_DB__PASSWORD": db_settings.password,
        "PY_LOAD_FAERS_DB__DBNAME": db_settings.dbname,
    }

    runner = CliRunner()

    # 1. Init DB and run the load
    assert runner.invoke(app, ["db-init"], env=env).exit_code == 0
    # Use delta mode to trigger the load
    result_run = runner.invoke(app, ["run", "--mode", "delta"], env=env)
    assert result_run.exit_code == 0
    assert "DQ Check Passed" in result_run.stdout

    # 2. Manually verify that the check passes
    result_verify_pass = runner.invoke(app, ["db-verify"], env=env)
    assert result_verify_pass.exit_code == 0
    assert "DQ Check Passed" in result_verify_pass.stdout

    # 3. Inject a duplicate record to make the check fail
    loader = PostgresLoader(db_settings)
    loader.connect()
    assert loader.conn is not None

    with loader.conn.cursor(row_factory=dict_row) as cur:
        # Get an existing record
        cur.execute("SELECT * FROM demo LIMIT 1;")
        record_to_duplicate = cur.fetchone()
        assert record_to_duplicate is not None
        # Insert it with a new primaryid but the same caseid
        cur.execute(
            "INSERT INTO demo (primaryid, caseid, fda_dt) VALUES (%s, %s, %s);",
            (
                "DUPLICATE-PRIMARYID",
                record_to_duplicate["caseid"],
                record_to_duplicate["fda_dt"],
            ),
        )
        loader.commit()
    loader.conn.close()

    # 4. Manually verify that the check now fails
    result_verify_fail = runner.invoke(app, ["db-verify"], env=env)
    assert result_verify_fail.exit_code == 1
    assert "DQ Check FAILED" in result_verify_fail.stdout
