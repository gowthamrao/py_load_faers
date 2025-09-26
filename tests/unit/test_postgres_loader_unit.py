# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Unit tests for the PostgresLoader.
"""
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from pydantic import BaseModel

from py_load_faers.config import DatabaseSettings
from py_load_faers.exceptions import DataQualityError
from py_load_faers.postgres.loader import PostgresLoader


@pytest.fixture
def db_settings() -> DatabaseSettings:
    """Provides mock database settings."""
    return DatabaseSettings(host="host", user="user", password="pw", dbname="db")


@pytest.fixture
def loader(db_settings: DatabaseSettings) -> PostgresLoader:
    """Provides a PostgresLoader instance with a mocked connection."""
    loader = PostgresLoader(db_settings)
    loader.conn = MagicMock()
    return loader


def test_connect_failure(db_settings: DatabaseSettings) -> None:
    """Test that a connection failure raises a psycopg.Error."""
    with patch("psycopg.connect", side_effect=psycopg.OperationalError("Connection failed")):
        loader = PostgresLoader(db_settings)
        with pytest.raises(psycopg.Error):
            loader.connect()


def test_methods_require_connection(db_settings: DatabaseSettings) -> None:
    """Test that methods raise ConnectionError if the connection is not available."""
    loader = PostgresLoader(db_settings)
    loader.conn = None  # Ensure no connection

    with pytest.raises(ConnectionError):
        loader.initialize_schema({})
    with pytest.raises(ConnectionError):
        loader.execute_native_bulk_load("demo", Path("test.csv"))
    with pytest.raises(ConnectionError):
        loader.execute_deletions(["1"])
    with pytest.raises(ConnectionError):
        loader.handle_delta_merge([], {})
    with pytest.raises(ConnectionError):
        loader.update_load_history({})
    with pytest.raises(ConnectionError):
        loader.get_last_successful_load()
    with pytest.raises(ConnectionError):
        loader.run_post_load_dq_checks()


def test_no_op_methods_with_no_connection(db_settings: DatabaseSettings) -> None:
    """Test that transaction methods do not fail if the connection is not available."""
    loader = PostgresLoader(db_settings)
    loader.conn = None
    # These methods should not raise an error, just do nothing
    loader.begin_transaction()
    loader.commit()
    loader.rollback()


def test_initialize_schema_with_drop(loader: PostgresLoader) -> None:
    """Test schema initialization with the drop_existing flag."""

    class MockModel(BaseModel):
        id: int

    mock_cursor = MagicMock()
    loader.conn.cursor.return_value.__enter__.return_value = mock_cursor

    loader.initialize_schema({"mock": MockModel}, drop_existing=True)

    # Check that DROP TABLE was called for the model table and the history table
    drop_calls = [
        call for call in mock_cursor.execute.call_args_list if "DROP TABLE" in call[0][0]
    ]
    assert len(drop_calls) == 2
    assert "DROP TABLE IF EXISTS _faers_load_history CASCADE;" in str(drop_calls)
    assert "DROP TABLE IF EXISTS mock CASCADE;" in str(drop_calls)


def test_generate_ddl_with_optional_type(loader: PostgresLoader) -> None:
    """Test DDL generation for a model with an Optional (Union) type."""

    class ModelWithOptional(BaseModel):
        optional_field: Optional[str]

    ddl = loader._generate_create_table_ddl("test_optional", ModelWithOptional)
    # The type should resolve to TEXT because it's nullable
    assert '"optional_field" TEXT NULL' in ddl


def test_execute_native_bulk_load_unsupported_format(
    loader: PostgresLoader, tmp_path: Path
) -> None:
    """Test that bulk loading an unsupported file format raises ValueError."""
    unsupported_file = tmp_path / "test.txt"
    unsupported_file.write_text("dummy content")
    with pytest.raises(ValueError, match="Unsupported file format for bulk load: .txt"):
        loader.execute_native_bulk_load("demo", unsupported_file)


def test_execute_native_bulk_load_empty_parquet(loader: PostgresLoader, tmp_path: Path) -> None:
    """Test that an empty parquet file is skipped correctly."""
    empty_parquet = tmp_path / "empty.parquet"
    # Create an empty parquet file
    with patch("polars.read_parquet") as mock_read:
        mock_read.return_value.is_empty.return_value = True
        loader.execute_native_bulk_load("demo", empty_parquet)
        # Assert that the copy command was not executed
        assert not loader.conn.cursor.return_value.__enter__.return_value.copy.called


def test_execute_deletions_no_primary_ids_found(loader: PostgresLoader) -> None:
    """Test that deletions are skipped if no matching primary_ids are found."""
    mock_cursor = MagicMock()
    # Simulate the query for primary_ids returning an empty list
    mock_cursor.fetchall.return_value = []
    loader.conn.cursor.return_value.__enter__.return_value = mock_cursor

    deleted_count = loader.execute_deletions(["nonexistent_caseid"])
    assert deleted_count == 0
    # Ensure the DELETE statements were not executed
    assert len(mock_cursor.execute.call_args_list) == 1  # Only the initial SELECT
    assert "SELECT primaryid FROM demo" in mock_cursor.execute.call_args_list[0][0][0]


def test_run_post_load_dq_checks_no_result(loader: PostgresLoader) -> None:
    """Test the DQ check when the query returns no result."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    loader.conn.cursor.return_value.__enter__.return_value = mock_cursor

    with pytest.raises(DataQualityError, match="Could not retrieve DQ check results"):
        loader.run_post_load_dq_checks()


def test_rollback(loader: PostgresLoader) -> None:
    """Test that the rollback method is called on the connection."""
    loader.rollback()
    loader.conn.rollback.assert_called_once()


def test_generate_ddl_with_int_and_float(loader: PostgresLoader) -> None:
    """Test DDL generation for integer and float types."""

    class ModelWithNumeric(BaseModel):
        int_field: int
        float_field: float

    ddl = loader._generate_create_table_ddl("test_numeric", ModelWithNumeric)
    assert '"int_field" BIGINT NULL' in ddl
    assert '"float_field" DOUBLE PRECISION NULL' in ddl


def test_execute_native_bulk_load_zero_byte_file(loader: PostgresLoader, tmp_path: Path) -> None:
    """Test that a zero-byte file is skipped."""
    zero_byte_file = tmp_path / "zero.csv"
    zero_byte_file.touch()
    loader.execute_native_bulk_load("demo", zero_byte_file)
    # Assert that the cursor and copy were not even called
    loader.conn.cursor.assert_not_called()


def test_execute_deletions_with_ids(loader: PostgresLoader) -> None:
    """Test the deletion logic when primary IDs are found."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [{"primaryid": "123"}, {"primaryid": "456"}]
    mock_cursor.rowcount = 1  # Simulate one row deleted per table
    loader.conn.cursor.return_value.__enter__.return_value = mock_cursor

    deleted_count = loader.execute_deletions(["case1", "case2"])

    assert deleted_count == 7  # 7 tables * 1 row deleted

    delete_calls = [c for c in mock_cursor.execute.call_args_list if "DELETE FROM" in c[0][0]]
    assert len(delete_calls) == 7
    assert "DELETE FROM ther" in delete_calls[0][0][0]
    assert "DELETE FROM demo" in delete_calls[6][0][0]
    assert delete_calls[0][0][1] == (["123", "456"],)


def test_bulk_load_csv_multiple_chunks(loader: PostgresLoader, tmp_path: Path) -> None:
    """Test that the CSV bulk loader reads the file in chunks."""
    csv_file = tmp_path / "test.csv"
    # Create content larger than the 8192 chunk size to ensure the loop runs
    content = "header\n" + "a" * 9000
    csv_file.write_text(content)

    mock_copy = MagicMock()
    (
        loader.conn.cursor.return_value.__enter__.return_value.copy.return_value.__enter__.return_value
    ) = mock_copy

    loader.execute_native_bulk_load("demo", csv_file)
    # Check that write was called multiple times
    assert mock_copy.write.call_count > 1
