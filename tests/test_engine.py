# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Unit tests for the FaersLoaderEngine.
"""
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import polars as pl
import pytest
from py_load_faers.config import AppSettings, DatabaseSettings, DownloaderSettings, ProcessingSettings
from py_load_faers.engine import FaersLoaderEngine, _generate_quarters_to_load


@pytest.fixture
def mock_db_loader() -> MagicMock:
    """Fixture for a mocked AbstractDatabaseLoader."""
    return MagicMock()


@pytest.fixture
def mock_config() -> AppSettings:
    """Fixture for a mock AppSettings object."""
    return AppSettings(
        db=DatabaseSettings(),
        downloader=DownloaderSettings(),
        processing=ProcessingSettings(),
    )


def test_run_load_unsupported_mode(mock_config: AppSettings, mock_db_loader: MagicMock) -> None:
    """Test that an unsupported load mode raises NotImplementedError."""
    engine = FaersLoaderEngine(mock_config, mock_db_loader)
    with pytest.raises(NotImplementedError, match="Unsupported load mode: invalid_mode"):
        engine.run_load(mode="invalid_mode")
    mock_db_loader.rollback.assert_called_once()


def test_run_load_delta_no_latest_quarter(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test delta load when the latest quarter cannot be determined."""
    mock_db_loader.get_last_successful_load.return_value = "2023q1"
    with patch("py_load_faers.engine.find_latest_quarter", return_value=None):
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        result = engine.run_load(mode="delta")
        assert result is None
        mock_db_loader.commit.assert_called_once()


def test_run_load_delta_up_to_date(mock_config: AppSettings, mock_db_loader: MagicMock) -> None:
    """Test delta load when the database is already up to date."""
    mock_db_loader.get_last_successful_load.return_value = "2023q2"
    with patch("py_load_faers.engine.find_latest_quarter", return_value="2023q2"):
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        result = engine.run_load(mode="delta")
        assert result is None
        mock_db_loader.commit.assert_called_once()


def test_process_quarter_download_failure(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test that processing fails if the download fails."""
    engine = FaersLoaderEngine(mock_config, mock_db_loader)
    with patch("py_load_faers.engine.download_quarter", return_value=None):
        with pytest.raises(RuntimeError, match="Download failed for quarter 2023q1"):
            engine.run_load(quarter="2023q1")
    mock_db_loader.rollback.assert_called_once()
    # The metadata dict is the first positional arg of the last call
    final_metadata = mock_db_loader.update_load_history.call_args[0][0]
    assert final_metadata["status"] == "FAILED"


def test_process_quarter_no_demo_records(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test processing a quarter that has no DEMO records."""
    engine = FaersLoaderEngine(mock_config, mock_db_loader)
    # Mock the return value for the DQ checks to avoid unpack error
    mock_db_loader.run_post_load_dq_checks.return_value = (True, "OK")
    with patch("py_load_faers.engine.download_quarter", return_value=("zip_path", "checksum")), patch(
        "py_load_faers.engine.FaersLoaderEngine._parse_quarter_zip",
        return_value=(iter([]), set()),
    ), patch("py_load_faers.engine.stage_data", return_value={"other_table": [Path("file")]}):
        engine.run_load(quarter="2023q1")
        final_metadata = mock_db_loader.update_load_history.call_args[0][0]
        assert final_metadata["status"] == "SUCCESS"


def test_process_quarter_exception_and_rollback(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test that any exception during processing triggers a rollback."""
    engine = FaersLoaderEngine(mock_config, mock_db_loader)
    error_message = "Staging failed"
    with patch("py_load_faers.engine.download_quarter", return_value=("zip_path", "checksum")), patch(
        "py_load_faers.engine.FaersLoaderEngine._parse_quarter_zip",
        side_effect=Exception(error_message),
    ):
        with pytest.raises(Exception, match=error_message):
            engine.run_load(quarter="2023q1")

    mock_db_loader.rollback.assert_called_once()
    # The metadata dict is the first positional arg of the last call
    final_metadata = mock_db_loader.update_load_history.call_args[0][0]
    assert final_metadata["status"] == "FAILED"
    assert final_metadata["error_message"] == error_message


def test_generate_quarters_to_load() -> None:
    """Test the quarter generation logic."""
    assert list(_generate_quarters_to_load("2022q3", "2023q2")) == [
        "2022q3",
        "2022q4",
        "2023q1",
        "2023q2",
    ]
    assert list(_generate_quarters_to_load("2023q1", "2023q1")) == ["2023q1"]
    assert list(_generate_quarters_to_load(None, "2023q1")) == ["2023q1"]


def test_filter_staged_files_polars_empty_files(mock_config: AppSettings) -> None:
    """Test filtering with empty staged files."""
    engine = FaersLoaderEngine(mock_config, MagicMock())
    result = engine._filter_staged_files_polars({}, {"1"}, "parquet")
    assert result == {}


def test_get_caseids_from_final_demo_no_file(mock_config: AppSettings) -> None:
    """Test getting caseids when the demo file does not exist."""
    engine = FaersLoaderEngine(mock_config, MagicMock())
    assert engine._get_caseids_from_final_demo(None) == set()
    assert engine._get_caseids_from_final_demo(Path("nonexistent.parquet")) == set()


def test_parse_quarter_zip_xml(tmp_path: Path, mock_config: AppSettings) -> None:
    """Test parsing a zip file containing XML."""
    zip_path = tmp_path / "test.zip"
    xml_content = "<root></root>"
    with patch("zipfile.ZipFile") as mock_zip:
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ["test.xml"]
        mock_zip.return_value.__enter__.return_value.open.return_value.__enter__.return_value = (
            MagicMock()
        )
        with patch(
            "py_load_faers.engine.parse_xml_file", return_value=(iter([{"a": 1}]), {"1"})
        ) as mock_parse:
            engine = FaersLoaderEngine(mock_config, MagicMock())
            iterator, nulls = engine._parse_quarter_zip(zip_path, tmp_path)
            # We need to consume the iterator to check the content
            assert list(iterator) == [{"a": 1}]
            assert nulls == {"1"}
            mock_parse.assert_called_once()


def test_parse_quarter_zip_ascii(tmp_path: Path, mock_config: AppSettings) -> None:
    """Test parsing a zip file containing ASCII files."""
    zip_path = tmp_path / "test.zip"
    with patch("zipfile.ZipFile") as mock_zip:
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ["test.txt"]
        with patch("py_load_faers.engine.extract_zip_archive") as mock_extract, patch(
            "py_load_faers.engine.parse_ascii_quarter", return_value=(iter([]), set())
        ) as mock_parse:
            engine = FaersLoaderEngine(mock_config, MagicMock())
            engine._parse_quarter_zip(zip_path, tmp_path)
            mock_extract.assert_called_once_with(zip_path, tmp_path)
            mock_parse.assert_called_once_with(tmp_path)


def test_run_load_delta_first_run(mock_config: AppSettings, mock_db_loader: MagicMock) -> None:
    """Test the delta load logic for the very first run (no previous history)."""
    mock_db_loader.get_last_successful_load.return_value = None
    mock_db_loader.run_post_load_dq_checks.return_value = (True, "OK")
    with patch("py_load_faers.engine.find_latest_quarter", return_value="2023q1"), patch(
        "py_load_faers.engine.FaersLoaderEngine._process_quarter"
    ) as mock_process:
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        engine.run_load(mode="delta")
        # Should process only the latest quarter
        mock_process.assert_called_once_with("2023q1", "DELTA")


def test_filter_staged_files_polars_no_chunks(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """Test _filter_staged_files_polars when a table has no chunks."""
    engine = FaersLoaderEngine(mock_config, MagicMock())
    staged_files = {"demo": []}  # No chunks for demo table
    result = engine._filter_staged_files_polars(staged_files, {"1"}, "parquet")
    assert "demo" not in result


def test_filter_staged_files_empty_csv_with_known_model(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """Test creating an empty CSV file with headers for a known model."""
    mock_config.processing.staging_format = "csv"
    engine = FaersLoaderEngine(mock_config, MagicMock())

    # Create a dummy csv file that will be filtered to empty
    csv_path = tmp_path / "demo_chunk_0.csv"
    from py_load_faers.models import Demo

    headers = list(Demo.model_fields.keys())
    csv_path.write_text("$".join(headers) + "\n" + "$".join(["1", "101", "20250101", "", "", "", "", ""]))

    staged_files = {"demo": [csv_path]}
    # Filter with a primaryid that doesn't exist
    result = engine._filter_staged_files_polars(staged_files, {"999"}, "csv")
    final_path = result["demo"]
    assert final_path.exists()

    # The file should contain only the headers from the DEMO model
    expected_headers = "$".join(headers)
    assert final_path.read_text() == expected_headers


def test_run_load_delta_up_to_date_case_insensitive(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test delta load when the DB is up to date (case-insensitive)."""
    mock_db_loader.get_last_successful_load.return_value = "2023Q1"
    with patch("py_load_faers.engine.find_latest_quarter", return_value="2023q1"):
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        result = engine.run_load(mode="delta")
        assert result is None
        mock_db_loader.commit.assert_called_once()


def test_filter_staged_files_polars_empty_parquet(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """Test filtering to an empty parquet file creates a valid empty file."""
    mock_config.processing.staging_format = "parquet"
    engine = FaersLoaderEngine(mock_config, MagicMock())

    pq_path = tmp_path / "demo_chunk_0.parquet"
    pl.DataFrame({"primaryid": ["1"], "caseid": ["101"]}).write_parquet(pq_path)
    staged_files = {"demo": [pq_path]}

    # Filter with a non-existent primaryid to create an empty result
    result = engine._filter_staged_files_polars(staged_files, {"999"}, "parquet")

    final_path = result["demo"]
    assert final_path.exists()
    df = pl.read_parquet(final_path)
    assert df.is_empty()
    # Check that schema is preserved
    assert "primaryid" in df.columns
    assert "caseid" in df.columns


def test_run_load_standard_delta(mock_config: AppSettings, mock_db_loader: MagicMock) -> None:
    """Test a standard delta load run where new quarters are available."""
    mock_db_loader.get_last_successful_load.return_value = "2023q1"
    mock_db_loader.run_post_load_dq_checks.return_value = (True, "OK")
    with patch("py_load_faers.engine.find_latest_quarter", return_value="2023q3"), patch(
        "py_load_faers.engine.FaersLoaderEngine._process_quarter"
    ) as mock_process:
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        engine.run_load(mode="delta")
        # Should process Q2 and Q3
        assert mock_process.call_count == 2
        mock_process.assert_has_calls([call("2023q2", "DELTA"), call("2023q3", "DELTA")])


def test_filter_staged_files_polars_drug_cleaning(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """Test the specific drug name cleaning logic in the filtering function."""
    engine = FaersLoaderEngine(mock_config, MagicMock())
    drug_csv = tmp_path / "drug_chunk_0.parquet"
    messy_df = pl.DataFrame(
        {
            "primaryid": ["1", "2", "3", "4"],
            "drugname": [" Aspirin ", "Tylenol!!", "NULL", "Ibuprofen 200mg"],
        }
    )
    messy_df.write_parquet(drug_csv)

    staged_files = {"drug": [drug_csv]}
    result = engine._filter_staged_files_polars(staged_files, {"1", "2", "3", "4"}, "parquet")

    clean_df = pl.read_parquet(result["drug"])
    cleaned_names = sorted(clean_df["drugname"].to_list())
    assert cleaned_names == sorted(["ASPIRIN", "TYLENOL", "", "IBUPROFEN 200MG"])


def test_process_quarter_only_nullifications(
    mock_config: AppSettings, mock_db_loader: MagicMock
) -> None:
    """Test a quarter that only contains nullifications and no new data to load."""
    mock_db_loader.run_post_load_dq_checks.return_value = (True, "OK")
    engine = FaersLoaderEngine(mock_config, mock_db_loader)
    with patch("py_load_faers.engine.download_quarter", return_value=("zip", "checksum")), patch(
        "py_load_faers.engine.FaersLoaderEngine._parse_quarter_zip",
        # Return nullified IDs, but an iterator that yields no demo records
        return_value=(iter([{"other": [{"id": 1}]}]), {"null_id_1"}),
    ), patch(
        "py_load_faers.engine.stage_data", return_value={"other": [Path("file")]}
    ), patch(
        "py_load_faers.engine.deduplicate_polars", return_value=set()
    ):
        engine.run_load(quarter="2023q4")
        # Assert that deletions were called with a list, as per the engine's implementation
        mock_db_loader.execute_deletions.assert_called_once_with(["null_id_1"])
        mock_db_loader.handle_delta_merge.assert_not_called()
        final_metadata = mock_db_loader.update_load_history.call_args[0][0]
        assert final_metadata["status"] == "SUCCESS"


def test_run_load_delta_db_is_ahead(mock_config: AppSettings, mock_db_loader: MagicMock) -> None:
    """Test delta load when the DB is ahead of the latest available quarter."""
    mock_db_loader.get_last_successful_load.return_value = "2023q3"
    with patch("py_load_faers.engine.find_latest_quarter", return_value="2023q2"):
        engine = FaersLoaderEngine(mock_config, mock_db_loader)
        result = engine.run_load(mode="delta")
        assert result is None
        mock_db_loader.commit.assert_called_once()


def test_get_caseids_from_final_demo_empty_csv(mock_config: AppSettings, tmp_path: Path) -> None:
    """Test getting caseids from an empty demo CSV file."""
    mock_config.processing.staging_format = "csv"
    engine = FaersLoaderEngine(mock_config, MagicMock())
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("primaryid$caseid\n")
    assert engine._get_caseids_from_final_demo(empty_file) == set()


def test_filter_staged_files_polars_writes_parquet(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """Test that _filter_staged_files_polars correctly writes a final Parquet file."""
    mock_config.processing.staging_format = "parquet"
    engine = FaersLoaderEngine(mock_config, MagicMock())

    pq_path = tmp_path / "demo_chunk_0.parquet"
    pl.DataFrame({"primaryid": ["1", "2"], "caseid": ["101", "102"]}).write_parquet(pq_path)

    staged_files = {"demo": [pq_path]}
    result = engine._filter_staged_files_polars(staged_files, {"1"}, "parquet")
    final_path = result["demo"]
    assert final_path.exists()

    df = pl.read_parquet(final_path)
    assert df.shape == (1, 2)
    assert df["primaryid"][0] == "1"


def test_filter_staged_files_empty_unknown_model(
    mock_config: AppSettings, tmp_path: Path
) -> None:
    """
    Test the case where an empty CSV is created for a table not in FAERS_TABLE_MODELS.
    This covers the `else` branch of the `if model_type:` check.
    """
    mock_config.processing.staging_format = "csv"
    engine = FaersLoaderEngine(mock_config, MagicMock())
    csv_path = tmp_path / "unknown_table_chunk_0.csv"
    csv_path.write_text("primaryid$value\n1$A\n")
    staged_files = {"unknown_table": [csv_path]}

    # Filter to make the result empty
    result = engine._filter_staged_files_polars(staged_files, {"999"}, "csv")

    final_path = result["unknown_table"]
    assert final_path.exists()
    # The file should be empty because the model is unknown and no headers can be written
    assert final_path.read_text() == ""