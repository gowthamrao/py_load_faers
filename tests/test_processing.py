# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the data processing module.
"""
import csv
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List
from unittest.mock import patch

import pytest

from py_load_faers.processing import (
    deduplicate_polars,
    get_caseids_to_delete,
    clean_drug_names,
)


def test_get_caseids_to_delete_found(tmp_path: Path) -> None:
    """
    Test that get_caseids_to_delete correctly finds and parses a deletion file.
    """
    zip_path = tmp_path / "faers_ascii_2025q1.zip"
    delete_filename = "del25q1.txt"
    case_ids_to_delete = ["1001", "1002", "1003"]

    # Create a dummy zip file with a deletion list
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(delete_filename, "\n".join(case_ids_to_delete))
        zf.writestr("other_file.txt", "some data")

    result = get_caseids_to_delete(zip_path)
    assert result == set(case_ids_to_delete)


def test_get_caseids_to_delete_not_found(tmp_path: Path) -> None:
    """
    Test that get_caseids_to_delete returns an empty set when no deletion file exists.
    """
    zip_path = tmp_path / "faers_ascii_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("other_file.txt", "some data")
    result = get_caseids_to_delete(zip_path)
    assert result == set()


@pytest.fixture
def create_demo_csv(
    tmp_path: Path,
) -> Callable[[List[Dict[str, Any]], str], Path]:
    """A pytest fixture to create a sample DEMO csv file for testing."""

    def _create_csv(records: List[Dict[str, Any]], filename: str = "test_demo.csv") -> Path:
        csv_path = tmp_path / filename
        # Use a minimal set of headers for simplicity
        min_headers = ["primaryid", "caseid", "fda_dt"]

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="$")
            writer.writerow(min_headers)
            for record in records:
                writer.writerow([record.get(h, "") for h in min_headers])
        return csv_path

    return _create_csv


def test_deduplicate_polars_basic(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test the core logic: latest fda_dt wins."""
    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "1", "primaryid": "102", "fda_dt": "20240201"},  # Keep
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},  # Keep
    ]
    expected_ids = {"102", "201"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_basic.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_tiebreaker(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test the tie-breaking logic: when fda_dt is the same, latest primaryid wins."""
    records = [
        {"caseid": "3", "primaryid": "301", "fda_dt": "20240401"},
        {"caseid": "3", "primaryid": "302", "fda_dt": "20240401"},  # Keep
    ]
    expected_ids = {"302"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_tiebreaker.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_complex_mix(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test a complex mix of scenarios."""
    records = [
        # This order is intentionally mixed up to ensure sorting works correctly
        {"caseid": "1", "primaryid": "101", "fda_dt": "20230115"},
        {"caseid": "3", "primaryid": "301", "fda_dt": "20230505"},
        {"caseid": "2", "primaryid": "201", "fda_dt": "20230101"},  # Keep
        {"caseid": "1", "primaryid": "102", "fda_dt": "20230320"},  # Keep
        {"caseid": "3", "primaryid": "302", "fda_dt": "20230505"},  # Keep
    ]
    expected_ids = {"102", "201", "302"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_complex_mix.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_multiple_files(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test that deduplication works correctly across multiple source files."""
    records1 = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},  # Keep
    ]
    records2 = [
        {"caseid": "1", "primaryid": "102", "fda_dt": "20240201"},  # Keep
        {"caseid": "3", "primaryid": "301", "fda_dt": "20240401"},  # Keep
    ]
    expected_ids = {"102", "201", "301"}

    demo_file1 = create_demo_csv(records1, "demo1.csv")
    demo_file2 = create_demo_csv(records2, "demo2.csv")

    result = deduplicate_polars([demo_file1, demo_file2], "csv")
    assert result == expected_ids


def test_deduplicate_polars_empty_input() -> None:
    """Test that the function handles an empty list of files."""
    assert deduplicate_polars([], "csv") == set()


def test_deduplicate_polars_empty_file(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test that the function handles a file that is empty or has only a header."""
    demo_file = create_demo_csv([], "test_deduplicate_polars_empty_file.csv")
    assert deduplicate_polars([demo_file], "csv") == set()


def test_deduplicate_polars_missing_column(tmp_path: Path) -> None:
    """Test that the function raises an error if a required column is missing."""
    csv_path = tmp_path / "bad_data.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="$")
        writer.writerow(["caseid", "some_other_column"])
        writer.writerow(["1", "abc"])

    with pytest.raises(ValueError, match="Deduplication failed due to missing columns"):
        deduplicate_polars([csv_path], "csv")


def test_deduplicate_polars_with_missing_fda_dt(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test that records with missing fda_dt are handled correctly."""
    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "1", "primaryid": "102", "fda_dt": ""},  # Missing fda_dt
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},
    ]
    # The record with the missing fda_dt should be sorted last and thus not chosen.
    expected_ids = {"101", "201"}
    demo_file = create_demo_csv(records, "test_missing_fda_dt.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_with_malformed_fda_dt(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test that records with malformed fda_dt are handled correctly."""
    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "1", "primaryid": "102", "fda_dt": "NOT-A-DATE"},  # Malformed
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},
    ]
    # The record with the malformed fda_dt should be filtered out.
    expected_ids = {"101", "201"}
    demo_file = create_demo_csv(records, "test_malformed_fda_dt.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_non_numeric_primaryid(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path],
) -> None:
    """Test tie-breaking with non-numeric primaryid values."""
    records = [
        # Tie on fda_dt, should be broken by primaryid
        {"caseid": "5", "primaryid": "ABC-100", "fda_dt": "20240501"},
        {"caseid": "5", "primaryid": "ABC-101", "fda_dt": "20240501"},  # Keep
    ]
    expected_ids = {"ABC-101"}
    demo_file = create_demo_csv(records, "test_non_numeric_primaryid.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_get_caseids_to_delete_bad_zip(tmp_path: Path) -> None:
    """Test that a BadZipFile error is caught and re-raised."""
    bad_zip_path = tmp_path / "not_a_zip.zip"
    bad_zip_path.write_text("this is not a zip file")
    with pytest.raises(zipfile.BadZipFile):
        get_caseids_to_delete(bad_zip_path)


def test_get_caseids_to_delete_ignores_non_digit(tmp_path: Path) -> None:
    """Test that non-digit case IDs in the deletion file are ignored."""
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("del.txt", "123\nnot-a-digit\n456")
    result = get_caseids_to_delete(zip_path)
    assert result == {"123", "456"}


def test_deduplicate_polars_unsupported_format(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that an unsupported format raises a ValueError."""
    demo_file = create_demo_csv([], "dummy.csv")
    with pytest.raises(ValueError, match="Unsupported format for deduplication: txt"):
        deduplicate_polars([demo_file], "txt")


def test_deduplicate_polars_with_case_ids_to_ignore(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that case_ids_to_ignore are correctly excluded."""
    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},  # Should be ignored
        {"caseid": "3", "primaryid": "301", "fda_dt": "20240401"},
    ]
    expected_ids = {"101", "301"}
    demo_file = create_demo_csv(records, "test_ignore.csv")
    result = deduplicate_polars([demo_file], "csv", case_ids_to_ignore={"2"})
    assert result == expected_ids


def test_deduplicate_polars_collect_exception(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that an exception during the collect phase is handled."""
    demo_file = create_demo_csv([{"caseid": "1", "primaryid": "101", "fda_dt": "20240101"}])
    with patch("polars.LazyFrame.collect", side_effect=BaseException("Polars Panic")):
        # The function should catch the exception and return an empty set
        result = deduplicate_polars([demo_file], "csv")
        assert result == set()


def test_deduplicate_polars_generic_exception(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test handling of a generic exception during deduplication."""
    demo_file = create_demo_csv([{"caseid": "1", "primaryid": "101", "fda_dt": "20240101"}])
    with patch("polars.scan_csv", side_effect=Exception("Unexpected error")):
        with pytest.raises(Exception, match="Unexpected error"):
            deduplicate_polars([demo_file], "csv")


def test_clean_drug_names() -> None:
    """Test the clean_drug_names function."""
    records = [
        {"drugname": " Aspirin "},
        {"drugname": "Tylenol!!"},
        {"drugname": "NULL"},
        {"drugname": "null"},
        {"drugname": "Drug-X (Y)"},
        {"other_field": "value"},
        {"drugname": 123},  # Should be ignored
    ]
    cleaned = clean_drug_names(records)
    assert cleaned[0]["drugname"] == "ASPIRIN"
    assert cleaned[1]["drugname"] == "TYLENOL"
    assert cleaned[2]["drugname"] == ""
    assert cleaned[3]["drugname"] == ""
    assert cleaned[4]["drugname"] == "DRUGX Y"
    assert "other_field" in cleaned[5]
    assert cleaned[6]["drugname"] == 123


def test_deduplicate_polars_parquet(tmp_path: Path) -> None:
    """Test deduplication logic with Parquet files."""
    import polars as pl

    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "1", "primaryid": "102", "fda_dt": "20240201"},  # Keep
    ]
    expected_ids = {"102"}

    parquet_file = tmp_path / "demo.parquet"
    pl.DataFrame(records).write_parquet(parquet_file)

    result = deduplicate_polars([parquet_file], "parquet")
    assert result == expected_ids


def test_get_caseids_to_delete_generic_exception(tmp_path: Path) -> None:
    """Test that a generic exception in get_caseids_to_delete is handled."""
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("del.txt", "123")
    with patch("zipfile.ZipFile.open", side_effect=Exception("Unexpected read error")):
        with pytest.raises(Exception, match="Unexpected read error"):
            get_caseids_to_delete(zip_path)


def test_deduplicate_polars_no_valid_files(tmp_path: Path) -> None:
    """Test deduplicate_polars with a list of non-existent or empty files."""
    non_existent_file = tmp_path / "non_existent.csv"
    empty_file = tmp_path / "empty.csv"
    empty_file.touch()
    result = deduplicate_polars([non_existent_file, empty_file], "csv")
    assert result == set()


def test_deduplicate_polars_schema_error(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that a SchemaError during deduplication is handled."""
    import polars as pl

    demo_file = create_demo_csv([{"caseid": "1", "primaryid": "101", "fda_dt": "20240101"}])
    with patch(
        "polars.scan_csv",
        side_effect=pl.exceptions.SchemaError("Mismatched schema"),
    ):
        with pytest.raises(ValueError, match="Deduplication failed"):
            deduplicate_polars([demo_file], "csv")


def test_deduplicate_polars_missing_columns_in_lazyframe(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test the ValueError for missing columns after scanning."""
    import polars as pl

    # Create a file that is valid but will be mocked to produce a bad LazyFrame
    demo_file = create_demo_csv([{"caseid": "1", "primaryid": "101", "fda_dt": "20240101"}])

    # Mock the result of scan_csv to return a LazyFrame with a missing column
    mock_lf = pl.LazyFrame({"caseid": ["1"]})
    with patch("polars.scan_csv", return_value=mock_lf):
        with pytest.raises(ValueError, match="Deduplication failed due to missing columns"):
            deduplicate_polars([demo_file], "csv")
