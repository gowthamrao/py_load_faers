# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the staging module.
"""
import csv
import zipfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from pydantic import BaseModel

from py_load_faers.config import ProcessingSettings
from py_load_faers.staging import (
    extract_zip_archive,
    stage_data,
    stage_data_to_csv_files,
    stage_data_to_parquet_files,
)


# Test model
class MockPatient(BaseModel):
    primaryid: str
    caseid: str
    patient_sex: str


class MockDrug(BaseModel):
    primaryid: str
    caseid: str
    drugname: str


# Test data
MOCK_TABLE_MODELS = {
    "patient": MockPatient,
    "drug": MockDrug,
}

MOCK_RECORDS = [
    {
        "patient": [{"primaryid": "1", "caseid": "1", "patient_sex": "F"}],
        "drug": [
            {"primaryid": "1", "caseid": "1", "drugname": "A"},
            {"primaryid": "1", "caseid": "1", "drugname": "B"},
        ],
    },
    {
        "patient": [{"primaryid": "2", "caseid": "2", "patient_sex": "M"}],
        "drug": [{"primaryid": "2", "caseid": "2", "drugname": "C"}],
    },
]


def test_stage_data_to_csv_files(tmp_path: Path) -> None:
    """Test staging data to multiple CSV files with chunking."""
    staged_files = stage_data_to_csv_files(
        iter(MOCK_RECORDS), MOCK_TABLE_MODELS, chunk_size=2, staging_dir=tmp_path
    )

    assert "patient" in staged_files
    assert "drug" in staged_files
    assert len(staged_files["patient"]) == 1
    assert len(staged_files["drug"]) == 2  # 3 records, chunk_size=2 -> 2 files

    # Verify patient csv
    patient_file = staged_files["patient"][0]
    with patient_file.open("r") as f:
        reader = csv.reader(f, delimiter="$")
        assert next(reader) == ["primaryid", "caseid", "patient_sex"]
        assert next(reader) == ["1", "1", "F"]
        assert next(reader) == ["2", "2", "M"]

    # Verify drug csv (chunking)
    drug_file_1 = staged_files["drug"][0]
    with drug_file_1.open("r") as f:
        reader = csv.reader(f, delimiter="$")
        assert next(reader) == ["primaryid", "caseid", "drugname"]
        assert next(reader) == ["1", "1", "A"]
        assert next(reader) == ["1", "1", "B"]

    drug_file_2 = staged_files["drug"][1]
    with drug_file_2.open("r") as f:
        reader = csv.reader(f, delimiter="$")
        assert next(reader) == ["primaryid", "caseid", "drugname"]
        assert next(reader) == ["2", "2", "C"]


def test_stage_data_to_parquet_files(tmp_path: Path) -> None:
    """Test staging data to multiple Parquet files with chunking."""
    staged_files = stage_data_to_parquet_files(
        iter(MOCK_RECORDS), MOCK_TABLE_MODELS, chunk_size=2, staging_dir=tmp_path
    )

    assert "patient" in staged_files
    assert "drug" in staged_files
    assert len(staged_files["patient"]) == 1
    assert len(staged_files["drug"]) == 2  # 3 records, chunk_size=2 -> 2 files

    # Verify patient parquet
    patient_df = pl.read_parquet(staged_files["patient"][0])
    assert patient_df.shape == (2, 3)
    assert patient_df.columns == ["primaryid", "caseid", "patient_sex"]
    assert patient_df["primaryid"].to_list() == ["1", "2"]

    # Verify drug parquet
    drug_df_1 = pl.read_parquet(staged_files["drug"][0])
    assert drug_df_1.shape == (2, 3)
    assert drug_df_1["drugname"].to_list() == ["A", "B"]

    drug_df_2 = pl.read_parquet(staged_files["drug"][1])
    assert drug_df_2.shape == (1, 3)
    assert drug_df_2["drugname"].to_list() == ["C"]


def test_stage_data_to_parquet_files_empty_buffer_return(tmp_path: Path) -> None:
    """Test that _flush_buffer_to_parquet handles empty buffers correctly."""
    # This test ensures the guard clause `if not buffer: return` works.
    from py_load_faers.staging import _flush_buffer_to_parquet

    # Call with empty buffer, no exception should be raised.
    _flush_buffer_to_parquet(tmp_path, "patient", [], {}, {}, MockPatient)


def test_stage_data_dispatcher(tmp_path: Path) -> None:
    """Test the dispatcher function `stage_data`."""
    # Test CSV path
    settings_csv = ProcessingSettings(staging_format="csv")
    with patch("py_load_faers.staging.stage_data_to_csv_files") as mock_csv:
        stage_data(iter(MOCK_RECORDS), MOCK_TABLE_MODELS, settings_csv, tmp_path)
        mock_csv.assert_called_once()

    # Test Parquet path
    settings_parquet = ProcessingSettings(staging_format="parquet")
    with patch("py_load_faers.staging.stage_data_to_parquet_files") as mock_parquet:
        stage_data(iter(MOCK_RECORDS), MOCK_TABLE_MODELS, settings_parquet, tmp_path)
        mock_parquet.assert_called_once()

    # Test invalid format by mocking the setting
    settings_valid = ProcessingSettings(staging_format="csv")
    with patch.object(settings_valid, "staging_format", "invalid_format"):
        with pytest.raises(ValueError, match="Unsupported staging format: invalid_format"):
            stage_data(iter(MOCK_RECORDS), MOCK_TABLE_MODELS, settings_valid, tmp_path)


def test_stage_data_no_staging_dir(tmp_path: Path) -> None:
    """Test staging data without providing a staging directory."""
    with patch("tempfile.mkdtemp", return_value=str(tmp_path)):
        # Test CSV path
        settings_csv = ProcessingSettings(staging_format="csv")
        staged_files_csv = stage_data(iter(MOCK_RECORDS), MOCK_TABLE_MODELS, settings_csv)
        assert len(staged_files_csv["patient"]) == 1

        # Test Parquet path
        settings_parquet = ProcessingSettings(staging_format="parquet")
        staged_files_parquet = stage_data(iter(MOCK_RECORDS), MOCK_TABLE_MODELS, settings_parquet)
        assert len(staged_files_parquet["patient"]) == 1


def test_extract_zip_archive(tmp_path: Path) -> None:
    """
    Test that a zip archive is correctly extracted.
    """
    # 1. Create a dummy zip file
    zip_path = tmp_path / "test_archive.zip"
    file1_content = "hello"
    file2_content = "world"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", file1_content)
        zf.writestr("subdir/file2.txt", file2_content)

    # 2. Call the extraction function
    extract_dir = tmp_path / "extracted"
    extracted_files = extract_zip_archive(zip_path, extract_dir)

    # 3. Assertions
    assert extract_dir.exists()
    assert (extract_dir / "file1.txt").is_file()
    assert (extract_dir / "subdir" / "file2.txt").is_file()

    # Check the list of returned files
    assert len(extracted_files) == 2
    assert extract_dir / "file1.txt" in extracted_files
    assert extract_dir / "subdir" / "file2.txt" in extracted_files

    # Check file contents
    assert (extract_dir / "file1.txt").read_text() == file1_content
    assert (extract_dir / "subdir" / "file2.txt").read_text() == file2_content
