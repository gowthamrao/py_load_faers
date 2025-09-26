# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the ASCII file parser.
"""
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pytest
from py_load_faers.parser import parse_ascii_file, parse_ascii_quarter, parse_xml_file

SAMPLE_DEMO_DATA = """\
PRIMARYID$CASEID$CASEVERSION$I_F_CODE$EVENT_DT$MFR_DT
12345$67890$1$I$20250101$20250102
54321$98765$2$F$20250201$20250202
"""


def test_parse_ascii_file(tmp_path: Path) -> None:
    """Test that a standard FAERS ASCII file is parsed correctly."""
    data_file = tmp_path / "DEMO25Q1.txt"
    data_file.write_text(SAMPLE_DEMO_DATA)

    records = list(parse_ascii_file(data_file))

    assert len(records) == 2

    # Check the first record
    assert records[0] == {
        "primaryid": "12345",
        "caseid": "67890",
        "caseversion": "1",
        "i_f_code": "I",
        "event_dt": "20250101",
        "mfr_dt": "20250102",
    }

    # Check the second record's keys
    assert records[1]["primaryid"] == "54321"
    assert records[1]["caseid"] == "98765"


def test_parse_empty_file(tmp_path: Path) -> None:
    """Test that parsing an empty file yields no records."""
    data_file = tmp_path / "EMPTY.txt"
    data_file.write_text("")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0


def test_parse_ascii_quarter_no_demo(tmp_path: Path) -> None:
    """Test parsing a quarter where the DEMO file is missing."""
    # Create a dummy drug file but no demo file
    (tmp_path / "DRUG25Q1.TXT").write_text("primaryid$drugname\n1$A\n")
    record_generator, null_ids = parse_ascii_quarter(tmp_path)
    assert list(record_generator) == []
    assert null_ids == set()


def test_parse_deletion_file_malformed(tmp_path: Path) -> None:
    """Test parsing a malformed deletion file."""
    # File with no caseid column
    (tmp_path / "del_25q1.txt").write_text("wrong_header\n123")
    # A valid demo file is needed for the function to run
    (tmp_path / "DEMO25Q1.TXT").write_text(SAMPLE_DEMO_DATA)

    _, null_ids = parse_ascii_quarter(tmp_path)
    # Should return an empty set because the file could not be parsed
    assert null_ids == set()


def test_load_ascii_table_exception(tmp_path: Path) -> None:
    """Test that an exception during table loading is handled gracefully."""
    (tmp_path / "DEMO25Q1.TXT").write_text(SAMPLE_DEMO_DATA)
    with patch("polars.read_csv", side_effect=Exception("Read Error")):
        # The function should not crash, just log an error.
        record_generator, _ = parse_ascii_quarter(tmp_path)
        # No records should be generated if the demo file fails to load
        assert list(record_generator) == []


def test_parse_ascii_file_not_found() -> None:
    """Test that parse_ascii_file raises FileNotFoundError for a missing file."""
    with pytest.raises(FileNotFoundError):
        list(parse_ascii_file(Path("nonexistent_file.txt")))


def test_parse_ascii_file_generic_exception(tmp_path: Path) -> None:
    """Test that parse_ascii_file raises a generic exception on other errors."""
    data_file = tmp_path / "test.txt"
    data_file.write_text("ID$NAME\n1$A")
    with patch("pathlib.Path.open", side_effect=Exception("Unexpected I/O error")):
        with pytest.raises(Exception, match="Unexpected I/O error"):
            list(parse_ascii_file(data_file))


# --- XML Parser Tests ---


def test_parse_xml_missing_identifiers() -> None:
    """Test that XML records missing primaryid or caseid are skipped."""
    xml_content = b"""
    <root>
        <safetyreport>
            <case><caseid>1</caseid></case>
            <patient></patient>
        </safetyreport>
        <safetyreport>
            <safetyreportid>2</safetyreportid>
            <patient></patient>
        </safetyreport>
    </root>
    """
    xml_stream = BytesIO(xml_content)
    records, _ = parse_xml_file(xml_stream)
    assert len(list(records)) == 0


def test_parse_xml_no_indication() -> None:
    """Test an XML drug element with no indication sub-element."""
    xml_content = b"""
    <root>
        <safetyreport>
            <safetyreportid>1</safetyreportid>
            <case><caseid>1</caseid></case>
            <patient>
                <drug>
                    <drugsequencenumber>1</drugsequencenumber>
                    <medicinalproduct>DRUG_A</medicinalproduct>
                    <!-- No indication here -->
                </drug>
            </patient>
        </safetyreport>
    </root>
    """
    xml_stream = BytesIO(xml_content)
    records = list(parse_xml_file(xml_stream)[0])
    assert len(records) == 1
    # The 'indi' list should be empty
    assert records[0]["indi"] == []
    assert records[0]["drug"][0]["drugname"] == "DRUG_A"


def test_parse_xml_no_summary() -> None:
    """Test an XML report with no summary element."""
    xml_content = b"""
    <root>
        <safetyreport>
            <safetyreportid>1</safetyreportid>
            <case><caseid>1</caseid></case>
            <patient></patient>
            <!-- No summary here -->
        </safetyreport>
    </root>
    """
    xml_stream = BytesIO(xml_content)
    records = list(parse_xml_file(xml_stream)[0])
    assert len(records) == 1
    # The 'outc' list should be empty
    assert records[0]["outc"] == []


def test_parse_xml_generic_exception() -> None:
    """Test that a generic exception during XML parsing is handled."""
    xml_stream = BytesIO(b"<root><safetyreport>")  # Malformed XML
    with patch("lxml.etree.iterparse", side_effect=Exception("Parsing Crash")):
        with pytest.raises(Exception, match="Parsing Crash"):
            list(parse_xml_file(xml_stream)[0])


def test_parse_xml_missing_patient() -> None:
    """Test parsing an XML file where the <patient> tag is missing."""
    xml_content = b"""
<root>
    <safetyreport>
        <safetyreportid>1</safetyreportid>
        <case><caseid>1</caseid></case>
        <!-- No patient tag here -->
    </safetyreport>
</root>
"""
    xml_stream = BytesIO(xml_content)
    records, _ = parse_xml_file(xml_stream)
    record_list = list(records)
    assert len(record_list) == 1
    # All patient-derived tables should be empty
    assert record_list[0]["demo"] == []
    assert record_list[0]["drug"] == []
    assert record_list[0]["reac"] == []


def test_parse_xml_missing_patient_and_source() -> None:
    """Test parsing XML where optional high-level tags like <patient> are missing."""
    xml_content = b"""
<root>
    <safetyreport>
        <safetyreportid>1</safetyreportid>
        <case><caseid>1</caseid></case>
        <!-- No patient or primarysource tags -->
        <summary><result>DE</result></summary>
    </safetyreport>
</root>
"""
    xml_stream = BytesIO(xml_content)
    records, _ = parse_xml_file(xml_stream)
    record_list = list(records)
    assert len(record_list) == 1
    # Check that tables derived from patient and primarysource are empty
    assert record_list[0]["demo"] == []
    assert record_list[0]["drug"] == []
    assert record_list[0]["reac"] == []
    assert record_list[0]["ther"] == []
    assert record_list[0]["indi"] == []
    assert record_list[0]["rpsr"] == []
    # Check that the table from summary is still populated
    assert record_list[0]["outc"] == [{"primaryid": "1", "caseid": "1", "outc_cod": "DE"}]


def test_parse_malformed_data(tmp_path: Path) -> None:
    """Test parsing of a file with a malformed data row."""
    # The second data row has one fewer field than the header
    malformed_data = "ID$NAME$VALUE\n1$A$100\n2$B\n3$C$300"
    data_file = tmp_path / "MALFORMED.txt"
    data_file.write_text(malformed_data)

    records = list(parse_ascii_file(data_file))

    # The parser should resiliently skip the malformed row
    assert len(records) == 2
    assert records[0] == {"id": "1", "name": "A", "value": "100"}
    assert records[1] == {"id": "3", "name": "C", "value": "300"}


@pytest.mark.parametrize(
    "encoding, line_ending",
    [
        ("utf-8", "\n"),
        ("latin-1", "\n"),
        ("utf-16", "\n"),
        ("utf-8", "\r\n"),  # Windows line endings
        ("utf-8", "\r"),  # Old Mac line endings
    ],
)
def test_file_encodings_and_line_endings(tmp_path: Path, encoding: str, line_ending: str) -> None:
    """Test parsing with various file encodings and line endings."""
    data = line_ending.join(["ID$NAME", "1$Résumé", "2$Test"])
    data_file = tmp_path / "test.txt"
    data_file.write_bytes(data.encode(encoding))

    records = list(parse_ascii_file(data_file, encoding=encoding))

    assert len(records) == 2
    assert records[0] == {"id": "1", "name": "Résumé"}
    assert records[1] == {"id": "2", "name": "Test"}


def test_parse_ascii_quarter() -> None:
    """
    Tests the main ASCII parsing logic which reads a directory of ASCII files,
    handles deletions, and structures the data correctly.
    """
    # Point to the directory containing the unzipped ASCII files
    test_data_dir = Path("tests/integration/test_data/ascii_quarter")

    # This is the function we will build in the next step
    record_generator, nullified_case_ids = parse_ascii_quarter(test_data_dir)

    # Convert generator to list to inspect the results
    records = list(record_generator)

    # 1. Check that the deleted case was identified
    assert nullified_case_ids == {"102"}

    # 2. Check that the correct number of records are returned (3 total cases - 1 deleted)
    assert len(records) == 2

    # 3. Deeply inspect the structure of a returned record to ensure it matches the XML
    # parser's output
    case_101 = next((r for r in records if r["demo"][0]["caseid"] == "101"), None)
    assert case_101 is not None

    # Check the demo table data for case 101
    assert case_101["demo"] == [
        {
            "primaryid": "10101",
            "caseid": "101",
            "fda_dt": "20240101",
            "sex": "F",
            "age": "55",
            "reporter_country": "US",
        }
    ]

    # Check the drug table data for case 101
    assert len(case_101["drug"]) == 2
    assert case_101["drug"][0] == {
        "primaryid": "10101",
        "caseid": "101",
        "drug_seq": "1",
        "drugname": "ASPIRIN",
        "role_cod": "PS",
    }
    assert case_101["drug"][1]["drugname"] == "LISINOPRIL"

    # Check the reaction table data for case 101
    assert case_101["reac"] == [{"primaryid": "10101", "caseid": "101", "pt": "RASH"}]

    # Check case 103 as well
    case_103 = next((r for r in records if r["demo"][0]["caseid"] == "103"), None)
    assert case_103 is not None
    assert case_103["drug"][0]["drugname"] == "IBUPROFEN"
    assert len(case_103["reac"]) == 2
    assert {"primaryid": "10301", "caseid": "103", "pt": "NAUSEA"} in case_103["reac"]
    assert {"primaryid": "10301", "caseid": "103", "pt": "DIZZINESS"} in case_103["reac"]


def test_parse_header_only_file(tmp_path: Path) -> None:
    """Test that a file with only a header yields no records."""
    data_file = tmp_path / "HEADER.txt"
    data_file.write_text("PRIMARYID$CASEID")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0
