# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the downloader module.
"""
import errno
import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from pytest_mock import MockerFixture

from py_load_faers import downloader
from py_load_faers.config import DownloaderSettings

# Sample HTML content from the FDA FAERS website for mocking
SAMPLE_HTML = """
<html>
<body>
    <a href="/content/Exports/faers_ascii_2024q4.zip">2024Q4 ASCII</a>
    <a href="/content/Exports/faers_xml_2025q1.zip">2025Q1 XML</a>
    <a href="/content/Exports/faers_ascii_2025q1.zip">2025Q1 ASCII</a>
    <a href="/content/Exports/faers_ascii_2024q3.zip">2024Q3 ASCII</a>
</body>
</html>
"""


def test_find_latest_quarter(mocker: MockerFixture) -> None:
    """Test that the latest quarter is correctly parsed from the FDA website HTML."""
    # Mock the requests.get call
    mock_response = MagicMock()
    mock_response.content = SAMPLE_HTML.encode("utf-8")
    mock_response.raise_for_status.return_value = None
    mocker.patch("requests.Session.get", return_value=mock_response)

    latest_quarter = downloader.find_latest_quarter()
    assert latest_quarter == "2025q1"


def test_find_latest_quarter_no_links(mocker: MockerFixture) -> None:
    """Test behavior when no download links are found on the page."""
    mock_response = MagicMock()
    mock_response.content = b"<html><body>No links here.</body></html>"
    mocker.patch("requests.Session.get", return_value=mock_response)

    assert downloader.find_latest_quarter() is None


def test_download_quarter(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test the download, verification, and checksum calculation of a quarter file."""
    # Create a valid in-memory zip file for the mock download
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.txt", "this is a test file")
    zip_content = zip_buffer.getvalue()

    # Mock the requests.get call to simulate a download
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = str(len(zip_content))
    mock_response.iter_content.return_value = [zip_content]
    mocker.patch("requests.Session.get", return_value=mock_response)

    settings = DownloaderSettings(download_dir=str(tmp_path), retries=3, timeout=60)
    quarter_to_download = "2025q1"

    result = downloader.download_quarter(quarter_to_download, settings)
    assert result is not None
    result_path, result_checksum = result

    expected_path = tmp_path / f"faers_ascii_{quarter_to_download}.zip"
    assert result_path == expected_path
    assert expected_path.exists()
    assert expected_path.read_bytes() == zip_content
    # Check that the checksum is a 64-character hex string (SHA-256)
    assert isinstance(result_checksum, str)
    assert len(result_checksum) == 64
    assert all(c in "0123456789abcdef" for c in result_checksum)


def test_download_quarter_corrupted_file_is_deleted(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a corrupted (non-zip) downloaded file is deleted."""
    # Mock requests.get to simulate a successful download of some data
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = "123"
    mock_response.iter_content.return_value = [b"corrupted data"]
    mocker.patch("requests.Session.get", return_value=mock_response)

    # Mock zipfile.is_zipfile to return False, simulating a corrupted file
    mocker.patch("zipfile.is_zipfile", return_value=False)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    quarter = "2025q1"
    filepath = tmp_path / f"faers_ascii_{quarter}.zip"

    result = downloader.download_quarter(quarter, settings)

    assert result is None
    assert not filepath.exists()


def test_download_quarter_zero_byte_file_is_deleted(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a zero-byte downloaded file is treated as invalid and deleted."""
    # Mock requests.get to simulate a download of an empty file
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = "0"
    mock_response.iter_content.return_value = [b""]
    mocker.patch("requests.Session.get", return_value=mock_response)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    quarter = "2025q1"
    filepath = tmp_path / f"faers_ascii_{quarter}.zip"

    result = downloader.download_quarter(quarter, settings)

    assert result is None
    assert not filepath.exists()


@pytest.mark.parametrize(
    "status_code",
    [404, 500, 503],
)
def test_download_quarter_network_error(
    mocker: MockerFixture, tmp_path: Path, status_code: int
) -> None:
    """Test that network errors are handled gracefully."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} Client Error")
    mocker.patch("requests.Session.get", return_value=mock_response)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    result = downloader.download_quarter("2025q1", settings)
    assert result is None


def test_download_quarter_permission_error(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a PermissionError during file writing is handled."""
    # Mock requests.get to simulate a successful download
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = "123"
    mock_response.iter_content.return_value = [b"some data"]
    mocker.patch("requests.Session.get", return_value=mock_response)

    # Mock open to raise a PermissionError
    mocker.patch("builtins.open", side_effect=PermissionError("Permission denied"))

    settings = DownloaderSettings(download_dir=str(tmp_path))
    result = downloader.download_quarter("2025q1", settings)
    assert result is None


def test_download_quarter_os_error_cleanup(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that an OSError (e.g., disk full) during file writing is handled."""
    # Mock requests.get to simulate a successful download
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = "123"
    mock_response.iter_content.return_value = [b"some data"]
    mocker.patch("requests.Session.get", return_value=mock_response)

    # Mock open to raise an OSError simulating a disk full error
    mock_open = mocker.mock_open()
    mock_open.side_effect = OSError(errno.ENOSPC, "No space left on device")
    mocker.patch("builtins.open", mock_open)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    quarter = "2025q1"
    filepath = tmp_path / f"faers_ascii_{quarter}.zip"

    result = downloader.download_quarter(quarter, settings)

    assert result is None
    # The key check is that the exception is caught and doesn't propagate.
    # We also want to ensure no partial file is left, but since `open` itself
    # is mocked to fail, no file is ever created.
    assert not filepath.exists()


def test_download_quarter_timeout_cleanup(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a download timeout is handled and the partial file is cleaned up."""
    # Mock requests.get to raise a Timeout exception
    mocker.patch("requests.Session.get", side_effect=requests.Timeout)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    quarter = "2025q1"
    filepath = tmp_path / f"faers_ascii_{quarter}.zip"

    # Create a dummy file to ensure it gets deleted
    filepath.touch()

    result = downloader.download_quarter(quarter, settings)

    assert result is None
    assert not filepath.exists()


def test_download_quarter_non_ascii_path(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test downloading to a path with non-ASCII characters."""
    # Create a subdirectory with a non-ASCII name
    non_ascii_dir = tmp_path / "tést"
    non_ascii_dir.mkdir()

    # Create a valid in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.txt", "this is a test file")
    zip_content = zip_buffer.getvalue()

    # Mock the download
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = str(len(zip_content))
    mock_response.iter_content.return_value = [zip_content]
    mocker.patch("requests.Session.get", return_value=mock_response)

    settings = DownloaderSettings(download_dir=str(non_ascii_dir))
    quarter = "2025q1"

    result = downloader.download_quarter(quarter, settings)

    assert result is not None
    result_path, _ = result
    expected_path = non_ascii_dir / f"faers_ascii_{quarter}.zip"
    assert result_path == expected_path
    assert expected_path.exists()
    assert expected_path.read_bytes() == zip_content


def test_download_quarter_preexisting_file(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test that a pre-existing file is overwritten."""
    # Create a dummy file that will be "overwritten"
    quarter = "2025q1"
    preexisting_file = tmp_path / f"faers_ascii_{quarter}.zip"
    preexisting_file.write_text("old content")

    # Mock a successful download
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.txt", "new content")
    zip_content = zip_buffer.getvalue()

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers.get.return_value = str(len(zip_content))
    mock_response.iter_content.return_value = [zip_content]
    mocker.patch("requests.Session.get", return_value=mock_response)

    settings = DownloaderSettings(download_dir=str(tmp_path))
    result = downloader.download_quarter(quarter, settings)

    assert result is not None
    result_path, _ = result
    assert result_path.read_bytes() == zip_content
