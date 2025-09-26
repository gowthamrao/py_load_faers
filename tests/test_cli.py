# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the CLI module.
"""
import logging
import runpy
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from py_load_faers.cli import app
from py_load_faers.config import (
    AppSettings,
    DatabaseSettings,
    DownloaderSettings,
    ProcessingSettings,
)

runner = CliRunner()


@pytest.fixture
def mock_config() -> AppSettings:
    """Fixture for a mock config object."""
    return AppSettings(
        db=DatabaseSettings(type="postgresql", user="test", password="test", dbname="test"),
        downloader=DownloaderSettings(download_dir="/tmp/faers"),
        processing=ProcessingSettings(),
    )


def test_download_specific_quarter(mock_config: AppSettings, caplog) -> None:
    """Test the download command with a specific quarter."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.downloader.download_quarter"
    ) as mock_download:
        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["download", "--quarter", "2023q1"])
        assert result.exit_code == 0
        assert "Download process finished" in caplog.text
        mock_download.assert_called_once_with("2023q1", mock_config.downloader)


def test_download_latest_quarter(mock_config: AppSettings, caplog) -> None:
    """Test the download command for the latest quarter."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.downloader.find_latest_quarter", return_value="2023q2"
    ) as mock_find, patch(
        "py_load_faers.cli.downloader.download_quarter"
    ) as mock_download:
        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["download"])
        assert result.exit_code == 0
        assert "No quarter specified" in caplog.text
        mock_find.assert_called_once()
        mock_download.assert_called_once_with("2023q2", mock_config.downloader)


def test_download_no_quarter_found(mock_config: AppSettings, caplog) -> None:
    """Test download when no quarter is specified and none is found."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.downloader.find_latest_quarter", return_value=None
    ):
        with caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["download"])
        assert result.exit_code == 1
        assert "Could not determine a quarter to download" in caplog.text


def test_run_delta_mode(mock_config: AppSettings) -> None:
    """Test the run command in delta mode."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ), patch("py_load_faers.cli.FaersLoaderEngine") as mock_engine:
        mock_engine.return_value.run_load.return_value = (True, "DQ checks passed.")
        result = runner.invoke(app, ["run", "--mode", "delta"])
        assert result.exit_code == 0
        assert "ETL process completed. DQ checks passed." in result.stdout
        mock_engine.return_value.run_load.assert_called_once_with(
            mode="delta", quarter=None
        )


def test_run_partial_mode(mock_config: AppSettings) -> None:
    """Test the run command in partial mode."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ), patch("py_load_faers.cli.FaersLoaderEngine") as mock_engine:
        mock_engine.return_value.run_load.return_value = (True, "DQ checks passed.")
        result = runner.invoke(app, ["run", "--mode", "partial", "--quarter", "2023q1"])
        assert result.exit_code == 0
        mock_engine.return_value.run_load.assert_called_once_with(
            mode="partial", quarter="2023q1"
        )


def test_run_no_new_data(mock_config: AppSettings) -> None:
    """Test the run command when no new data is found."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ), patch("py_load_faers.cli.FaersLoaderEngine") as mock_engine:
        mock_engine.return_value.run_load.return_value = None
        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "no new data to load" in result.stdout


def test_run_unsupported_db(mock_config: AppSettings) -> None:
    """Test the run command with an unsupported database type."""
    mock_config.db.type = "sqlite"
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config):
        result = runner.invoke(app, ["run"])
        assert result.exit_code == 1
        assert "Unsupported database type: sqlite" in result.stdout


def test_run_exception(mock_config: AppSettings) -> None:
    """Test exception handling in the run command."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        mock_loader.return_value.connect.side_effect = Exception("Connection failed")
        result = runner.invoke(app, ["run"])
        assert result.exit_code == 1
        assert "An error occurred during the ETL process: Connection failed" in result.stdout


def test_db_init(mock_config: AppSettings, caplog) -> None:
    """Test the db-init command."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["db-init"])
        assert result.exit_code == 0
        assert "Database schema initialization complete" in caplog.text
        mock_loader.return_value.initialize_schema.assert_called_once()
        mock_loader.return_value.commit.assert_called_once()


def test_db_init_unsupported_db(mock_config: AppSettings, caplog) -> None:
    """Test db-init with an unsupported database."""
    mock_config.db.type = "mysql"
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config):
        with caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["db-init"])
        assert result.exit_code == 1
        assert "Unsupported database type: mysql" in caplog.text


def test_db_init_exception(mock_config: AppSettings, caplog) -> None:
    """Test exception handling in db-init."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        mock_loader.return_value.connect.side_effect = Exception("Connection failed")
        with caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["db-init"])
        assert result.exit_code == 1
        assert "An error occurred during database initialization" in caplog.text
        mock_loader.return_value.rollback.assert_called_once()


def test_db_verify(mock_config: AppSettings) -> None:
    """Test the db-verify command with passing checks."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        mock_loader.return_value.run_post_load_dq_checks.return_value = (
            True,
            "All DQ checks passed.",
        )
        result = runner.invoke(app, ["db-verify"])
        assert result.exit_code == 0
        assert "All DQ checks passed." in result.stdout


def test_db_verify_failed_checks(mock_config: AppSettings) -> None:
    """Test the db-verify command with failing checks."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        mock_loader.return_value.run_post_load_dq_checks.return_value = (
            False,
            "Duplicates found.",
        )
        result = runner.invoke(app, ["db-verify"])
        assert result.exit_code == 1
        assert "Duplicates found." in result.stdout


def test_db_verify_unsupported_db(mock_config: AppSettings) -> None:
    """Test db-verify with an unsupported database."""
    mock_config.db.type = "oracle"
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config):
        result = runner.invoke(app, ["db-verify"])
        assert result.exit_code == 1
        assert "Unsupported database type: oracle" in result.stdout


def test_db_verify_exception(mock_config: AppSettings) -> None:
    """Test exception handling in db-verify."""
    with patch("py_load_faers.cli.config.load_config", return_value=mock_config), patch(
        "py_load_faers.cli.PostgresLoader"
    ) as mock_loader:
        mock_loader.return_value.connect.side_effect = Exception("DB error")
        result = runner.invoke(app, ["db-verify"])
        assert result.exit_code == 1
        assert "Data quality verification failed: DB error" in result.stdout


def test_main_app_entrypoint() -> None:
    """Test that the main app entrypoint runs and exits when no command is given."""
    with patch("sys.argv", ["cli.py"]):
        with pytest.raises(SystemExit) as e:
            runpy.run_module("py_load_faers.cli", run_name="__main__")
        assert e.type == SystemExit
        # Typer/Click exits with 2 on usage error (e.g., missing command)
        assert e.value.code == 2