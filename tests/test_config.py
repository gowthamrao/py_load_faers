# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the configuration module.
"""
import os
from pathlib import Path
import pytest
from pytest import MonkeyPatch
from py_load_faers.config import load_config, AppSettings

# A sample config file for testing
SAMPLE_CONFIG_YAML = """
dev:
  db:
    host: localhost
    port: 5432
    user: dev_user
    password: dev_password
    dbname: faers_dev
  downloader:
    download_dir: /tmp/dev_downloads
  log_level: DEBUG

prod:
  db:
    host: prod.db.server.com
    port: 5433
    user: prod_user
    password: prod_password
    dbname: faers_prod
"""


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Create a sample config.yaml file in a temporary directory."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(SAMPLE_CONFIG_YAML)
    return config_path


def test_load_config_from_file_and_profile(sample_config_file: Path) -> None:
    """Test that configuration is loaded correctly from a specific profile in the YAML file."""
    settings = load_config(profile="prod", config_file=str(sample_config_file))

    assert isinstance(settings, AppSettings)
    assert settings.db.host == "prod.db.server.com"
    assert settings.db.port == 5433
    assert settings.db.user == "prod_user"
    # These fields are not in the 'prod' profile, so they should have their default values
    assert settings.downloader.download_dir == "./downloads"
    assert settings.log_level == "INFO"


def test_load_config_with_env_var_override(
    sample_config_file: Path, monkeypatch: MonkeyPatch
) -> None:
    """Test that environment variables override settings from the config file."""
    # Set environment variables to override the 'dev' profile
    monkeypatch.setenv("PY_LOAD_FAERS_DB__HOST", "env_host")
    monkeypatch.setenv("PY_LOAD_FAERS_DB__PORT", "9999")
    monkeypatch.setenv("PY_LOAD_FAERS_LOG_LEVEL", "WARNING")

    settings = load_config(profile="dev", config_file=str(sample_config_file))

    assert settings.db.host == "env_host"
    assert settings.db.port == 9999
    assert settings.db.user == "dev_user"  # This should still come from the file
    assert settings.log_level == "WARNING"


def test_load_config_defaults_when_no_file() -> None:
    """Test that default settings are used when no config file is present."""
    # Assuming no config.yaml exists in the root of the test execution dir
    if os.path.exists("config.yaml"):
        os.remove("config.yaml")

    settings = load_config()

    assert isinstance(settings, AppSettings)
    assert settings.db.host == "localhost"
    assert settings.downloader.download_dir == "./downloads"
    assert settings.log_level == "INFO"


def test_appsettings_from_yaml_success(tmp_path: Path) -> None:
    """Test loading settings directly from a YAML file via the classmethod."""
    yaml_content = """
db:
  host: from_yaml
  port: 1234
log_level: CRITICAL
"""
    config_path = tmp_path / "direct.yaml"
    config_path.write_text(yaml_content)

    settings = AppSettings.from_yaml(config_path)
    assert settings.db.host == "from_yaml"
    assert settings.db.port == 1234
    assert settings.log_level == "CRITICAL"
    assert settings.db.user == "user"  # Check default


def test_appsettings_from_yaml_not_found() -> None:
    """Test that from_yaml raises FileNotFoundError for a missing file."""
    with pytest.raises(FileNotFoundError):
        AppSettings.from_yaml(Path("non_existent_config.yaml"))


def test_load_config_with_empty_yaml_file(tmp_path: Path) -> None:
    """Test that loading an empty YAML file results in default settings."""
    config_path = tmp_path / "empty.yaml"
    config_path.touch()
    settings = load_config(config_file=str(config_path))
    assert settings.db.host == "localhost"
    assert settings.log_level == "INFO"


def test_load_config_with_profile_not_found(sample_config_file: Path) -> None:
    """Test loading with a profile that doesn't exist in the file."""
    settings = load_config(profile="non_existent_profile", config_file=str(sample_config_file))
    # Should fall back to default values, not other profiles
    assert settings.db.host == "localhost"
    assert settings.db.user == "user"
