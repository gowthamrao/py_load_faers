# tests/integration/conftest.py
import os
from typing import Iterator

import pytest
from testcontainers.postgres import PostgresContainer

from py_load_faers.config import DatabaseSettings


@pytest.fixture(scope="session")
def db_settings() -> Iterator[DatabaseSettings]:
    """
    Provides database settings for integration tests.

    If running in a CI environment (CI=true), it connects to a database
    service defined by environment variables. Otherwise, it spins up a
    Postgres container using testcontainers.
    """
    if os.environ.get("CI") == "true":
        # CI environment: Use the service container
        settings = DatabaseSettings(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", 5432)),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "postgres"),
            dbname=os.environ.get("DB_NAME", "testdb"),
        )
        yield settings
    else:
        # Local environment: Use testcontainers
        with PostgresContainer(
            "postgres:13",
            username="user",
            password="password",
            dbname="test_db",
        ) as container:
            settings = DatabaseSettings(
                host=container.get_container_host_ip(),
                port=container.get_exposed_port(5432),
                user=container.username,
                password=container.password,
                dbname=container.dbname,
            )
            yield settings


@pytest.fixture(scope="function")
def clean_db(db_settings: DatabaseSettings):
    """
    Fixture to ensure the database is clean before each test function.
    It does this by dropping and re-creating all tables.
    """
    from py_load_faers.models import FAERS_TABLE_MODELS
    from py_load_faers.postgres.loader import PostgresLoader

    loader = PostgresLoader(db_settings)
    try:
        loader.connect()
        assert loader.conn is not None
        # Drop and re-create the schema to ensure a pristine state
        loader.initialize_schema(FAERS_TABLE_MODELS, drop_existing=True)
        loader.commit()
    finally:
        if loader.conn:
            loader.conn.close()

    yield  # The test runs here
