# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Tests for the database abstract base class.
"""
from pathlib import Path
import pytest
from py_load_faers.database import AbstractDatabaseLoader


def test_abstract_methods_raise_not_implemented_error() -> None:
    """
    Verify that the abstract methods in AbstractDatabaseLoader raise
    NotImplementedError as they are not implemented in the base class.
    This test calls the methods on the class directly to test the base implementation.
    """
    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.connect(self=None)  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.begin_transaction(self=None)  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.commit(self=None)  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.rollback(self=None)  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.initialize_schema(self=None, schema_definition={})  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.execute_native_bulk_load(
            self=None, table_name="test", file_path=Path("test")  # type: ignore
        )

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.execute_deletions(self=None, case_ids=[])  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.handle_delta_merge(
            self=None, caseids_to_upsert=[], data_sources={}  # type: ignore
        )

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.update_load_history(self=None, metadata={})  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.get_last_successful_load(self=None)  # type: ignore

    with pytest.raises(NotImplementedError):
        AbstractDatabaseLoader.run_post_load_dq_checks(self=None)  # type: ignore