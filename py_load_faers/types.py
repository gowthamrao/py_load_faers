# -*- coding: utf-8 -*-
# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
This module defines common types and enums used across the application.
"""
from enum import Enum


class RunMode(str, Enum):
    """Enumeration for the different modes the ETL can run in."""

    DELTA = "delta"
    PARTIAL = "partial"


class FAERSFileType(str, Enum):
    """Enumeration for the types of FAERS data files."""

    XML = "xml"
    ASCII = "ascii"


class StagingFormat(str, Enum):
    """Enumeration for the intermediate staging file formats."""

    CSV = "csv"
    PARQUET = "parquet"
