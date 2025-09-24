# -*- coding: utf-8 -*-
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
