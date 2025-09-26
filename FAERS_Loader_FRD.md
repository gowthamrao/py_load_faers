# Functional Requirements Document (FRD): FAERS Data Loader

**Version:** 1.0
**Date:** 2025-09-25
**Status:** Draft

---

## 1. Introduction

### 1.1 Purpose and Objectives
This document outlines the functional requirements for a new Python package, `faers-loader`. The primary objective of this package is to provide a high-performance, cloud-agnostic, and extensible solution for downloading, processing, and loading U.S. Food and Drug Administration (FDA) Adverse Event Reporting System (FAERS) data into relational and analytical databases.

The core design principles are:
*   **Performance:** Utilize native bulk loading mechanisms of target databases to achieve maximum data ingestion speed, bypassing inefficient row-by-row insertion methods.
*   **Extensibility:** Architect the system with a modular plugin-based approach, allowing developers to easily add support for new database backends without modifying the core package.
*   **Correctness:** Implement the official FDA-recommended deduplication logic to ensure data integrity and accurately represent the case-version nature of FAERS data.
*   **Automation:** Provide robust mechanisms for automatically fetching, processing, and loading both historical and incremental (delta) quarterly data releases.

### 1.2 Scope

#### In-Scope
*   Automated download of FAERS quarterly data files (ASCII, XML, JSON) from the official FDA source.
*   Parsing of all three official file formats.
*   Implementation of full (historical) and delta (incremental) loading strategies.
*   Implementation of the FDA-recommended deduplication logic based on `PRIMARYID`, `CASEID`, and `FDA_DT`.
*   Creation and management of relational schemas for the 7 core FAERS tables.
*   Native, high-performance bulk loading into a target database.
*   A default, production-ready implementation for PostgreSQL.
*   An extensible architecture (plugin system) for adding other database targets.
*   Optional, modular functionality for data standardization (e.g., drug name normalization to RxNorm).
*   Configuration via environment variables and/or a configuration file.
*   Execution via a Command Line Interface (CLI) and a programmatic Python API.
*   Comprehensive logging, testing, and documentation.

#### Out-of-Scope
*   Advanced data analysis, visualization, or signal detection on the loaded FAERS data.
*   A user interface (UI) for managing the loading process.
*   Real-time data streaming from sources other than the FDA's quarterly files.
*   Management of the underlying database infrastructure.

### 1.3 Target Audience
*   **Data Engineers & ETL Developers:** Users who will install, configure, and run the package to populate their databases.
*   **Pharmacovigilance Data Analysts:** Users who will consume the data loaded by this package.
*   **Platform Developers:** Developers who may extend the package by creating new plugins for different database backends.

### 1.4 Glossary of Terms
*   **FAERS:** FDA Adverse Event Reporting System. A database containing adverse event reports, medication error reports, and product quality complaints.
*   **ICH E2B:** An international safety reporting standard for the electronic transmission of Individual Case Safety Reports (ICSRs). FAERS XML files are compliant with this standard.
*   **MedDRA:** Medical Dictionary for Regulatory Activities. The standard terminology used for coding adverse event terms in FAERS.
*   **RxNorm:** A standardized nomenclature for clinical drugs produced by the U.S. National Library of Medicine (NLM). Used for drug name standardization.
*   **Native Bulk Load:** The most performant method for loading large volumes of data into a database, typically by using a database-specific utility (e.g., `COPY` in PostgreSQL, `bcp` in SQL Server) that bypasses the SQL engine for direct data file ingestion.
*   **Delta Load:** An incremental load that processes only new or updated data since the last run, as opposed to a full load of the entire dataset.
*   **PRIMARYID:** A unique identifier for each version of a case report in FAERS.
*   **CASEID:** A non-unique identifier for a case. Multiple reports (versions) can share the same `CASEID`.

---

## 2. Data Acquisition and Source Management

### 2.1 Data Source
The system must acquire data exclusively from the official FDA FAERS Quarterly Data Extract Files page.

### 2.2 Automated Download Mechanism
*   The package must be able to automatically detect the latest available quarterly data files from the FDA website.
*   Downloads must be resumable to handle network interruptions.
*   The system must verify the integrity of downloaded files using the checksums (MD5) provided by the FDA.
*   The download mechanism must be configurable to fetch a specific range of quarters or all historical data.

### 2.3 File Format Handling
*   The system must be capable of parsing all three FAERS data formats:
    *   **ASCII:** Standard $-delimited text files.
    *   **XML:** ICH E2B compliant XML files.
    *   **JSON:** As provided by sources like OpenFDA or future FDA pilots.
*   The parsing logic must be robust to minor format variations and encoding issues (e.g., Latin-1 vs. UTF-8).

### 2.4 Intermediate Representation
*   After parsing, the raw data must be converted into a standardized, efficient intermediate format before being loaded into the target database.
*   This format must be optimized for bulk loading operations. Apache Parquet is the recommended format due to its columnar nature, compression, and schema support. Optimized, well-typed CSV is an acceptable alternative.
*   This intermediate representation decouples the parsing logic from the loading logic, facilitating extensibility.

---

## 3. Data Processing and Structure

### 3.1 Deduplication and Versioning Strategy
*   The system **must** implement the official FDA deduplication logic to ensure only the latest version of each case is present in the final "latest" tables.
*   The logic is as follows: For each `CASEID`, the record with the most recent `FDA_DT` (FDA receipt date) is selected. If multiple records share the same `CASEEID` and `FDA_DT`, the one with the highest `PRIMARYID` is chosen as the latest version.
*   The system should provide an option to either load only the latest versions or to load all historical versions into a separate, versioned schema for audit and research purposes.

### 3.2 Data Cleaning
*   The system must perform basic data cleaning, including:
    *   Standardizing character encoding to UTF-8.
    *   Handling malformed records or lines gracefully (e.g., by logging and skipping them).
    *   Normalizing date fields, including handling partial dates (e.g., `2023` -> `2023-01-01`, `202301` -> `2023-01-01`) in a configurable and deterministic manner.

### 3.3 Data Representations

#### 3.3.1 Raw Representation
*   The primary output of the loader will be a direct relational mapping of the 7 core FAERS tables: `DEMO`, `DRUG`, `REAC`, `OUTC`, `RPSR`, `THER`, and `INDI`.
*   The system will define a standardized schema for these tables, including appropriate data types (e.g., `INTEGER`, `VARCHAR`, `DATE`), primary keys, and foreign key relationships where applicable.

#### 3.3.2 Standardized Representation (Optional Module)
*   The package must support an optional, pluggable module for creating an enriched, standardized representation of the data. This module will not be part of the core package but can be installed as an extension.
*   Requirements for this module include:
    *   **Drug Normalization:** Mapping reported drug names to standardized concepts from RxNorm at the single active ingredient level.
    *   **Age Normalization:** Converting age fields (`AGE`, `AGE_COD`) into a single, normalized numeric age in years.
    *   **Country Standardization:** Mapping country codes to the ISO 3166-1 alpha-2 standard.

### 3.4 Metadata Management

#### 3.4.1 Source Metadata
*   The system must capture and store metadata about the source data, including the FAERS release quarter (e.g., `2023Q4`), download timestamp, and links to the original data dictionaries.

#### 3.4.2 Process Metadata
*   A dedicated metadata schema must be maintained to track the state of the loading process. This is critical for enabling robust delta loads.
*   This metadata must include:
    *   Load history for each quarter.
    *   File checksums of processed files.
    *   Row counts (pre- and post-deduplication).
    *   Load status (e.g., `STARTED`, `COMPLETED`, `FAILED`).
    *   Timestamps for each processing stage.

---

## 4. Data Loading Mechanism

### 4.1 Load Strategies
The system must support the following loading strategies:
*   **Full Load:** A complete historical load, wiping the target tables and reloading all data from scratch.
*   **Delta Load:** An incremental load that identifies the last successfully loaded quarter from the process metadata and loads only subsequent new quarters.
*   **Partial Reload:** An ability to reload specific, user-defined quarters of data.

### 4.2 Native Performance Requirement
*   The use of native bulk loading utilities is **mandatory** for all data loading operations.
*   Standard ORM or row-by-row `INSERT` statements are explicitly forbidden for loading the primary data tables due to their poor performance with large datasets.

### 4.3 Memory Efficiency
*   The system must be designed to handle multi-gigabyte FAERS files without requiring excessive memory.
*   This shall be achieved through data chunking or streaming during both parsing and loading phases.

### 4.4 Transaction Management and Atomicity
*   Each quarterly load must be executed within a single database transaction.
*   The system must ensure that a quarter is either loaded completely or fully rolled back in the event of a failure, preventing partial data states. A staging table mechanism is recommended for this purpose.

### 4.5 Schema Management
*   The system must be capable of automatically creating the required schemas and tables in the target database if they do not exist.
*   The system should also support schema evolution (e.g., adding a new column) in a non-destructive way, though this is a secondary requirement.

---

## 5. Architecture and Extensibility (Cloud Agnostic Design)

### 5.1 Core Architecture
The package will be architected in a modular fashion, separating the core logic (downloading, parsing, deduplication) from the database-specific loading logic.
*   **Core Package (`faers-loader`):** Contains all database-agnostic functionality.
*   **Database Extensions (`faers-loader-<db>`):** Separate, installable packages that provide implementations for specific database backends.

### 5.2 The Loader Interface (Database Abstraction Layer)
*   A Python Abstract Base Class (ABC) named `BaseLoader` will define the contract that all database connectors must implement.
*   The `BaseLoader` interface must define abstract methods for:
    *   `__init__(self, config)`: Initializing with database-specific connection details.
    *   `connect(self)`: Establishing a connection.
    *   `prepare_schema(self)`: Creating necessary schemas and tables.
    *   `load_data(self, intermediate_file_path, target_table)`: Executing the native bulk load operation from an intermediate file.
    *   `execute_transactional_script(self, sql_script)`: Running post-load transformations (like deduplication) in a transaction.
    *   `close(self)`: Closing the connection.

### 5.3 Default Implementation: PostgreSQL
*   The package will ship with a default, built-in implementation for PostgreSQL.
*   This implementation **must** use the `COPY FROM STDIN` protocol for maximum efficiency, likely via the `psycopg` library.
*   It must provide configuration options for `COPY` parameters, such as buffer size and delimiter handling.

### 5.4 Extension/Plugin System
*   The system will use Python's native `entry_points` mechanism (defined in `pyproject.toml`) to discover and register available database loader extensions.
*   When a user runs the loader, they will specify a target (e.g., `postgres`, `redshift`). The system will look up the corresponding entry point and instantiate the correct `BaseLoader` implementation.
*   Guidelines for extension developers will be provided, detailing how to implement native loading for their target database. For example:
    *   A `faers-loader-redshift` extension would implement `load_data` by first uploading the intermediate Parquet file to an S3 bucket and then issuing a `COPY` command to Redshift.

---

## 6. Configuration and Usability

### 6.1 Configuration Management
*   All aspects of the package must be configurable. The order of precedence for configuration will be:
    1.  Environment variables (e.g., `FAERS_DB_PASSWORD`).
    2.  A YAML configuration file (e.g., `config.yml`).
    3.  Default values.
*   The system must provide a mechanism for securely handling database credentials, such as reading them from environment variables or a secure vault system (integration is out of scope, but the design should not preclude it).

### 6.2 Execution Interfaces

#### 6.2.1 CLI
*   A user-friendly CLI will be the primary interface for running the loader.
*   Core commands will include:
    *   `faers-loader run --mode [full|delta] --target [postgres|redshift|...]`
    *   `faers-loader download --quarters 2023Q1 2023Q2`
    *   `faers-loader configure`

#### 6.2.2 Python API
*   A clean, well-documented Python API must be provided to allow for programmatic execution and integration into orchestration tools like Airflow, Dagster, or Prefect.
*   Example API usage:
    ```python
    from faers_loader import FAErsLoader
    loader = FAErsLoader(config_path="config.yml")
    loader.run(mode="delta")
    ```

### 6.3 Logging and Monitoring
*   The system must implement comprehensive logging using Python's standard `logging` module.
*   Log levels (DEBUG, INFO, WARNING, ERROR) must be configurable.
*   The option for structured (JSON) logging must be available to facilitate integration with modern log analysis platforms.

---

## 7. Testing, Maintenance, and Deployment

### 7.1 Testing Strategy
*   **Unit Tests:** A comprehensive suite of unit tests using `pytest` will cover all core logic, including parsing functions, data cleaning rules, and the deduplication algorithm.
*   **Integration Tests:** The testing strategy **must** include integration tests that validate the end-to-end loading process for each supported database backend. These tests will use containerized databases (e.g., via Testcontainers or Docker Compose) to create ephemeral database instances.
*   **Data Quality Checks:** Automated checks will be run post-load to verify data integrity, such as checking for null primary keys and ensuring row counts match expectations.

### 7.2 Packaging and Distribution
*   The package will be managed using a modern Python packaging tool like Poetry or Hatch.
*   All dependencies and project metadata will be defined in `pyproject.toml`.
*   The package will be distributable via PyPI.

### 7.3 Code Quality and CI/CD
*   A Continuous Integration/Continuous Deployment (CI/CD) pipeline (e.g., using GitHub Actions) is required.
*   The pipeline must enforce:
    *   **Static Type Checking:** All code must be fully type-hinted and pass `mypy --strict`.
    *   **Linting:** Code must adhere to standards checked by `Ruff`.
    *   **Formatting:** Code must be formatted using `Black`.
    *   **Automated Testing:** All tests must pass before a change can be merged.

### 7.4 Documentation
*   **User Guide:** Detailed instructions on how to install, configure, and run the `faers-loader`.
*   **Developer Guide:** Instructions for setting up a development environment and contributing to the project.
*   **Extension Guide:** A specific guide for developers wishing to create new database loader plugins, including a detailed explanation of the `BaseLoader` interface and the plugin system.
*   **API Reference:** Auto-generated documentation for the public Python API.