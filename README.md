# py_load_faers

`py_load_faers` is a Python package for downloading, processing, and loading the FDA Adverse Event Reporting System (FAERS) data into a database.

## Project Overview

The FDA Adverse Event Reporting System (FAERS) is a database that contains information on adverse event and medication error reports submitted to FDA. The database is designed to support the FDA's post-marketing safety surveillance program for drug and therapeutic biologic products.

This package provides a command-line interface (CLI) to automate the process of downloading the quarterly FAERS data files, processing them, and loading them into a database.

## Key Features

*   Download quarterly FAERS data files from the FDA website.
*   Support for both ASCII and XML data formats.
*   Process and load data into a PostgreSQL database.
*   Deduplication of FAERS data based on FDA's recommendations.
*   Handle nullified cases.

## Installation

To install the package, you can use `pip`:

```bash
pip install py_load_faers
```

## Configuration

The package can be configured using environment variables. The following variables are available:

*   `PY_LOAD_FAERS_DB__TYPE`: The type of database to use. Currently, only `postgresql` is supported.
*   `PY_LOAD_FAERS_DB__HOST`: The database host.
*   `PY_LOAD_FAERS_DB__PORT`: The database port.
*   `PY_LOAD_FAERS_DB__USER`: The database user.
*   `PY_LOAD_FAERS_DB__PASSWORD`: The database password.
*   `PY_LOAD_FAERS_DB__DBNAME`: The database name.
*   `PY_LOAD_FAERS_DOWNLOADER__DOWNLOAD_DIR`: The directory to download the FAERS data files to.
*   `PY_LOAD_FAERS_PROCESSING__STAGING_FORMAT`: The format to use for staging the data. Can be `csv` or `parquet`.

## Usage

The package provides a CLI for managing the FAERS data loading process.

### Initialize the database

To initialize the database schema, run the `db-init` command:

```bash
py_load_faers db-init
```

### Run the ETL process

To run the full ETL process for a specific quarter, use the `run` command:

```bash
py_load_faers run --quarter 2023q1
```

To run the ETL process in delta mode (i.e., load all new quarters since the last successful load), use the `run` command with the `--mode delta` option:

```bash
py_load_faers run --mode delta
```

### Verify the data

To run data quality checks on the loaded data, use the `db-verify` command:

```bash
py_load_faers db-verify
```

## Development & Contributing

To contribute to the project, you can clone the repository and install the dependencies using `poetry`:

```bash
git clone https://github.com/your-username/py_load_faers.git
cd py_load_faers
poetry install
```

To run the tests, use `pytest`:

```bash
poetry run pytest
```

## License

This project is **Source-Available** and dual-licensed.

The software is available under the [Prosperity Public License 3.0.0](LICENSE.md). You may use the software for non-commercial purposes, or for a commercial trial period of up to 30 days.

Commercial use beyond the 30-day trial period requires a separate commercial license. Please contact Gowtham Adamane Rao for details.
