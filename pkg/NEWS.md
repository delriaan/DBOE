# DBOE 2.3.2

## Updates
- `get.metadata()`: Enforce upper-case on column names of the response after querying `INFORMATION_SCHEMA.COLMUMNS`
- `make.virtual_database()` Added support for DuckDB (on-disk and in-memory)

# DBOE 2.3.1

## Bug Fixes

- __`make.virtual_database()`__: Set the default value for argument `exclude` to `NA`.

# DBOE 2.3.0

## Breaking Changes

- __`get.metadata()`__ and __`make.virtual_database()`__ are now standalone functions instead of class methods. Each has been re-written and simplified with largely the same function signatures as previous versions.

## Updates

- Added function __`make.db_connection()`__, a DBI connection helper function