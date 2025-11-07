# Version 2.3

## 2.3.4

### Bug Fixes

-   `%look.for()`: Fixed a runtime error preventing matching column names from returning.

### Updates

-   `DBOE`, `get.metadata()`: Added support for SQLite.

## 2.3.3

### Updates

-   `DBOE`: Removed methods `get.metadata()` and `make.virtual_database()` from public methods
-   `make.db_connection()`: Returned object has attribute "odbc_string"
-   `make.virtual_database()`: The "dots-list" now accepts qualified name (e.g., "catalog.schema.table")

## 2.3.2

### Updates

-   `get.metadata()`: Enforce upper-case on column names of the response after querying `INFORMATION_SCHEMA.COLMUMNS`
-   `make.virtual_database()`: Added support for DuckDB (on-disk and in-memory)

## 2.3.1

### Bug Fixes

-   `make.virtual_database()`: Set the default value for argument `exclude` to `NA`.

## 2.3.0

### Breaking Changes

-   `get.metadata()` and `make.virtual_database()` are now standalone functions instead of class methods. Each has been re-written and simplified with largely the same function signatures as previous versions.

### Updates

-   Added function `make.db_connection()`, a DBI connection helper function