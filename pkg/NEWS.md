---
toc-title: Table of contents
---

# DBOE Version 2.3

## 2.3.7

### Updates

-   `get.metadata()` : Now accepts specific connection object names as
    arguments to the dots list. When provided, only metadata for those
    connections will be returned. When not provided, metadata for all
    connections will be returned (the default behavior).
-   `make.virtual_database()` : Arguments to `...` will be flattened --
    names follow the behavior of `unlist()`.

### Bug Fix

-   `DBOE$reconnect()` : Corrected connection reinitialization code.

## 2.3.6

### Updates

-   Added unexported S7 class `dboe_lazy_table()`
-   `get.metadata()` : Now creates `dboe_lazy_table()` objects in the
    connection environment.
-   `make.virtual_database()`:
    -   Added argument `lazy`. When `TRUE` (the default),
        `dboe_lazy_table()` objects are automatically included.
        Arguments to the dots list with matching names in the connection
        environment are overriden.
    -   Arguments to `...` will be flattened into an unnamed list
        (documentation updated accordingly).

### Bug Fix

-   `%look.for%`: Matching column names now return

## 2.3.5

### Updates

-   `make.virtual_database()`: Now accepts qualified names (e.g.,
    "catalog.schema.table") for improved flexibility in multi-database
    environments
-   Removed DuckDB and SQLite from supported database engines and
    package Imports
-   Dependencies refined: added `dbplyr (>= 2.5.0)` for enhanced data
    manipulation
-   `make.db_connection()`: Internal refactoring of variable naming for
    consistency

## 2.3.4

### Updates

-   Dependencies simplified: Removed DuckDB and RSQLite from Imports
    (MSSQL and MySQL remain primary supported engines)
-   `get.metadata()`: Refactored internal logic for improved clarity and
    maintainability
-   `look.for()`: Simplified group column intersection logic
-   Package documentation updated to reflect narrowed database engine
    support

## 2.3.3

### Updates

-   `DBOE`: Removed methods `get.metadata()` and
    `make.virtual_database()` from public methods
-   `make.db_connection()`: Returned object has attribute "odbc_string"
-   `make.virtual_database()`: The "dots-list" now accepts qualified
    name (e.g., "catalog.schema.table")

## 2.3.2

### Updates

-   `get.metadata()`: Enforce upper-case on column names of the response
    after querying `INFORMATION_SCHEMA.COLMUMNS`
-   `make.virtual_database()`: Added support for DuckDB (on-disk and
    in-memory)

## 2.3.1

### Bug Fixes

-   `make.virtual_database()`: Set the default value for argument
    `exclude` to `NA`.

## 2.3.0

### Breaking Changes

-   `get.metadata()` and `make.virtual_database()` are now standalone
    functions instead of class methods. Each has been re-written and
    simplified with largely the same function signatures as previous
    versions.

### Updates

-   Added function `make.db_connection()`, a DBI connection helper
    function
