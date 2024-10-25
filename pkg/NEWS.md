# DBOE 2.3.0

## Breaking Changes

- **`get.metadata()`** and **`make.virtual_database()`** are now standalone functions instead of class methods. Each has been re-written and simplified with largely the same function signatures as previous versions.

## Updates

- Added function **`make.db_connection()`**, a DBI connection helper function