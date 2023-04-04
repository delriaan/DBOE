# Database Object Explorer

`DBOE` facilitates the navigation of SQL Server DBMS objects by way of retrieving the metatadata objects.

It also provides methods to retrieve information from the [enterprise data warehouse changelog](http://datacatalog.alliance.local/en/datacatalog/edwchangelog). Once retrieved, changes can be compared against user-supplied source code.

## Workflow
The workflow should start with `DBOE$new(dsn, credentials)` and followed by one or more methods listed below: 

- `$get.metadata()`

- `$get.xdep(db)`

- `$get.changelog()`
    - `$search.changelog(db, ...)`
    - `$crossref.code(code_files, use.search)`

`DBOE$report` must be called separately
