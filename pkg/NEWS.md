# DBOE 2.2.9

## Updated

- DBOE class method `make.virtual_database()`: 
  Added parameter *include.procs* to allow the scanning of stored procedures to be "opt-in". This was done to accomodate situations where the permissions to read certain metadata objects from the database have not been granted.
  