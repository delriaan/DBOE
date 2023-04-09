library(book.of.workflow);
load_unloaded(purrr
	, stringi
	, magrittr
	, rlang
	, DBI
	, httr
	, keyring
	, tictoc
	, "data.table{+%like%}"
	)

db_conns <- list()
db_conns$mysql <- DBI::dbConnect(odbc::odbc(), "MySQL"
														, database = "mysql"
														, user = "delriaan"
														, password = keyring::key_get(service = "MySQL", username = "delriaan", keyring = "R"))

db_conns$GW2DB <- DBI::dbConnect(RODBCDBI::ODBC(), "GW2DB"
														# , server = "imperialtower"
														# , database = "GW2DB"
														# , case = "nochange"
														# , database = "GW2DB"
														, user = "imperial_agent"
														, password = keyring::key_get(service = "MSSQL", username = "imperial_agent", keyring = "R"))

source("pkg/R/DBOE.R")
X <- DBOE$new()
# debug(X$get.metadata)
X$get.metadata(!!!db_conns)

# undebug(X$make.virtual_database)
X$make.virtual_database(conn = "GW2DB", target_env = globalenv(), dim_items, character)
X$make.virtual_database(conn = "mysql", target_env = globalenv(), rental, staff)

purrr::walk(db_conns, dbDisconnect)
rm(db_conns)

