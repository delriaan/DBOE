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


# load_unloaded(DBOE)
source("pkg/R/DBOE.R")
X <- DBOE$new()
# debug(X$get.metadata)
X$get.metadata(!!!db_conns)
X$GW2DB$metamap[, unique(tbl_name)]
X$mysql$metamap[, unique(tbl_name)]

# undebug(X$make.virtual_database)
X$make.virtual_database(conn = "GW2DB", target_env = globalenv(), dim_items)
X$make.virtual_database(conn = "mysql", target_env = globalenv(), rental)

dim_items |> dplyr::select("id", "name")  |> dplyr::collect()

db_conns$MySQL |> dbGetQuery("SELECT * FROM mysql.help_relation LIMIT 10")
X$MySQL$metamap[(schema_name == "information_schema"), unique(tbl_name)]
db_conns$MySQL |> dbGetQuery("SELECT * FROM information_schema.columns LIMIT 10")
# ALL: database = TABLE_CATALOG| ROUTINE_CATALOG
# ALL: schema = TABLE_SCHEMA | ROUTINE_SCHEMA
# tables: TABLE_NAME, TABLE_TYPE, TABLE_ROWS, TABLE_COLLATION
# columns: TABLE_NAME:COLUMN_TYPE
# views: TABLE_NAME:VIEW_DEFINITION
# routines: ROUTINE_NAME, ROUTINE_BODY, ROUTINE_DEFINITION


X$make.virtual_database(conn = "MySQL", target_env = .GlobalEnv, memory_by_thread_by_current_bytes)
X$MySQL$category
actor |> dplyr::select("staff_id", "location")  |> dplyr::collect()

purrr::walk(db_conns, dbDisconnect)
rm(db_conns)

