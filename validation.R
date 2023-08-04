 # VALIDATION ----
library(book.of.workflow);
load_unloaded(
	purrr
	, stringi
	, magrittr
	, rlang
	, DBI
	, httr
	, keyring
	, tictoc
	, "data.table{+%like%}"
	)
# library(DBOE)

.easy_pass_mssql <- book.of.utilities::kr_key(service = "MSSQL", username = "imperial_agent", keyring = "local")
.easy_pass_mysql <- book.of.utilities::kr_key(service = "MySQL", username = "delriaan", keyring = "local")

db_conns <- { list(
		mysql = DBI::dbConnect(odbc::odbc()
														, dsn = "MySQL"
														, server = "imperialtower"
														, database = "mysql"
														, user = "delriaan"
														, password = .easy_pass_mysql@get())

		, GW2DB = RODBCDBI::dbConnect(odbc::odbc()
														, dsn = "GW2DB"
														, case = "nochange"
														, uid = "imperial_agent"
														, pwd = .easy_pass_mssql@get()
														)
		)}

# source("pkg/R/DBOE.R")
X <- DBOE$new()

# debug(X$get.metadata)
X$get.metadata(!!!db_conns)

# undebug(X$make.virtual_database)
X$make.virtual_database(conn = "GW2DB", target_env = globalenv(), sch = dbo, dim_items, character, profession)

DBI::dbListTables(db_conns$GW2DB)
X$GW2DB %look.for% "dim|sys" |> unique()

dplyr::tbl(db_conns$GW2DB, "dim_items")


DBI::dbGetQuery(db_conns$mysql, "SELECT * FROM world.country LIMIT 10")
X$make.virtual_database(conn = "mysql", target_env = globalenv(), country, account)
X$mysql$metamap[(schema_name == "world")]

purrr::walk(db_conns, dbDisconnect)
rm(db_conns)

 # PKGDOWN ----
pkgdown::build_site(pkg = "pkg", override = list(destination = "../docs"))
