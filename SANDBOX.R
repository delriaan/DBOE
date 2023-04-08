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
db_conns$MySQL <- DBI::dbConnect(odbc::odbc(), "MySQL", user = "delriaan"
														, password = keyring::key_get(service = "MySQL", username = "delriaan", keyring = "R"))

db_conns$GW2DB <- DBI::dbConnect(RODBCDBI::ODBC(), "GW2DB"
														# , server = "imperialtower"
														# , database = "GW2DB"
														# , case = "nochange"
														, user = "imperial_agent"
														, password = keyring::key_get(service = "MSSQL", username = "imperial_agent", keyring = "R"))

.x <- db_conns[[2]];
.y <- names(db_conns)[[2]];
chatty <- TRUE;


# load_unloaded(DBOE)
source("pkg/R/DBOE.R")
X <- DBOE$new()
# debug(X$get.metadata)
X$get.metadata(!!!db_conns)
# X$GW2DB$dim_character |> head()
# GW2DB <- new.env()
# X$make.virtual_database("GW2DB", db_env = GW2DB)
# View(X)

purrr::walk(db_conns, dbDisconnect)
rm(db_conns)

