library(book.of.workflow);
load_unloaded(DBOE)
X <- DBOE$new(list(GW2DB = DBI::dbConnect(odbc::odbc(), "GW2DB")))
X$get.metadata()
X$GW2DB$dim_character |> head()
GW2DB <- new.env()
X$make.virtual_database("GW2DB", db_env = GW2DB)
View(X)
