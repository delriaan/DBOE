library(book.of.workflow);
load_unloaded(data.table, purrr, stringi, httr, DBI, xml2, keyring, lubridate, tictoc, book.of.features)

# ~VALIDATION ====

tic.clear(); tic.clearlog();
dsn <- readline("Enter the DSN: ");
user
db <- dbConnect(odbc::odbc(), );

# PASS 2021.08.26
tic("Unit test: new(dsn, credentials)");{
	dbms.monitor <- DBOE$new(db)
}; toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $get.metadata()"); dbms.monitor$get.metadata(); toc(log = TRUE)

# pkgdown::build_site(pkg = "pkg", override = list(destination = "../docs"))
