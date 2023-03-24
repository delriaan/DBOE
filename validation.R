library(book.of.workflow);
if (TRUE){ library(DBOE)} else { do.load_unloaded("data.table, purrr, stringi, httr, DBI, xml2, keyring, lubridate, tictoc, book.of.features") }

# ~VALIDATION ====
.params <- list(
    kr = readline("Enter the keyring to use for authentication: ")
    , dsn = readline("Enter DSNs (separated by space or comma) as DSN=database: ") %>%
        stri_split_regex("[ =,]", simplify = TRUE) %>% { enlist( .[(1:length(.) %% 2) == 0], .[(1:length(.) %% 2) != 0]) }
        # AMCS_DW_IC=AMCS_DW_IC DScience_DB=DScience_DB JIVA=Jiva_Interfaces
    , root  = readline(sprintf("Enter root directory to search for code cross-references <%s>: ", getwd()))
    , code_search_pattern = readline("Enter a pattern for source code search: ")
      # [#]{2}.*.(jiva|prtf|foster.*.multi|hedis)
    );

tic.clear(); tic.clearlog();
# PASS 2021.08.26
tic("Unit test: new(dsn, credentials)");{ dbms.monitor <- DBOE$new(dsn = .params$dsn, credentials = .params$kr) }; toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $get.metadata()"); dbms.monitor$get.metadata(); toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $get.xdep()"); { walk(.params$dsn, ~dbms.monitor$get.xdep(.x))}; toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $get.changelog(); `keyring` functionality"); dbms.monitor$get.changelog() %$% changelog %>% str; toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $search.changelog(retain = FALSE)|Chained invocation with different filters");{
  dbms.monitor$search.history %$% rm(list = ls());
  dbms.monitor$
    search.changelog(change_note %ilike% "hedis", db = "AMCS_DW_IC", retain = "hedis-related")$
    search.changelog(REMOVED, db = "AMCS_DW_IC", retain = "removed objects")$
    search.changelog(schedule == "FUTURE", db = "AMCS_DW_IC", retain = "future changes")$
    search.changelog(eff_date >= as.Date("2021-08-01"), db = "AMCS_DW_IC", retain = "2021-08")$
    search.changelog(change_note %ilike% "claim", db = "AMCS_DW_IC", retain = "claims-related")$
    search.history %>% ls;
}; toc(log = TRUE);

# PASS 2021.08.26
tic("Unit test: $crossref.code(code_files, retain, use.search, ... )"); {
  .my_code_files = dir(.params$root, pattern = "[.]R$", recursive = TRUE, full.names = TRUE, include.dirs = FALSE) %>%
    keep(~.x %ilike% .params$code_search_pattern);
  # dbms.monitor$code_xref.history %$% rm(list = ls());
 	dbms.monitor$crossref.code(code_files = .my_code_files, use.search = "c", chatty = TRUE);
  dbms.monitor$crossref.code(code_files = .my_code_files, use.search = "c", chatty = TRUE);
  dbms.monitor$crossref.code(code_files = .my_code_files, use.search = "c", chatty = TRUE);
  dbms.monitor$code_xref.history %>% ls
}; toc(log = TRUE);

# PASSED 2021.08.26
tic("Unit test: $report <active binding>"); { dbms.monitor$report }; toc(log = TRUE);

tic.log() %>% reduce(c);
#
# ~FINALIZE ====
keyring::keyring_lock("TFS_GIT")

# pkgdown::build_site(pkg = "pkg", override = list(destination = "../docs"))
