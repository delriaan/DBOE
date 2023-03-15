#' Database Object Explorer
#'
#' @description
#' \code{DBOE} facilitates the navigation of SQL Server DBMS objects by way of retrieving the metatadata objects.
#' It also provides methods to retrieve information from the data catalog: specifically, the \href{http://datacatalog.alliance.local/en/datacatalog/edwchangelog}{EDW changelog}.  Once retrieved, changes can be compared against user-supplied source code
#'
#' @note
#' The workflow should look like the following:\cr \code{DBOE$new(dsn = , credentials = )\{$get.metadata()\{$get.xdep(db = )\{$get.changelog()\{$search.changelog(db = , ...) | crossref.code(code_files = , use.search = "c")\{; dboe$report\}\}\}\}\}}
#' @export
DBOE <- { R6::R6Class(
    classname = "DBOE"
    , lock_objects = FALSE
    , public = { list(
        #' @field conns A named list of DBI connections given as <database name> = <DBI connection>
        conns = NULL,
        #' @description
        #' Initialize the class object
        #' @param conns A named list with key-value elements (e.g., DSN_name = default_database_name): this can be set (\code{self$dsn <- list(...)}) before using DBMS-related methods
        #' @param keyring_credentials A list holding the \code{\link[keyring]{keyring}} name, service, and username for the secured password
        #' @return The class object, invisibly
        initialize = function(conns, keyring_credentials = NULL){
          self$conns <- conns;
          private$keyring_credentials <- keyring_credentials

          invisible(self);
        },
        #' @description
        #' \code{$get.metadata()} retrieves metadata information for the database pointed to by argument \code{conns}. Once metadata has been retrieved, metadata can be accessed for tables, views, and stored procedures using the following access method: \code{<DBOE obj>$<database name>$<table/view/proc name>}
        #' @param conns A named list of DBI connections given as <database name> = <DBI connection>
        #' @param chatty (logical) When \code{TRUE}, additional execution messages are sent to the console
        #' @family metadata
        #' @return The class object, invisibly
        get.metadata = function(conns = self$conns, chatty = FALSE){
          iwalk(conns, ~{
          	# An ODBC connection for each value in .y must be created to use this mapper as-is.
            this.db = .x@info$dbname;
            neo.conn = .x;

            tic("\tMetadata -> " %s+% this.db);
            proxy_env = new.env(); assign(.y, proxy_env, envir = self);

            schema.filter = "^(db[_]|sys|infor|mdm|guest)"

            req.objs = iterators::iter("sys." %s+% c("tables", "columns", "schemas", "procedures", "views", "types", "synonyms"));

            check.etl_obj = function(obj){
            		if (!exists(obj, envir = proxy_env)){ message(sprintf("<%s> Failed to retrieve %s", this.db, obj)) }
            	}

            message(sprintf("Processing metadata for <%s>", this.db));

            # Tables ====
            proxy_env$sys.tables <- { do.get_data(
            	this.conn = neo.conn
              , src.name = "sys.tables"
              , post.op = function(i){ setDT(i) %>% setkey(schema_id, parent_object_id, object_id) %>% setnames("name", "tbl_name") }
              , promise = !TRUE
            )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Columns ====
            proxy_env$sys.columns <- { do.get_data(
            	this.conn = neo.conn
              , src.name = "sys.columns"
              , post.op = function(i){ setDT(i) %>% setkey(object_id) %>% setnames("name", "col_name") }
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Schemas ====
            proxy_env$sys.schemas <- { do.get_data(
            	this.conn = neo.conn
            	, src.name = "sys.schemas"
              , post.op = function(i){ setDT(i) %>% setkey(schema_id) %>% setnames("name", "schema_name") }
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Procs ====
            proxy_env$sys.procedures <- { do.get_data(
            	this.conn = neo.conn
            	, src.name = "sys.procedures"
              , sel = "[name], [schema_id], [object_id], proc_create_date = [create_date], proc_upd_date = [modify_date]"
              , post.op = function(i){
                  setDT(i)[
                  proxy_env$sys.schemas[!(schema_name %ilike% "^(sys|infor|mdm|guest)"), .(schema_id, schema_name)]
                  , on = "schema_id", nomatch = 0
                  ][, proc_def := map2(schema_name, name, ~{
                      dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) %>%
                        unlist %>% reduce(paste0)
                    }) %>% unlist
                  ] %>% setnames("name", "proc_name") %>% setkey(schema_id, proc_name)
                }
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Views ====
            proxy_env$sys.views <- { do.get_data(
            	this.conn = neo.conn
            	, src.name = "sys.views"
              , sel = "[name], [schema_id], [object_id], view_create_date = [create_date], view_upd_date = [modify_date]"
              , post.op = function(i){
                  setDT(i)[
                    proxy_env$sys.schemas[!(schema_name %ilike% schema.filter), .(schema_id, schema_name)]
                    , on = "schema_id", nomatch = 0
                    ][, view_def := map2_chr(schema_name, name, ~{
                         dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) %>%
                         unlist %>% reduce(paste0)
                      }) %>% modify_if(~is_empty(.x), .f = ~"##NO DEF##", .else = eval)
                    ] %>% setnames("name", "view_name") %>% setkey(schema_id, view_name)
                }
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Types ====
            proxy_env$sys.types <- { do.get_data(
            	this.conn = neo.conn
            	, src.name = "sys.types"
              , sel = "name, system_type_id, schema_id"
              , post.op = function(i){ setDT(i)[, .(data_type = paste(name, collapse = ", ")), by = .(schema_id, system_type_id)] %>%
              		setkey(schema_id, system_type_id) }
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Synonyms ====
            proxy_env$sys.synonyms <- { do.get_data(
            	this.conn = neo.conn
            	, src.name = "sys.synonyms"
              , sel = "[name], tgt_name = base_object_name, [schema_id], [object_id], syn_create_date = [create_date], syn_upd_date = [modify_date]"
              , post.op = rlang::inject(function(i){
                  setDT(i)[
                    get(!!.y, envir = self)$sys.schemas[!(schema_name %ilike% schema.filter), .(schema_id, schema_name)]
                    , on = "schema_id", nomatch = 0
                    ] %>% setnames("name", "syn_name") %>% setkey(schema_id, syn_name)
                })
              , promise = !TRUE
              )}
            check.etl_obj(iterators::nextElem(req.objs))

            # Combined Metadata ====
            proxy_env %$% {
            	metamap <- sys.tables[
	              sys.schemas[, !"principal_id"]
	              , on = "schema_id", .(tbl_name, schema_name, object_id)
	              ][
	              sys.columns[sys.types, on = "system_type_id", .(col_name, column_id, data_type, system_type_id, max_length
	                , precision, scale, is_nullable, is_identity, is_computed, is_xml_document, object_id)]
	              , on = c("object_id"), nomatch = 0
	              ][, c("object_id", "database", "col_meta") := list(NULL, this.db
	                    , pmap(data.table(system_type_id, max_length, precision, scale, is_nullable, is_identity, is_computed, is_xml_document), list))
	              ][, !c("system_type_id", "max_length", "precision", "scale", "is_nullable", "is_identity", "is_computed", "is_xml_document")
	              ] %>%
	              setcolorder(c("database", "tbl_name", "col_name")) %>%
	              setkey(tbl_name, column_id);
            }
            # Active bindings ====
            message("Creating active bindings")
            # Stored procedures
						if (nrow(proxy_env$sys.procedures) > 0){
							.temp <- proxy_env$sys.procedures[
						          !is.na(proc_name)
						          , list(list(.SD[, schema_name:proc_def]))
						          , by = proc_name
						          ] %$% rlang::set_names(V1, proc_name);

							proxy_env$.proc_dm <- rlang::as_data_mask(.temp)

							ls(.temp, pattern = "^[a-z]") |> purrr::walk(~{
							    rlang::expr(makeActiveBinding(
							      sym = !!.x
							      , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.proc_dm)) }
							      , env = proxy_env
							      )) |> eval()
							  })
						}

            # Views
						if (nrow(proxy_env$sys.views) > 0){
	            .temp <- proxy_env$sys.views[
							          !is.na(view_name)
							          , list(list(.SD[, schema_name:view_def]))
							          , by = view_name
							          ] %$% rlang::set_names(V1, view_name);

							proxy_env$.view_dm <- rlang::as_data_mask(.temp);

							ls(.temp, pattern = "^[a-z]") |> purrr::walk(~{
							    rlang::expr(makeActiveBinding(
							      sym = !!.x
							      , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.view_dm)) }
							      , env = proxy_env
							      )) |> eval()
							  });
          	}

            # Tables and columns
						.temp <- proxy_env$metamap[!is.na(tbl_name)] |>
											split(by = "tbl_name") |>
											purrr::map(~{
												split(.x, by = "col_name") |>
												purrr::map(~.x[, schema_name:col_meta])
											})

						proxy_env$.tbl_dm <- rlang::as_data_mask(.temp)

						ls(.temp, pattern = "^[a-zA-Z]") |> purrr::walk(~{
						  rlang::expr(makeActiveBinding(
						    sym = !!.x
						    , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.tbl_dm)) }
						    , env = proxy_env
						    )) |> eval()
						})

            toc(log = TRUE, quiet = !chatty);
          });

          invisible(self);
        },
        #' @description
        #' \code{get.xdep()} retrieves cross-database dependencies for endpoints provided in \code{self$conns}
        #' @param db (string, symbol) The name of the database for which cross-database dependencies will be searched
        #' @family metadata
        #' @return The class object, invisibly
        get.xdep = function(db){
        	if (missing(db)){ stop("Missing argument `db`") } else { db <- as.character(rlang::enexpr(db))}
        	db_list = names(self$conns)
          cross_dbs = mget(discard(db_list, ~.x == db), envir = self);
          this_db <- self[[db]];

          # Cross-refs for views ...
          tryCatch({
            invisible(this_db$sys.views[, .(view_def, view_name)] %>% as.list() %$% {
              map(purrr::set_names(view_def, view_name), ~{
                  .needle = .x;

                  map(cross_dbs, ~{
                    .x %$% { c(sys.tables$tbl_name, sys.views$tbl_name)} %>%
                      keep(~.needle %ilike% .x ) %>%
                  		purrr::compact()
                    })
                })
            	} %>% c() %>% setattr(x = this_db$sys.views, name = "dependencies"))
            }, error = function(e){ message(db %s+% ": Error in checking for cross-refs of views") });

          # Cross-refs for procs ...
          tryCatch({
            invisible(this_db$sys.procedures[, .(proc_name, proc_def)] %>% as.list() %$% {
              map(purrr::set_names(proc_def, proc_name), ~{
                  .needle = .x;

                  map(cross_dbs, ~{
                  	.x %$% { c(sys.tables$tbl_name, sys.views$tbl_name)} %>%
                  		keep(~.needle %ilike% .x ) %>%
                  		purrr::compact()
                    })
                })
              } %>% c() %>% setattr(x = this_db$sys.procedures, name = "dependencies"))
            }, error = function(e){ message(db %s+% ": Error in checking for cross-refs of stored procedures") });

          invisible(self);
        },
        #' @description
        #' \code{$get.changelog()} retrieves the content of the \href{http://datacatalog.alliance.local/en/datacatalog/edwchangelog}{EDW changelog}, parses it, and transforms it into a tabular format making it fit to be compared against database metadata and source code.  This, along with the HTTP response content itself are stored in a class-member list named \code{$changelog} with the following members: \cr
        #' \describe{
        #'    \item{content}{The HTML markup of the <body> of the retrieved content as an XML document pointer}
        #'    \item{explore}{A \code{\link[data.table]{data.table}} object containing the parsed and transformed elements of the changelog markup.  Each column and the entire object itself is provided with descriptive attributes }
        #' }
        #' @param chatty (logical) When \code{TRUE}, additional execution messages are sent to the console
        #' @family changelog
        #' @return The class object, invisibly
        get.changelog = function(chatty = FALSE){
          .dictionary = { c(
	            "Grouping ID at the level of the date of change"
	            , "Grouping ID at the level of the type of change"
	            , "The date of the entry"
	            , "The entry notes containing what was changed along with additional details"
	            , "A type of change" %>% rep.int(5)
	            , "Either the date of entry or a future date (matches against 'schedule' and looks for a date in format \"([0-9]+[.]?)+\" "
	            , "One of \"PAST\" or \"FUTURE\" based on the value of column 'eff_date' relative to the current date "
	            )} %>%
          	enlist("change_set_id", "change_det_id", "notification_date", "change_note", "ADDED", "CHANGED", "DEPRECATED", "REMOVED", "FIXED", "eff_date", "schedule");

          if (is.null(private$keyring_credentials)){
          	message("No keyring credentials detected ...")
          	private$keyring_credentials <- list(
          		keyring = readline(prompt = "... enter keyring name or leave blank: ") %>% { ifelse(stringi::stri_length(.) == 0, NULL, .) }
          		, service = readline(prompt = "... enter keyring service or leave blank: ") %>% { ifelse(stringi::stri_length(.) == 0, NULL, .) }
          		, username = readline(prompt = "... enter keyring username or leave blank: ") %>% { ifelse(stringi::stri_length(.) == 0, NULL, .) }
          		)
          }

          .changelog = list();
          .changelog$content = query_adhoc(url = self$changelog_url, !!!private$keyring_credentials) %>%
          											read_html() %>% xml_children() %>% last();
          .changelog$explore = { # This entire code block navigates the structure of the retrieved page from the website
            rbindlist({
              .changelog$content %>% xml_find_all("//template[@slot='contents']") %>%
                xml_children() %>% xml_find_all("//blockquote|//h1/*") %>%
                map2(xml_name(.), ~tryCatch({
                  if (chatty){ message(.y) }
                  list(
                    tag = xml_name(.x)
                    , # {.y == "a"} | "XPATH search above (xml_find_all(...)) returns the '<a>' as the first child of the search path "//blockquote.
                    vals = if(.y == "a"){ xml_attr(.x, "href") %>% stri_extract_first_regex("[0-9]+")
                    	} else {
                          sapply(xml_children(.x), xml_text) %>%
                          stri_enc_toascii() %>%
                          stri_split_fixed("\n", simplify = FALSE) %>% reduce(rbind) %>%
                          stri_trim_both() %>% stri_replace_first_regex("[-]", "")
                        }
                    )}
                  , error = function(e){ }
                  , finally = ""
                	))
            })[
            , # "Unwrap" the HTML markup structure, matching tags with the text
            { c(.SD[, !c("tag")]
              , xform.basis_vector(.SD$tag)[, map(.SD, cumsum)]
              , xform.basis_vector(vals, bvec = keep(vals, ~.x %like% "^[A-Z]{5,15}$") %>% unique)
              )}
            ][
            , # +{notification_date, sub_group} | Date of log entry on page, indicator of the changeset across each date of entry
            `:=`(notification_date = first(vals), sub_group = cumsum(vals %like% "^[A-Z]{5,9}$"))
            , by = c("a")
            ][][
            , c(
            	list(vals = vals)
              , # @TRANSFORMATION{ ADDED, CHANGED, DEPRECATED, REMOVED, FIXED } | For each group defined in the 'by' expression, return the cumulative maximum: this maps each of the changes under "vals" to the correct type of change.  This allows unnecessary rows to be removed in subsequent steps.
              map(.SD[, !c("vals", "blockquote")], ~cummax(.x))
              )
            , by = .(a, sub_group, notification_date)
            ][
            # @FILTER | Remove rows that do not indicate a change type or where elements of 'vals' only have the change type (these are simply not needed at this point)
            which(and(!vals %like% "[A-Z]{5,9}", data.table(ADDED, CHANGED, DEPRECATED, REMOVED, FIXED) %>% pmap_lgl(function(...){ sum(c(...)) > 0 })))
            ][, modify_at(.SD, ls(.SD, pattern = "[A-Z]{5,9}"), as.logical)
            ][, # +{eff_date} | When "schedule" is matched in 'vals', it is assumed a scheduled implementation date is given.  This code extracts that date if it exists or returns the log entry date if there is no match.
            eff_date := { ifelse(
              vals %ilike% "schedule"
              , stri_extract_all_regex(vals, "([0-9]+[.]?)+", simplify = TRUE) %>% stri_replace_all_fixed(".", "", vectorize_all = FALSE)
              , notification_date
              )} %>% as.Date(format = "%Y%m%d")
            ][, # +{schedule}  | Chronology indicator
              schedule := (sign(today() - eff_date)) %>% as.character %>% { set_names(c("FUTURE", "TODAY", "PAST"), as.character(-1:1))[.] }
            ][] %>% setnames(c(1:2, 4), c("change_set_id", "change_det_id", "change_note"))
          }

          # Set definition attributes for each column
          iwalk(.dictionary, ~setattr(.changelog$explore[[.y]], "def", .x))

          self$changelog <- .changelog;

          invisible(self)
        },
        #' @description
        #' \code{$search.changelog()} executes a search on the object returned from the call to class method \code{$get.changelog()}
        #' @param db (string) The name of the database for which changes are sought
        #' @param ... Expressions to filter the changelog object via \code{\link[dplyr]{filter}}
        #' @param retain (logical) When \code{TRUE}, the results are saved in \code{self$search.history}; if a string, the results are assigned to an object of the same name in \code{self$search.history}.
        #' @family changelog
        #' @return The class object, invisibly
        search.changelog = function(db, ..., retain = TRUE){
            # Create the output ====
            output = dplyr::filter(.data = copy(self$changelog$explore), ...)[
              , self[[db]]$metamap[!is.na(tbl_name)
                , list(
                    change_obj = keep(tbl_name %>% unique, ~change_note %ilike% .x) %>% list %>%
                    	modify_if(~identical(character(0), .x), ~"<Removed/Missing>")
                    , ADDED, CHANGED, DEPRECATED, REMOVED, FIXED
                    )
                ]
              , by = .(notification_date, change_note, schedule, eff_date)
              ];

            # Return the output depending on the class of argument 'retain' ====
            switch(
              class(retain)
              , "logical" = if (retain){
                      if (!"search.history" %in% ls(envir = self)){ self$search.history <- new.env() }
                      assign("search_" %s+% format(Sys.time(), "%Y.%m.%d_%H.%M.%S"), output, envir = self$search.history);
                      invisible(self);
                    } else { print(output); invisible(self) }
              , "character" = {
                    if (!"search.history" %in% ls(envir = self)){ self$search.history <- new.env() }
                    assign(paste0(retain, "_", format(Sys.time(), "%Y.%m.%d_%H.%M.%S")), output, envir = self$search.history);
                    invisible(self);
                  }
              , invisible(self)
              );
          },
        #' @description
        #' \code{$crossref.code()} attempts to match the changelog against both the database metadata and source files (provided by path and name in \code{code_files})
        #' @param code_files (string[]) A vector of fully-qualified file paths
        #' @param use.search (object) When not \code{NULL} the provided object provides changelog search results
        #' @param retain (logical) When \code{TRUE}, the results are saved in \code{self$xref.history}; if a string, the results are assigned to an object of the same name in \code{self$xref.history}.
        #' @param chatty (logical) When \code{TRUE}, additional execution messages are sent to the console
        #' @param ... Additional names of objects to search: useful when there's a suspicion of deleted objects missing from metadata
        #' @family changelog
        #' @return The class object, invisibly
        crossref.code = function(code_files, use.search, retain = TRUE, chatty = FALSE, ...){
          .us = NULL;
          if (missing(use.search)){ use.search <- self$changelog } else
          if ("choose" %ilike% use.search){
            use.search <- tcltk::tk_select.list(
              choices = ls(self$search.history)
              , preselect = ls(self$search.history)[1]
              , title = "Choose a previously made search"
              , multiple = FALSE
              ) %T>% { .us <<- . } %>% get(envir = self$search.history)
          } else { use.search <- use.search %T>% { .us <<- . } %>% get(envir = self$search.history) }

          code_files %<>% purrr::set_names(
            stri_replace_all_fixed(., "\\", "/", vectorize_all = FALSE) %>%
              stri_replace_all_regex("[:]+", "", vectorize_all = FALSE)
            );

          if (chatty){ message(length(code_files) %s+% " objects provided ...") }
          code_files_content = imap_dfr(code_files, ~{
          	list(file = .y, text = stri_read_lines(.x) %>% paste(collapse = " "))
          	}) %>% setDT();

          .needle = use.search$change_obj %>% reduce(c) %>%
          	discard(~.x %like% "Missing") %>%
            rlang::list2(...) %>%
          	unlist() %>% unique() %T>% {
            	if (or(length(.) == 0, identical(character(0), .))){ message("No matches found: exiting ..."); return(0) }
            }

          if (chatty){ print(.us); print(code_files_content) }

          output = (code_files_content %look.for% paste(unlist(.needle), collapse = "|"));

          if (!all(map_lgl(output, ~identical(character(), .x)))){
	          # Return the output depending on the class of argument 'retain' ====
	          switch(
	            class(retain)
	            , "logical" = if (retain){
	                  if (!"code_xref.history" %in% ls(envir = self)){ self$code_xref.history <- new.env() }
	                  assign(
	                    paste0(c("xref_", .us %s+% "_")[1 + !is.null(.us)], format(Sys.time(), "%Y.%m.%d_%H.%M.%S"))
	                    , output
	                    , envir = self$code_xref.history
	                    );

	                  invisible(self);
	                } else { print(output); invisible(self) }
	            , "character" = {
	                  if (!"code_xref.history" %in% ls(envir = self)){ self$code_xref.history <- new.env() }
	                  assign(
	                  	paste0(retain, "_", format(Sys.time(), "%Y.%m.%d_%H.%M.%S"))
	                  	, output[, related_search := .us]
	                  	, envir = self$code_xref.history
	                  	);

	                  invisible(self);
	                }
	            , invisible(self)
	          	);
          } else { invisible(self)}
        },
				#' Create a Virtual DBOE Database
				#' @param dboe A \code{DBOE} object
				#' @param db The \code{DBOE} connection name to use
				#' @return An assignable environment object with \code{\link[DBI]}-sourced \code{\link[dplyr]{tbl}s} (this cannot be R6-piped)
				make.virtual_database = function(db = names(self$conns)){
					db <- match.arg(db);

					c(self[[db]] %$% {
						sys.tables[sys.schemas, on = "schema_id"][!is.na(tbl_name)] %$% rlang::set_names(purrr::map2(dplyr::sql(schema_name), dplyr::sql(tbl_name), dbplyr::in_schema), tbl_name)}
						, self[[db]] %$% {
							sys.views[sys.schemas, on = "schema_id"][!is.na(view_name)] %$% rlang::set_names(purrr::map2(dplyr::sql(schema_name), dplyr::sql(view_name), dbplyr::in_schema), view_name)}
						) %>%
						.[!duplicated(names(.))] |>
						purrr::map(purrr::possibly(~dplyr::tbl(src = self$conns[[db]], from = .x), otherwise = "Could not connect")) |>
						list2env(new.env())
				}
      )}
    , private = list(keyring_credentials = NULL, changelog.url = "http://datacatalog.alliance.local/en/datacatalog/edwchangelog")
    , active = { list(
    		#' @field report \code{$report} returns a dataset showing what files should be checked for detailed changes in which database objects. The code cross-reference search selected is stored as an attribute of the output labeled "search_name".
        report = function(){
          .search_name = NULL;

          xref_search = if (!"code_xref.history" %in% ls(self)){
          # xref_search = dbms.monitor$code_xref.history$xref_2021.08.19_17.03.46
            message("No code cross-reference searches saved.  Please run and 'retain' a cross-reference search first: exiting ...");
            return(0);
          } else {
              .search_name <- { tcltk::tk_select.list(
                choices = ls(self$code_xref.history)
                , title = "Choose a cross-reference search to use:"
                , multiple = FALSE
                )}
              get(.search_name, envir = self$code_xref.history);
            }

          xref_search[, { c(
            # Element I: Path to source file with GIT as root
            { path %>%
                stri_extract_all_regex("([A-Za-z#&\\-_/ ]+([A-Za-z#&_ ]+)?).*.R$", simplify = FALSE) %>%
                { paste(stri_sub(., 1, 20), stri_sub(., stri_length(.) - 40, stri_length(.)), sep = "~") } %>%
                reduce(rbind) %>% list
            }
            , # Element II: Occurrence map of which database objects are referenced in each source file
            { xform.basis_vector(code, bvec = attr(xref_search, "needle"), regex = TRUE)[
              # Post-test operation: Remove zero-valued columns, and create a row filter for zero-valued rows
              , c(discard(.SD, ~sum(.x) == 0), list(row.fltr = apply(.SD, 1, sum) > 0))
            ]}
            )}][(row.fltr), !"row.fltr"] %>%
          setnames(1L, "file_path") %>%
          setattr("search_name", .search_name)
        },
        #' @field log.searches \code{$log.searches} shows what changelog searches have been made
        log.searches = function(){ if ("search.history" %in% ls(self)){ ls(self$search.history) } else { message("No history to search") }},
        #' @field xref.searches \code{$xref.searches} lists what code cross-reference searches have been made
        xref.searches = function(){ if ("code_xref.history" %in% ls(self)){ ls(self$code_xref.history) } else { message("No history to search") }},
        #' @field credentials The values passed during initialization for use in \code{\link[keyring]{key_get}}
        credentials = function(i){
	      		if (missing(i)){
	      			return(private$keyring_credentials) } else {
	      			private$keyring_credentials <- i;
	      			message(paste0("New Credentials:\n   ", paste(names(private$keyring_credentials), private$keyring_credentials, sep = " -> ", collapse = "\n   ")));
	      		}},
        #' @field changelog_url The URL to use for the Enterprise Data Warehouse changelog
        changelog_url = function(i){
        		if (missing(i)){
        			return(private$changelog.url) } else {
      				private$changelog.url <- i;
      				message("New Changelog URL:  " %s+% private$changelog.url);
      			}}
        )}
    )}

query_adhoc <- function(url, ...){
#' Retrieve the contents of a query
#'
#' \code{query_adhoc} calls \code{\link[httr]{GET}}, and passes the credentials referenced by argument \code{cred}.  The response is converted to class 'character' before returning the output
#' @param url (string) The URL to call
#' @param ... Arguments to be passed to \code{\link[keyring]{key_get}}
#'
#' @return The content of the URL
#' @export

	.args = rlang::list2(...)
	keyring_args = .args[intersect(names(.args), c("username", "password", "keyring", "service"))]
	auth_type = .args[intersect(names(.args), c("type"))] %>% { ifelse(is_empty(.), "basic", unlist(.)) }
	password	= NULL;
	auth.data = NULL;

	logi_vec = list(
		is_lan = any(stri_detect_regex(str = url, pattern = "(intranet[.])?alliance(healthplan|bhc).+(org|local)"))
		, is_keyless = is_empty(keyring_args)
		);

	# Check for intranet URLs and set authentication objects if found
	auth.data <- authenticate(
		user = ifelse(logi_vec$is_lan, keyring_args$username %||% {"ALLIANCE\\" %s+% Sys.getenv("USERNAME")}, keyring_args$username)
		, password = if (logi_vec$is_keyless){ NULL } else { rlang::inject(keyring::key_get(!!!keyring_args)) }
		, type = ifelse(logi_vec$is_lan, "ntlm", auth_type)
		)

  GET(url = url, eval(auth.data)) %$% content %>% rawToChar()
}

`%look.for%` <- function(i, x){
#' Look for a Database Object Names
#'
#' @param i (object) A data.frame, data.table, or coercible containing DBMS object names
#' @param x (string[]) A vector of REGEX patterns or exact names to use for matching against database object names
#'
#' @return A \code{\link[data.table]{data.table}} object with the items that were found, if any
#' @export

  if (is.environment(i)){ if (rlang::env_has(i, "metamap")){ i <- i$metamap }}
  if (!is.data.table(i)){ i <- as.data.table(i) }

  .hits = i[, map(.SD, ~{
    .needle = .x;
    .haystack = x;
    .test_1 = which(.needle %in% .haystack)
    .test_2 = sapply(.haystack, function(.straw){ .needle %ilike% .straw }) %>% as.data.table %>% apply(1, any) %>% which
    c(.test_1, .test_2) %>% unique %>% sort %>% na.omit
  }) %>% unlist(use.names = FALSE) %>% na.omit];

  # Verify that results exists, or exit if not
  if (identical(integer(0), .hits)){
    message("[FAIL]: <" %s+% paste(x, collapse = ", ") %s+% "> not found or failed to find matches.");
    return(0);
  }
  i[(.hits)] %>% setkeyv(intersect(c("database", "schema_name", "tbl_name", "proc_name"), names(.))) %>% {
    .out = .;

    if ("col_names" %in% names(.)){ .out[!is.na(col_name), .(col_names = list(c(col_name))), by = c(key(.out))] } else { .out }
  }
}

# pkgdown::build_site()
# usethis::use_proprietary_license("Chionesu George")
