#' Database Object Explorer
#'
#' @description
#' \code{DBOE} facilitates the navigation of SQL Server DBMS objects by way of retrieving the metatadata objects.
#' It also provides methods to retrieve information from the data catalog: specifically, the \href{http://datacatalog.alliance.local/en/datacatalog/edwchangelog}{EDW changelog}.  Once retrieved, changes can be compared against user-supplied source code
#'
#' @note
#' The workflow should look like the following:\cr \code{DBOE$new(dsn = , credentials = )\{$get.metadata()\{$get.xdep(db = )\{$get.changelog()\{$search.changelog(db = , ...) | crossref.code(code_files = , use.search = "c")\{; dboe$report\}\}\}\}\}}
#'
#' @importFrom rlang %||%
#' @importFrom stringi %s+%
#' @importFrom magrittr %>% %$% %T>%
#' @importFrom purrr reduce
#' @importFrom tictoc tic toc
#' @import data.table
#' @import DBI
#'
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
        	require(data.table, quietly = TRUE)
          purrr::iwalk(conns, ~{
          	# An ODBC connection for each value in .y must be created to use this mapper as-is.
            proxy_env <- new.env();
            this.db = .x@info$dbname;
            neo.conn = .x;

            tictoc::tic("\tMetadata -> " %s+% this.db);
            assign(.y, proxy_env, envir = self);

            schema.filter <- "^(db[_]|sys|infor|mdm|guest)"

            req.objs <- iterators::iter("sys." %s+% c("tables", "columns", "schemas", "procedures", "views", "types", "synonyms"));

            check.etl_obj <- function(obj){
          		if (!exists(obj, envir = proxy_env)){ message(sprintf("<%s> Failed to retrieve %s", this.db, obj)) }
          	}

            post.op <- function(i, j = NULL){
              		i <- as.data.table(i)
              		if (j != "schemas"){ setkeyv(i, cols = intersect(c("schema_id", "parent_object_id", "object_id"), names(i))) }
              		setnames(i, "name", paste0(ifelse(j == "tables", "tbl", ifelse(j == "columns", "col", "schema")), "_name"))
              	}

            message(sprintf("Processing metadata for <%s>", this.db));

            # Tables, Columns, Schemas ====
            purrr::walk(c("tables", "columns", "schemas"), ~{
            	assign(paste0("sys.", .x), DBI::dbReadTable(neo.conn, DBI::Id(schema = "sys", table = .x)) |> post.op(.x), envir = proxy_env)
	            check.etl_obj(iterators::nextElem(req.objs))
            })

            # Procs ====
            post.op <- function(i){
                  as.data.table(i)[
	                  proxy_env$sys.schemas[!(schema_name %ilike% "^(sys|infor|mdm|guest)"), c("schema_id", "schema_name")]
	                  , on = "schema_id", nomatch = 0
	                  ][, proc_def := purrr::map2(schema_name, name, ~{
	                      DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
	                        unlist() |> purrr::reduce(paste0)
	                    }) |> unlist()
	                  ] |>
	              		setnames("name", "proc_name") |>
	              		setkey(schema_id, proc_name)
                }
            proxy_env$sys.procedures <- { DBI::dbGetQuery(neo.conn, "SELECT [name], [schema_id], [object_id], proc_create_date = [create_date], proc_upd_date = [modify_date] FROM sys.procedures")} |> post.op();
            check.etl_obj(iterators::nextElem(req.objs))

            # Views ====
            post.op <- function(i){
                  as.data.table(i)[
                    proxy_env$sys.schemas[!(schema_name %ilike% schema.filter), c("schema_id", "schema_name")]
                    , on = "schema_id", nomatch = 0
                    ][, view_def := purrr::map2_chr(schema_name, name, ~{
                        DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
                        unlist() |>
                    		purrr::reduce(paste0)
                      }) |>
                    	purrr::modify_if(~rlang::is_empty(.x), .f = ~"##NO DEF##", .else = eval)
                    ] |>
              		setnames("name", "view_name") |>
              		setkey(schema_id, view_name)
                }
            proxy_env$sys.views <- { DBI::dbGetQuery(neo.conn, "SELECT [name], [schema_id], [object_id], view_create_date = [create_date], view_upd_date = [modify_date] FROM sys.views")} |> post.op()

            check.etl_obj(iterators::nextElem(req.objs))

            # Types ====
        		post.op <- function(i){
            		as.data.table(i)[, .(data_type = paste(name, collapse = ", ")), by = c("schema_id", "system_type_id")] |>
            		setkey(schema_id, system_type_id)
            	}
            proxy_env$sys.types <- { DBI::dbGetQuery(neo.conn, "SELECT [name], system_type_id, schema_id FROM sys.types") |> post.op()}
            check.etl_obj(iterators::nextElem(req.objs))

            # Combined Metadata ====
            assign(this.db, this.db, envir = proxy_env)

            proxy_env %$% {
            	metamap <- sys.tables[
	              sys.schemas[, !"principal_id"]
	              , on = "schema_id", .(tbl_name, schema_name, object_id)
	              ][
	              sys.columns[sys.types, on = "system_type_id", .(col_name, column_id, data_type, system_type_id, max_length
	                , precision, scale, is_nullable, is_identity, is_computed, is_xml_document, object_id)]
	              , on = c("object_id"), nomatch = 0
	              ][, c("object_id", "database", "col_meta") := list(
	              			NULL, this.db
	                    , apply(
	                    		X = data.table(system_type_id, max_length, precision, scale, is_nullable, is_identity, is_computed, is_xml_document)
	                    		, MARGIN = 1
	                    		, FUN = list
	                    		, simplify = FALSE
	                    		))
	              ][, !c("system_type_id", "max_length", "precision", "scale", "is_nullable", "is_identity", "is_computed", "is_xml_document")] |>
	              setcolorder(c("database", "tbl_name", "col_name")) |>
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
						    , fun = function(){ rlang::eval_tidy(rlang::quo(.data[[!!.x]]), data = !!(proxy_env$.tbl_dm)) }
						    , env = proxy_env
						    )) |> eval()
						});

						tictoc::toc(log = TRUE, quiet = !chatty);
          });

          invisible(self);
        },
        #' @description
        #' \code{get.xdep()} retrieves cross-database dependencies for endpoints provided in \code{self$conns}.  Results are saved as attributes to either of \code{sys.views} or code{sys.procedures}.
        #' @param db (string, symbol) The name of the database for which cross-database dependencies will be searched
        #' @family metadata
        #' @return The class object, invisibly
        get.xdep = function(db){
        	if (missing(db)){ stop("Missing argument `db`") } else { db <- as.character(rlang::enexpr(db)) }

        	db_list <- names(self$conns)

          cross_dbs <- mget(purrr::discard(db_list, ~.x == db), envir = self);

          this_db <- self[[db]];

          .queue <- { list(
	          	sys.views = this_db$sys.views %$% purrr::set_names(view_def, view_name)
	          	, sys.procedures = this_db$sys.procedures %$% purrr::set_names(proc_def, proc_name)
	          	)}

          # Cross-refs for views and stored procedures ...
      		purrr::imap(.queue, ~{
      			.needle = .x;
      			.nm = .y;

	          tryCatch({
              purrr::imap(cross_dbs, ~{
                .x %$% { c(sys.tables$tbl_name, sys.views$tbl_name)} |>
                  purrr::keep(~ .needle %ilike% .x) |>
              		purrr::compact()
                }) |> setattr(x = this_db[[.y]], name = "dependencies")
            	}, error = function(e){ message(db %s+% ": Error in checking for cross-references against " %s+% .nm) })
      		});

          invisible(self);
        },
				#' @description
				#' \code{$make.virtual_database} creates a set of \code{\link[dplyr]{tbl}} objects in an environment
				#' @param conn_name The \code{DBOE} connection name to use
				#' @param db_env The environment object where created objects should be stored
				#' @param ... The names of objects to retrieve: retrieves all tables and views if left blank.  If given as named arguments, the name becomes the local object name
				#' @return An assignable environment object with \code{DBI}-sourced \code{\link[dplyr]{tbl}}s
				make.virtual_database = function(conn_name = names(self$conns), db_env = rlang::caller_env(), ...){
					db <- match.arg(conn_name);
					all_objs <- self[[db]] %$% {
						.tables <- sys.tables[sys.schemas, on = "schema_id"][!is.na(tbl_name)] %$% {
							rlang::set_names(purrr::map2(dplyr::sql(schema_name), dplyr::sql(tbl_name), dbplyr::in_schema), tbl_name)
						}

						.views <- sys.views[sys.schemas, on = "schema_id"][!is.na(view_name)] %$% {
							rlang::set_names(purrr::map2(dplyr::sql(schema_name), dplyr::sql(view_name), dbplyr::in_schema), view_name)
						}

						c(.tables, .views) |> names() |> purrr::set_names()
					}
					obj_queue <- rlang::enexprs(..., .named = TRUE) |> purrr::map(rlang::as_label) |> unlist() %||% all_objs;
					obj_queue[obj_queue %in% all_objs] |>
						purrr::discard(duplicated) |>
						purrr::iwalk(purrr::possibly(~assign(.y, dplyr::tbl(src = self$conns[[db]], from = .x), envir = db_env), otherwise = "Could not connect"));

					invisible(self)
				}
      )}
    , active = { list(
        #' @field credentials The values passed during initialization for use in \code{\link[keyring]{key_get}}
        credentials = function(i){
	      		if (missing(i)){
	      			return(private$keyring_credentials)
	      		} else {
	      			private$keyring_credentials <- i;
	      			message(paste0("New Credentials:\n   "
	      										 , paste(names(private$keyring_credentials), private$keyring_credentials, sep = " -> ", collapse = "\n   ")));
	      		}}
        )}
    , private = list(keyring_credentials = NULL)
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
	auth_type = .args[intersect(names(.args), c("type"))] %>% { ifelse(rlang::is_empty(.), "basic", unlist(.)) }

	password	= NULL;
	auth.data = NULL;

	logi_vec = list(
		is_lan = any(stringi::stri_detect_regex(str = url, pattern = "(intranet[.])?alliance(healthplan|bhc).+(org|local)"))
		, is_keyless = rlang::is_empty(keyring_args)
		);

	# Check for intranet URLs and set authentication objects if found
	auth.data <- httr::authenticate(
		user = ifelse(logi_vec$is_lan, keyring_args$username %||% {"ALLIANCE\\" %s+% Sys.getenv("USERNAME")}, keyring_args$username)
		, password = if (logi_vec$is_keyless){ NULL } else { rlang::inject(keyring::key_get(!!!keyring_args)) }
		, type = ifelse(logi_vec$is_lan, "ntlm", auth_type)
		)

  httr::GET(url = url, eval(auth.data)) %$% content %>% rawToChar()
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

  .hits = i[, purrr::map(.SD, ~{
	    .needle = .x;
	    .haystack = x;
	    .test_1 = which(.needle %in% .haystack)
	    .test_2 = sapply(.haystack, function(.straw){ .needle %ilike% .straw }) |>
    						as.data.table() |> apply(1, any) |> which()
	    c(.test_1, .test_2) |> unique() |> sort() |> stats::na.omit()
	  }) |>
	  unlist(use.names = FALSE) |> stats::na.omit()
  ];

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

