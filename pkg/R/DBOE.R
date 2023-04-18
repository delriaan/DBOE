#' Database Object Explorer
#'
#' @description
#' \code{DBOE} facilitates the navigation of SQL-based database engines by means of metadata. Engines tested to date include Microsoft SQL Server and MySQL.
#'
#' @importFrom rlang %||%
#' @importFrom magrittr %>% %$% %T>% %<>%
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
        #' @description
        #' Initialize the class object
        #' @return The class object, invisibly
        initialize = function(){
          invisible(self);
        },
        #' @description
        #' \code{$get.metadata()} retrieves metadata information for the database pointed to by argument \code{conns}. Once metadata has been retrieved, metadata can be accessed for tables, views, and stored procedures using the following access method: \code{<DBOE obj>$<database name>$<table/view/proc name>}
        #' @param ... \code{\link[rlang]{dots_list}}: one or more DBI/ODBC connection objects.
        #' @param chatty (logical) When \code{TRUE}, additional execution messages are sent to the console
        #'
        #' @return The class object, invisibly
        get.metadata = function(..., chatty = FALSE){
          # Helper function
          check.etl_obj <- function(obj){
	          proxy_env <- rlang::caller_env() %$% proxy_env;
	          this.db <- rlang::caller_env() %$% this.db;

        		if (!hasName(proxy_env, obj)){ message(sprintf("<%s> Failed to retrieve %s", this.db, obj)) }
          }

					# Helper function
          post.op <- function(i, j, dbms){
          	force(dbms);
          	force(i);
          	proxy_env <- rlang::caller_env() %$% proxy_env;
          	neo.conn <- rlang::caller_env() %$% neo.conn;
          	this.db <- rlang::caller_env() %$% this.db;

						rlang::exprs(
							default = {
								data.table::setDT(i) |>
			        		data.table::setnames(
			        			ifelse(dbms != "MySQL", "name", "TABLE_NAME")
			        			, paste0(c(columns = "col", tables = "tbl", schemas = "schema")[j], "_name")
			        			) %>%
									data.table::setnames(names(.) |> tolower());

		        		if (j != "schemas"){
		          			data.table::setkeyv(
		          				i, cols = intersect(
		          							c("schema_id", "parent_object_id", "object_id", "table_schema", "table_name", "column_name")
		          							, tolower(names(i)))
		          				)
		          		} else { i }
	        		}
							, views = {
									(if (dbms != "MySQL"){
										data.table::as.data.table(i)[
			                proxy_env$sys.schemas[, .(schema_id, schema_name)]
			                , on = "schema_id", nomatch = 0
			                ][
			                , view_def := purrr::map2_chr(schema_name, name, ~{
			                    DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
			                    unlist() |>
			                		purrr::reduce(paste0)
			                  }) |>
			                	purrr::modify_if(rlang::is_empty, .f = ~"##NO DEF##")
		                	]
									} else {
										data.table::as.data.table(i) %>%
											data.table::setnames(names(.) |> tolower()) |>
											data.table::setnames(c("view_definition", "table_name"), c("view_def", "view_name")) |>
		                	purrr::modify_at("view_def", .f = purrr::modify_if, rlang::is_empty, ~"##NO DEF##")
									}) -> i;

			        		if (j != "schemas"){
			          			data.table::setkeyv(
			          				i, cols = intersect(
			          							c("view_name", "schema_name", "column_name", "schema_id", "parent_object_id", "object_id")
			          							, names(i))
			          				)
			          		} else { i }
		        		}
							, types = {
									data.table::as.data.table(i)[
										, .(data_type = paste(name, collapse = ", "))
										, by = c("schema_id", "system_type_id")
										] |>
            			data.table::setkey(schema_id, system_type_id)
								}
              , procedures = {
              		(if (dbms != "MySQL"){
            				data.table::as.data.table(i)[
			                proxy_env$sys.schemas[!(schema_name %ilike% "^(sys|infor|mdm|guest)"), c("schema_id", "schema_name")]
			                , on = "schema_id"
			                , nomatch = 0
			                ][, proc_def := purrr::map2(schema_name, name, ~{
			                    DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
			                      unlist() |> purrr::reduce(paste0)
			                  }) |> unlist()
			                ] |>
              			data.table::setnames("name","proc_name") |>
	            			data.table::setkey(schema_id, proc_name)
              		} else {
										data.table::as.data.table(i) %>%
											data.table::setnames(names(.) |> tolower()) |>
											data.table::setnames(c("routine_definition", "routine_name")
																					 , c("proc_def", "proc_name")) |>
		                	purrr::modify_at("proc_def", .f = purrr::modify_if, rlang::is_empty, ~"##NO DEF##")
              		}) -> i

			        		if (j != "schemas"){
			          			data.table::setkeyv(
			          				i, cols = intersect(
			          							c("schema_id", "parent_object_id", "object_id", "table_schema", "proc_name", "column_name")
			          							, tolower(names(i)))
			          				)
			          		} else { i }
            		}
							)[[ ifelse(j %in% c("tables", "columns", "schemas"), "default", j) ]] |>
          		eval()
           }

          # Define the queue
          .queue <- rlang::dots_list(
          		...
          		, .ignore_empty = "none"
          		, .homonyms = "last"
          		, .check_assign = TRUE
          		) |>
          	purrr::map(~{
							.out <- rlang::list2(conn = .x, !!!{ DBI::dbGetInfo(.x) %$% mget(ls(pattern = "dbname|dbms[.]name|sourcename"))});

							if (!hasName(.out, "dbms.name")){
									.out$dbms.name <- .out$dbname;
									.out$dbname <- .out$sourcename;
								}

							.out[c("conn", "dbname", "sourcename", "dbms.name")] |>
								purrr::set_names(c("neo.conn", "this.db", "sourcename", "this.dbms"))
						}) %>%
          	purrr::set_names(purrr::map_chr(., ~.x$this.db));

					# Execute
          purrr::iwalk(.queue, ~{
						# An ODBC connection for each value in .y must be created to use this mapper as-is.
						proxy_env <- new.env()

						list2env(.x, envir = environment())

						private$connections[[.y]] <- neo.conn;
						assign(.y, proxy_env, envir = self);

	  				dbms_types <- { list(
	          	`Microsoft SQL Server` = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "procedures", "schemas", "types", "synonyms")
	          			, data = DBI::dbReadTable(conn = neo.conn, name = DBI::Id(catalog = this.db, schema = "sys", table = x))
	          			, procedures = DBI::dbReadTable(conn = neo.conn
      								, name = DBI::Id(catalog = this.db, schema = "sys", table = "procedures")
	          					)[, c("name", "schema_id", "object_id", "create_date", "modify_date")] |>
	          				data.table::setnames(c("create_date", "modify_date"), c("proc_create_date", "proc_upd_date"))
	          			)
	          	, MySQL = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "routines")
	          			, data = DBI::dbGetQuery(conn = neo.conn, statement = paste0("SELECT * FROM information_schema.", x))
	          			, procedures = DBI::dbGetQuery(conn = neo.conn, statement = "SELECT * FROM information_schema.routines")
	          			)
	          	)[[this.dbms]]
	  				}

						req.objs <- dbms_types$meta |> sort() |> purrr::set_names();

            if (chatty) message(sprintf("Processing metadata for <%s>", this.db));
            tictoc::tic(paste("\tMetadata -> ", this.db));

            # Tables, Columns, Schemas, Views (VALIDATE: MySQL[1] MSSQL[?]) ====
            purrr::keep(req.objs, ~.x %in% c("schemas", "tables", "columns", "views"))  |>
            	purrr::iwalk(~{
	            	x <- .x;
	            	y <- paste0(ifelse(this.dbms == "MySQL", "", "sys."), req.objs[.x]);

			          .out <- dbms_types$data |> eval();

			          if ((nrow(.out) %||% 0) > 0){
			          	assign(y, post.op(i = .out, j = x, dbms = this.dbms), envir = proxy_env)
			          	check.etl_obj(y);
			          }
	            });

            # Procs (VALIDATE: MySQL[1] MSSQL[1]) ====
						.this <- dbms_types$procedures |> eval();

						if ((nrow(.this) %||% 0) > 0){
							obj <- paste0(
											ifelse(this.dbms == "MySQL", "", "sys.")
											, req.objs[which(grepl("procedure|routine", req.objs))]
											)

							assign(obj, value = post.op(i = .this, j = "procedures", dbms = this.dbms), envir = proxy_env)

							check.etl_obj(obj);
						}

            # Types (VALIDATE: MySQL[1] MSSQL[1]) ====
            if (this.dbms != "MySQL"){
            	.this <- DBI::dbGetQuery(neo.conn, "SELECT [name], system_type_id, schema_id FROM sys.types");

            	if ((nrow(.this) %||% 0) > 0){
            		assign("sys.types", post.op(i = .this, j = "types", dbms = names(dbms_types)), envir = proxy_env);

	            	check.etl_obj(paste0("sys.", req.objs[which(grepl("types", req.objs))]));
            	}
            }

						# Combined Metadata (VALIDATE: MySQL[1] MSSQL[1]) ====
            assign(this.db, proxy_env, envir = self);

            metamap_action <- rlang::quos(
            	`Microsoft SQL Server` = proxy_env %$% {
            		metamap <- { sys.tables[
		              sys.schemas[, !"principal_id"]
		              , on = "schema_id"
		              , .(tbl_name, schema_name, object_id)
		              ][
		              sys.columns[
		              	sys.types
		              	, on = "system_type_id"
		              	, .(col_name, column_id, data_type, system_type_id, max_length
		              			, precision, scale, is_nullable, is_identity, is_computed, is_xml_document, object_id
		              			)]
		              , on = c("object_id"), nomatch = 0
		              ][
		              , c("object_id", "database", "col_meta") := {
		              		list(NULL, this.db, apply(
		                  		X = data.table(system_type_id, max_length, precision, scale, is_nullable
		                  									 , is_identity, is_computed, is_xml_document)
		                  		, MARGIN = 1
		                  		, FUN = list
		                  		, simplify = FALSE
		                  		))
		              		}
		              ][
		              !is.na(tbl_name)
		              , !c("system_type_id", "max_length", "precision", "scale"
		              			 , "is_nullable", "is_identity", "is_computed", "is_xml_document")
	              	]
            		} %>%
	            		.[!duplicated(.[, !"col_meta"])] |>
		              data.table::setcolorder(c("database", "tbl_name", "col_name")) |>
		              data.table::setkey(tbl_name, column_id)
		            }
            	, MySQL = proxy_env %$% {
            			metamap <- { columns[
	            			, .(database = this.db
	            					, column_type
		            				, ordinal_position
				            		, col_meta = .SD[, .(
					            			character_maximum_length
					            			, character_octet_length
					            			, precision = numeric_precision
					            			, scale = numeric_scale
					            			, column_default
					            			, is_nullable
					            			, extra
					            			, privileges
					            			, column_comment
					            			, generation_expression
					            			)] |>
		            					purrr::array_branch(1)
			            		), by = .(table_schema, tbl_name = col_name, col_name = column_name)
			            	][
			            	tables[, .(database = this.db, table_schema, tbl_name, table_type, max_data_length, table_comment)] |> unique()
			            	, on = c("database", "table_schema", "tbl_name")
			            	, nomatch = 0
			            	, allow.cartesian = TRUE
			            	]
            			}
            			metamap %<>% { .[!duplicated(.[, !"col_meta"])] |>
	            			data.table::setnames("table_schema", "schema_name") |>
		            		data.table::setkey(database, tbl_name, col_name) |>
		            		data.table::setcolorder()
            			}
	            	}
            	);

            .meta_idx <- c(grepl("microsoft|odbcconnection", this.dbms, ignore.case = TRUE)
            							 , grepl("mysql", this.db, ignore.case = TRUE)) |> which();

            rlang::eval_tidy(metamap_action[[.meta_idx]]);

            # Active bindings (VALIDATE: MySQL[1] MSSQL[1]) ====
            if (chatty) message("Creating active bindings");

            # Stored procedures (VALIDATE: MySQL[1] MSSQL[1])
						if ((nrow(proxy_env$sys.procedures) %||% 0) > 0){
							.temp <- proxy_env$sys.procedures[
						          !is.na(proc_name)
						          , list(list(.SD[, schema_name:proc_def]))
						          , by = proc_name
						          ] %$% rlang::set_names(V1, proc_name);

							proxy_env$.proc_dm <- rlang::as_data_mask(.temp);

							ls(.temp, pattern = "^[a-z]") |> purrr::walk(~{
							    rlang::expr(makeActiveBinding(
							      sym = !!.x
							      , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.proc_dm)) }
							      , env = proxy_env
							      )) |> eval()
							  });
						}

						if ((nrow(proxy_env$routines) %||% 0) > 0){
							.temp <- proxy_env$routines[
						          !is.na(proc_name)
						          , list(proc_created = created
						          			 , list(.SD[, .(proc_def, routine_comment)])
						          			 )
						          , by = .(proc_name, schema_name = routine_schema)
						          ] %$% rlang::set_names(V2, proc_name);

							proxy_env$.proc_dm <- rlang::as_data_mask(.temp)

							ls(.temp, pattern = "^[a-z]") |> purrr::walk(~{
							    rlang::expr(makeActiveBinding(
							      sym = !!.x
							      , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.proc_dm)) }
							      , env = proxy_env
							      )) |> eval()
							  })
						}

            # Views (VALIDATE: MySQL[1] MSSQL[1])
						if ((nrow(proxy_env$sys.views) %||% 0) > 0){
	            .temp <- data.table::setnames(proxy_env$sys.views, "name", "view_name", skip_absent = TRUE)[
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

						if ((nrow(proxy_env$views) %||% 0) > 0){
	            .temp <- proxy_env$views[!is.na(table_schema), .(view_name, schema_name = table_schema, view_def)];

							proxy_env$.view_dm <- rlang::as_data_mask(.temp);

							ls(.temp, pattern = "^[a-z]") |> purrr::walk(~{
							    rlang::expr(makeActiveBinding(
							      sym = !!.x
							      , fun = function(){ rlang::eval_tidy(quo(.data[[!!.x]]), data = !!(proxy_env$.view_dm)) }
							      , env = proxy_env
							      )) |> eval()
							  });
          	}

            # Tables and Columns (VALIDATE: MySQL[1] MSSQL[1])
						.temp <- proxy_env$metamap[!is.na(tbl_name)] |>
								split(by = "tbl_name") |>
								purrr::map(~{
									split(.x, by = "col_name") |>
									purrr::map(~.x[, schema_name:col_meta])
								});

						proxy_env$.tbl_dm <- rlang::as_data_mask(.temp);

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
				#' \code{$make.virtual_database} creates a set of \code{\link[dplyr]{tbl}} objects in an environment.
				#'
				#' @param conn The name of a metadata environment (created after calling \code{$get.metadata() })
				#' @param target_env The environment object where created objects should be stored
				#' @param ... Names or patterns of objects to retrieve to link from source.
				#'
				#' @return An assignable environment object with \code{DBI}-sourced \code{\link[dplyr]{tbl}}s
				make.virtual_database = function(conn, target_env = rlang::caller_env(), ...){
					force(target_env);

					db <- purrr::modify_if(conn, is.numeric, \(x) names(private$connections)[x]);

					dbms <- DBI::dbGetInfo(private$connections[[db]]);
					dbms <- if (rlang::has_name(dbms, "dbms.name")){ dbms$dbms.name } else { dbms$dbname }

					db_env <- self[[db]];

					obj_queue <- rlang::enexprs(...) |>
								purrr::map(rlang::as_label) |>
								unlist() |>
								paste(collapse = "|");

					if (rlang::is_empty(obj_queue)){ return("No values passed to `...`: exiting ...") }

					.tables <- ls(db_env, pattern = "^(sys.)?tables$");
					.tables <- if (!rlang::is_empty(.tables)){
							.out <- suppressWarnings(db_env$metamap %look.for% obj_queue %>% unique())
							if (!rlang::is_empty(.out)) {
								unique(.out) |> data.table::setnames(c("table_schema", "view_name"), c("schema_name", "tbl_name"), skip_absent = TRUE)
							} else { .out }
						}

					.views <- ls(db_env, pattern = "^(sys.)?views$");
					.views <- if (!rlang::is_empty(.views)){
							.out <- suppressWarnings(db_env[[.views]] %look.for% obj_queue)
							if (!rlang::is_empty(.out)) {
								unique(.out) |> data.table::setnames(c("table_schema", "view_name"), c("schema_name", "tbl_name"), skip_absent = TRUE)
							} else { .out }
						}

					.assign_fun <- \(src, i) assign(i[[length(i)]] , dplyr::tbl(
									src = src
									, from = if (dbms == "MySQL"){
													dbplyr::in_schema(schema = dbplyr::sql(i$schema_name), table = dbplyr::sql(i$tbl_name))
												} else {
													dbplyr::in_catalog(
														catalog = dbplyr::sql(i$database)
														, schema = dbplyr::sql(i$schema_name)
														, table = dbplyr::sql(i$tbl_name)
														)
												}
									)
								, envir = target_env
								);

					.assign_fun <-  purrr::possibly(.assign_fun, otherwise = "Could not connect");

					rbindlist(list(.tables, .views) |> purrr::compact(), fill = TRUE) |>
						apply(MARGIN = 1, FUN = \(x) .assign_fun(src = private$connections[[db]], i = as.list(x)));

					invisible(self)
				}
      )}
    , active = { list(
    		#' @field connection.list Sets or returns a list of saved connections.  When providing a list, it should be names by database.
    		connection.list = function(i = NULL){
    				if (is.list(i)){ private$connections <- i } else { return(private$connections) }
    			}
    		)}
    , private = { list(connections = NULL)}
    )}
#
`%look.for%` <- function(i, x){
#' Look for a Database Metadata Reference
#'
#' The \code{\%look.for\%} operator searches the provided metadata environment or \code{metamap} object in such an environment for the Regex pattern passed to \code{x}.
#'
#' @param i (object) A \code{metamap} object or database environment (e.g., \code{DBOE$database}) containing the \code{metamap} object
#' @param x (string[]) A vector of REGEX patterns or exact names to use for matching against database object names
#'
#' @return A \code{\link[data.table]{data.table}} object with the items that were found, if any
#'
#' @export

  if (is.environment(i)){ if (rlang::env_has(i, "metamap")){ i <- i$metamap }}
  if (!is.data.table(i)){ i <- as.data.table(i) }

  .hits = i[, purrr::map(.SD, ~{
	    .needle = .x;
	    .haystack = x;
	    test_1 = which(.needle %in% .haystack)
	    test_2 = sapply(.haystack, function(.bale){ .needle %ilike% .bale }) |> which()
	    unlist(c(test_1, test_2)) |> unique()
	  }) |> unlist(use.names = FALSE)];

  # Verify that results exists, or exit if not
  if (identical(integer(0), .hits)){
    message(paste0("[FAIL]: <", paste(x, collapse = ", "), "> not found or failed to find matches."));
    return(NULL);
  }

  .out <- i[(.hits)] %>%
  			data.table::setattr(
	  			"group_cols"
	  			, intersect(c("database"
  										, "table_schema", "schema_name"
  										, "tbl_name", "proc_name", "view_name"
  										), names(i))
  			)

  if ("col_names" %in% names(.out)){
  	.out[!is.na(col_name), .(col_names = list(c(col_name))), by = c(attr(.out, "group_cols"))]
  } else { .out[, c(attr(.out, "group_cols")), with = FALSE] }
}
