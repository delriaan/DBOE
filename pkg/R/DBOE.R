#' Database Object Explorer
#'
#' @description
#' \code{DBOE} facilitates the navigation of SQL Server DBMS objects by way of retrieving the metatadata objects.
#' It also provides methods to retrieve information from the data catalog: specifically, the \href{http://datacatalog.alliance.local/en/datacatalog/edwchangelog}{EDW changelog}.  Once retrieved, changes can be compared against user-supplied source code
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
        #' @description
        #' Initialize the class object
        #' @return The class object, invisibly
        initialize = function(){
          invisible(self);
        },
        #' @description
        #' \code{$get.metadata()} retrieves metadata information for the database pointed to by argument \code{conns}. Once metadata has been retrieved, metadata can be accessed for tables, views, and stored procedures using the following access method: \code{<DBOE obj>$<database name>$<table/view/proc name>}
        #' @param ... \code{\link[rlang]{dots_list}}: one or more DBI/ODBC connection objects.  Unnamed entries will be given generic names.
        #' @param chatty (logical) When \code{TRUE}, additional execution messages are sent to the console
        #' @family metadata
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

						rlang::exprs(
							default = {
								data.table::setDT(i) |>
			        		data.table::setnames(
			        			ifelse(dbms != "MySQL", "name", "TABLE_NAME")
			        			, paste0(ifelse(j == "tables", "tbl", ifelse(j == "columns", "col", "schema")), "_name")
			        			) %>%
									data.table::setnames(names(.) |> tolower());

		        		if (j != "schemas"){
		          			data.table::setkeyv(
		          				i, cols = intersect(c("schema_id", "parent_object_id", "object_id"
		          															, "table_schema", "table_name", "column_name")
		          														, names(i) |> tolower()))
		          		} else { i }
	        		}
							, views = {
									i <- if (dbms != "MySQL"){
												data.table::as.data.table(i)[
				                proxy_env$sys.schemas[!(schema_name %ilike% schema.filter), c("schema_id", "schema_name")]
				                , on = "schema_id", nomatch = 0
				                ][, view_def := purrr::map2_chr(schema_name, name, ~{
				                    DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
				                    unlist() |>
				                		purrr::reduce(paste0)
				                  }) |>
				                	purrr::modify_if(rlang::is_empty, .f = ~"##NO DEF##")
				                ]
											} else {
												data.table::as.data.table(i) %>%
													data.table::setnames(names(.) |> tolower()) |>
													data.table::setnames("view_definition", "view_def") |>
				                	purrr::modify_at("view_def", .f = purrr::modify_if, rlang::is_empty, ~"##NO DEF##")
											}

		          		data.table::setnames(i, ifelse(dbms != "MySQL", "name", "table_name"), "view_name")

			        		if (j != "schemas"){
			          			data.table::setkeyv(i, cols = intersect(c("schema_id", "parent_object_id", "object_id", "table_schema", "view_name", "column_name"), names(i) |> tolower()))
			          		} else { i }
		        		}
							, types = {
									data.table::as.data.table(i)[, .(data_type = paste(name, collapse = ", ")), by = c("schema_id", "system_type_id")] |>
            			data.table::setkey(schema_id, system_type_id)
								}
              , procedures = {
              		i <- if (dbms != "MySQL"){
		              			data.table::as.data.table(i)[
					                proxy_env$sys.schemas[!(schema_name %ilike% "^(sys|infor|mdm|guest)"), c("schema_id", "schema_name")]
					                , on = "schema_id", nomatch = 0
					                ][, proc_def := purrr::map2(schema_name, name, ~{
					                    DBI::dbGetQuery(neo.conn, statement = sprintf("EXEC sp_helptext N'%s.%s.%s'", this.db, .x, .y)) |>
					                      unlist() |> purrr::reduce(paste0)
					                  }) |> unlist()
					                ] |>
			            			data.table::setkey(schema_id, proc_name)
			              		} else {
													data.table::as.data.table(i) %>%
														data.table::setnames(names(.) |> tolower()) |>
														data.table::setnames("routine_definition", "proc_def") |>
					                	purrr::modify_at("proc_def", .f = purrr::modify_if, rlang::is_empty, ~"##NO DEF##")
			              		}
	            		data.table::setnames(i, ifelse(dbms != "MySQL", "name", "routine_name"), "proc_name")

			        		if (j != "schemas"){
			          			data.table::setkeyv(i, cols = intersect(c("schema_id", "parent_object_id", "object_id", "table_schema", "proc_name", "column_name"), names(i) |> tolower()))
			          		} else { i }
            		}
							)[[ifelse(j %in% c("tables", "columns", "views", "schemas"), "default", j)]] |> eval()
           }

          # Execution
          .queue <- rlang::dots_list(
          		...
          		, .ignore_empty = "none"
          		, .homonyms = "last"
          		, .check_assign = TRUE
          		);

          names(.queue) <- ifelse(names(.queue) == ""
          					 , paste0("db_", stringi::stri_pad_left(seq_along(names(.queue)), width = 2, pad = "0"))
          					 , names(.queue)
          					 );

          	purrr::iwalk(.queue, ~{
						# An ODBC connection for each value in .y must be created to use this mapper as-is.
						proxy_env <- new.env()

						this.db <- { attr(.x, "odbc") |>
								attr("connection.string") |>
								stringi::stri_split_fixed(";", simplify = TRUE) |>
								purrr::keep(~grepl("DATABASE", .x)) |>
								stringi::stri_split_fixed("=", simplify = TRUE) |>
								magrittr::extract(2)
						}

						if (is.na(this.db)){ this.db <- .x@info$dbname }

						this.dbms <- class(.x);
						if (this.dbms != "MySQL"){ this.dbms <- "Microsoft SQL Server"}

						neo.conn <- private$connections[[.y]] <- .x;
						assign(.y, proxy_env, envir = self);

	  				dbms_types <- { list(
	          	`Microsoft SQL Server` = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "procedures", "schemas", "types", "synonyms")
	          			, data = DBI::dbReadTable(conn = neo.conn, name = DBI::Id(schema = "sys", table = x))
	          			, procedures = DBI::dbGetQuery(conn = neo.conn, "SELECT [name], [schema_id], [object_id], proc_create_date = [create_date], proc_upd_date = [modify_date] FROM sys.procedures")
	          			)
	          	, MySQL = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "routines")
	          			, data = DBI::dbReadTable(conn = neo.conn, name = DBI::Id(schema = "INFORMATION_schema", table = x))
	          			, procedures = DBI::dbGetQuery(conn = neo.conn, "SELECT * FROM INFORMATION_SCHEMA.routines")
	          			)
	          	)} %>% .[max(c(which(names(.) == this.dbms | grepl("ODBC", names(.))), 1))];

						req.objs <- dbms_types[[1]]$meta |> purrr::set_names();

            message(sprintf("Processing metadata for <%s>", this.db));
            tictoc::tic("\tMetadata -> " %s+% this.db);

            # Tables, Columns, Schemas (VALIDATE: MySQL[1] MSSQL[?]) ====
            purrr::keep(req.objs, ~.x %in% c("schemas", "tables", "columns", "views"))  |>
            	purrr::iwalk(~{
	            	x <- .x;
	            	y <- paste0(ifelse(this.dbms=="MySQL", "", "sys."), req.objs[.x]);
			          .out <- dbms_types[[1]]$data |> eval();

			          if ((nrow(.out) %||% 0) > 0){
			          	assign(y, post.op(i = .out, j = x, dbms = names(dbms_types)), envir = proxy_env)
			          	check.etl_obj(y);
			          }
	            });

            # Procs (VALIDATE: MySQL[1] MSSQL[1]) ====
						.this <- dbms_types[[1]]$procedures |> eval();

						if ((nrow(.this) %||% 0) > 0){
							paste0(ifelse(this.dbms=="MySQL", "", "sys.")
										 , dbms_types[[1]]$meta[which(grepl("procedure|routine", req.objs))]
										 ) |>
								assign(value = post.op(i = .this, j = "procedures", dbms = names(dbms_types)), envir = proxy_env)

							check.etl_obj(paste0(
								ifelse(this.dbms=="MySQL", "", "sys.")
								, req.objs[which(grepl("procedure|routine", req.objs))]
								));
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
            		metamap <- sys.tables[
		              sys.schemas[, !"principal_id"]
		              , on = "schema_id", .(tbl_name, schema_name, object_id)
		              ][
		              sys.columns[
		              	sys.types
		              	, on = "system_type_id"
		              	, .(col_name, column_id, data_type, system_type_id, max_length
		              			, precision, scale, is_nullable, is_identity, is_computed, is_xml_document, object_id
		              			)]
		              , on = c("object_id"), nomatch = 0
		              ][
		              , c("object_id", "database", "col_meta") := { list(NULL, this.db
		                  , apply(
		                  		X = data.table(system_type_id, max_length, precision, scale, is_nullable
		                  									 , is_identity, is_computed, is_xml_document)
		                  		, MARGIN = 1
		                  		, FUN = list
		                  		, simplify = FALSE
		                  		))
		              		}
		              ][, !c("system_type_id", "max_length", "precision", "scale"
		              			 , "is_nullable", "is_identity", "is_computed", "is_xml_document")
	              	] |>
	              data.table::setcolorder(c("database", "tbl_name", "col_name")) |>
	              data.table::setkey(tbl_name, column_id);
	            }
            	, MySQL = proxy_env %$% {
            			metamap <- columns[
	            		, .(database = this.db
		            			, column_id = column_key
			            		, column_type, ordinal_position
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
				            			)] |> purrr::array_branch(1)
			            	), by = .(table_schema, col_name = column_name, data_type)
		            	][
		            	tables[, .(table_schema, tbl_name, table_type, max_data_length, table_comment)] |> unique()
		            	, on = c("table_schema"), nomatch = 0, allow.cartesian = TRUE
		            	] |>
            			data.table::setnames("table_schema", "schema_name") |>
	            		data.table::setcolorder(c("database", "tbl_name", "col_name")) |>
	            		data.table::setkey(tbl_name, column_id);
	            	}
            	);

            .meta_idx <- c(grepl("microsoft|odbcconnection", this.dbms, ignore.case = TRUE)
            							 , grepl("mysql", this.db, ignore.case = TRUE)) |> which();

            rlang::eval_tidy(metamap_action[[.meta_idx]]);

            # Active bindings (VALIDATE: MySQL[1] MSSQL[1]) ====
            message("Creating active bindings")

            # Stored procedures (VALIDATE: MySQL[1] MSSQL[1])
						if ((nrow(proxy_env$sys.procedures) %||% 0) > 0){
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

						if ((nrow(proxy_env$views) %||% 0) > 0){
	            .temp <- proxy_env$views[
	            					!is.na(schema_name)
	            					, .(view_name = schema_name, schema_name = table_schema, view_definition)
	            					]

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
				#' \code{$make.virtual_database} creates a set of \code{\link[dplyr]{tbl}} objects in an environment
				#' @param conn The name of a metadata environment (created after calling \code{$get.metadata() })
				#' @param db_env The environment object where created objects should be stored
				#' @param ... The names of objects to retrieve: retrieves all tables and views if left blank.  If given as named arguments, the name becomes the local object name
				#' @return An assignable environment object with \code{DBI}-sourced \code{\link[dplyr]{tbl}}s
				make.virtual_database = function(conn, db_env = rlang::caller_env(), ...){
					db <- purrr::modify_if(conn, is.numeric, ~names(private$connections)[.x]);

					all_objs <- self[[db]] %$% {
						.tables <- sys.tables[sys.schemas, on = "schema_id"][!is.na(tbl_name)] %$% {
							rlang::set_names(
								purrr::map2(dplyr::sql(schema_name), dplyr::sql(tbl_name), dbplyr::in_schema)
								, tbl_name
								)
						}

						.views <- sys.views[sys.schemas, on = "schema_id"][!is.na(view_name)] %$% {
							rlang::set_names(
								purrr::map2(dplyr::sql(schema_name), dplyr::sql(view_name), dbplyr::in_schema)
								, view_name
								)
						}

						c(.tables, .views) |> names() |> purrr::set_names()
					}

					obj_queue <- rlang::enexprs(..., .named = TRUE) |>
						purrr::map(rlang::as_label) |>
						unlist() %||% all_objs;

					obj_queue[obj_queue %in% all_objs] |>
						purrr::discard(duplicated) |>
						purrr::iwalk(purrr::possibly(~{
							assign(.y, dplyr::tbl(src = private$connections[[db]], from = .x), envir = db_env)
							}, otherwise = "Could not connect"));

					invisible(self)
				}
      )}
    , active = { list(
    		#' @field connection.list Sets or returns a list of saved connections
    		connection.list = function(i = NULL){
    				if (is.list(i)){ private$connections <- i } else { return(private$connections) }
    			}
    		)}
    , private = { list(connections = NULL)}
    )}
#
`%look.for%` <- function(i, x){
#' Look for a Database Metadata
#'
#' The \code{\%look.for\%} operator searches the provided metadata environment or \code{metamap} object in such an environment for the pattern passed to \code{x}
#' @param i (object) A data.frame, data.table, or coercible containing object names
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

