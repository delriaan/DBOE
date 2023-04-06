library(book.of.workflow);
load_unloaded(purrr
	, stringi
	, magrittr
	, rlang
	, DBI
	, httr
	, keyring
	, tictoc)

db_conns <- c("MySQL", "GW2DB") |> purrr::set_names() |> map(~dbConnect(odbc::odbc(), .x))
.x <- db_conns$MySQL
.y <- "MySQL"
{
					# Helper function
          check.etl_obj <- function(obj){ if (!hasName(proxy_env, obj)){ message(sprintf("<%s> Failed to retrieve %s", this.db, obj)) } }

					# Helper function
          post.op <- function(i, j, dbms = names(dbms_types)){
          	force(dbms);
          	force(i);

						rlang::exprs(
							default = {
								data.table::setDT(i) |>
			        		data.table::setnames(
			        			ifelse(dbms != "MySQL", "name", "TABLE_NAME")
			        			, paste0(ifelse(j == "tables", "tbl", ifelse(j == "columns", "col", "schema")), "_name")
			        			) %>% data.table::setnames(names(.) |> tolower());

		        		if (j != "schemas"){
		          			data.table::setkeyv(i, cols = intersect(c("schema_id", "parent_object_id", "object_id", "table_schema", "table_name", "column_name"), names(i) |> tolower()))
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
							)[[j]] |> eval()
           }

					# purrr::iwalk(db_conns, ~{
						# An ODBC connection for each value in .y must be created to use this mapper as-is.
						proxy_env <- new.env();
						this.db <- .x@info$dbname;
						this.dbms <- class(.x)
						neo.conn <- .x;
						assign(.y, proxy_env, envir = self);

	  				dbms_types <- { list(
	          	`Microsoft SQL Server` = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "procedures", "types", "schemas", "synonyms")
	          			, data = DBI::dbReadTable(conn = neo.conn, name = DBI::Id(schema = "sys", table = x))
	          			, procedures = DBI::dbGetQuery(conn = neo.conn, "SELECT [name], [schema_id], [object_id], proc_create_date = [create_date], proc_upd_date = [modify_date] FROM sys.procedures")
	          			)
	          	, MySQL = rlang::exprs(
	          			meta = !!c("tables", "columns", "views", "routines")
	          			, data = DBI::dbReadTable(conn = neo.conn, name = DBI::Id(schema = "INFORMATION_schema", table = x))
	          			, procedures = DBI::dbGetQuery(conn = neo.conn, "SELECT * FROM INFORMATION_SCHEMA.routines")
	          			)
	          	)} %>% .[which(names(.) == this.dbms)];

						req.objs <- dbms_types[[1]]$meta

            tictoc::tic("\tMetadata -> " %s+% this.db);

            message(sprintf("Processing metadata for <%s>", this.db));

            # Tables, Columns, Schemas (VALIDATE: MySQL[1] MSSQL[?]) ====
            purrr::keep(req.objs, ~.x %in% c("tables", "columns", "schemas", "views")) |>
            	purrr::iwalk(~{
	            	x <- .x;
			          assign(.x, dbms_types[[1]]$data |> eval() |> post.op(ifelse(x == "views", x, "default")), envir = proxy_env)
			          check.etl_obj(req.objs[[.y]])
	            })

            # Procs (VALIDATE: MySQL[1] MSSQL[?]) ====
						assign(dbms_types[[1]]$meta[4], dbms_types[[1]]$procedures |> eval() |> post.op("procedures"), envir = proxy_env)
						check.etl_obj(req.objs[4]);

            # Types (VALIDATE: MySQL[1] MSSQL[?]) ====
            if (this.dbms != "MySQL"){
            	assign("sys.types", DBI::dbGetQuery(neo.conn, "SELECT [name], system_type_id, schema_id FROM sys.types") |> post.op("types"), envir = proxy_env);
            	check.etl_obj(iterators::nextElem(req.objs));
            }


						# Combined Metadata (VALIDATE: MySQL[?] MSSQL[?]) ====
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

            # Active bindings (VALIDATE: MySQL[?] MSSQL[?]) ====
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
          # });

          invisible(self);
					# })
        }


dbGetQuery(db_conns$MySQL, "SELECT * FROM INFORMATION_SCHEMA.tables LIMIT 1000") |> View()
dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.tables") |> names()

dbGetQuery(db_conns$MySQL, "SELECT * FROM INFORMATION_SCHEMA.columns LIMIT 1000") |> View()
dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.columns") |> names()

dbGetQuery(db_conns$MySQL, "SELECT * FROM INFORMATION_SCHEMA.views LIMIT 10") |> View()
dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.views") |> names()

dbGetQuery(db_conns$MySQL, "SELECT * FROM INFORMATION_SCHEMA.routines LIMIT 10") |> names()
dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.procedures") |> names()

dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.types") |> names()
dbGetQuery(db_conns$GW2DB, "SELECT TOP 10 * FROM sys.schemas") |> names()
# load_unloaded(DBOE)
# X <- DBOE$new(list(GW2DB = DBI::dbConnect(odbc::odbc(), "GW2DB")))
# X$get.metadata()
# X$GW2DB$dim_character |> head()
# GW2DB <- new.env()
# X$make.virtual_database("GW2DB", db_env = GW2DB)
# View(X)

dbDisconnect(db_conns)
