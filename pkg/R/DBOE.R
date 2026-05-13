.onLoad <- function(...) {
  S7::methods_register()
}

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
DBOE <- R6::R6Class(
  classname = "DBOE"
  , lock_objects = FALSE
  , public = list(
      #' @description
      #' Initialize the class object
			#' @param ... \code{\link[rlang]{dots_list}}: named DBI-compliant connection objects
      #' @return The class object, invisibly
      initialize = function(...){
				if (...length() > 0){
					if (!any(...names() == "")){
						rlang::list2(...) |>
							purrr::iwalk(\(conn, nm){
								if (DBI::dbIsValid(conn)){
									private$connections[[nm]] <<- conn
								} else {
									cli::cli_alert_danger("{nm} is not a valid database connection: skipping ...")
								}
							})
					} else {
						cli::cli_abort("All arguments must be named or empty: aborting ...")
					}
				}

        invisible(self)
      }
			#' @description
			#' Refresh Database Connections
			#' @param ... \code{\link[rlang]{dots_list}}: Symbols or names of connections to refresh (e.g., `names(dboe$connection.list)`)
			#' @return None: Matching connections in `dboe$connection.list` are reinitialized.
			, reconnect = function(...){
					if (...length() == 0){
						conns <- private$connections
					} else {
						conns <- private$connections[rlang::enexprs(...) |> as.character()]
					}
				
					purrr::iwalk(conns, ~tryCatch({
						if (DBI::dbIsValid(.x)){ 
							cli::cli_alert_info("Connection '{(.y)}' is valid: no action taken ...")
							return()
						}
						cli::cli_alert_info("Attempting to refresh connection '{(.y)}' ...")

						drv <- attr(.x, "driver")
						db <- fs::path_file(drv@dbdir)
						private$connections[[.y]] <<- make.db_connection(
							Server = "localhost"
							, Database = db
							, drv = duckdb::duckdb(dbdir = db)
							)
						if (DBI::dbIsValid(private$connections[[.y]])){
							assign(".conn", private$connections[[.y]], envir = self[[.y]])
							cli::cli_alert_success("Connection '{(.y)}' successfully refreshed!")
						}
					}, error = \(e) print(e)))
				}
			)
  , active = list(
  		#' @field connection.list Sets or returns a list of saved connections.  When providing a list, it should be names by database.
  		connection.list = function(i = NULL){
  				if (is.list(i)){ 
						private$connections <- i
					} else { 
						return(private$connections) 
					}
  			}
  		)
  , private = { list(connections = NULL)}
  )
#