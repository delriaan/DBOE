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
  , public = { list(
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
    )}
  , active = { list(
  		#' @field connection.list Sets or returns a list of saved connections.  When providing a list, it should be names by database.
  		connection.list = function(i = NULL){
  				if (is.list(i)){ private$connections <- i } else { return(private$connections) }
  			}
  		)}
  , private = { list(connections = NULL)}
  )
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
  if (!data.table::is.data.table(i)){ i <- data.table::as.data.table(i) }

  .hits <- i[, purrr::map(.SD, \(.needle){
	    .haystack = x
	    test_1 = which(.needle %in% .haystack)
	    test_2 = sapply(.haystack, function(.bale){ .needle %ilike% .bale }) |> which()
	    unlist(c(test_1, test_2)) |> unique()
	  }) |> 
		unlist(use.names = FALSE)]

  # Verify that results exists, or exit if not
  if (identical(integer(0), .hits)){
    message(paste0("[FAIL]: <", paste(x, collapse = ", "), "> not found or failed to find matches."))
    return(NULL)
  }

  .out <- i[(.hits)] |>
  			data.table::setattr(
	  			"group_cols"
	  			, intersect(c("database", "table_schema", "schema_name", "tbl_name", "proc_name", "view_name"), names(i))
  				)

  if ("col_names" %in% names(.out)){
  	.out[!is.na(col_name), .(col_names = list(c(col_name))), by = c(attr(.out, "group_cols"))]
  } else {
  	.out[, c(attr(.out, "group_cols")), with = FALSE]
  }
}
