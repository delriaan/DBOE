#' Database Object Explorer
#'
#' @description
#' \code{DBOE} facilitates the navigation of SQL-based database engines by means of metadata. Engines tested to date include Microsoft SQL Server and MySQL.
#'
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
			#' 
			#' @param ... \code{\link[rlang]{dots_list}} A named list of \code{\link[DBI]{DBI}} connection objects.
      #' @return The class object, invisibly
      initialize = function(...){
				if (...length() == 0){
					stop("No DBI connection objects provided to `...`: aborting")
				} else {
					private$connections <- rlang::dots_list(..., .named = TRUE, .ignore_empty = "all", .homonyms = "last")
					purrr::walk(names(private$connections), \(db) assign(db, new.env(), envir = self))
				}

        invisible(self);
      }
    )}
  , active = { list(
  		#' @field connection.list Sets or returns a list of saved connections.  When providing a list, it should be names by database.
  		connection.list = function(i = NULL){
				if (is.list(i)){ 
					private$connections[] <- i
					return()
				} else { 
					return(private$connections) 
				}
			}
		)}
  , private = { list(connections = NULL)}
  )}
