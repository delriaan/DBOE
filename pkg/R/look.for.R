`%look.for%` <- function(haystack, x){
	#' Look for a Database Metadata Reference
	#'
	#' The \code{\%look.for\%} operator searches the provided metadata environment or \code{metamap} object in such an environment for the Regex pattern passed to \code{x}.
	#'
	#' @param haystack (object) A \code{metamap} object or database environment (e.g., \code{DBOE$database}) containing the \code{metamap} object
	#' @param x (string[]) A vector of REGEX patterns or exact names to use for matching against database object names
	#'
	#' @return A \code{\link[data.table]{data.table}} object with the items that were found, if any
	#'
	#' @export

  haystack <- switch(
    is.environment(haystack)
    , "TRUE" = { if (rlang::env_has(haystack, "metamap")){ haystack$metamap } else { haystack } }
    , "FALSE" = { data.table::as.data.table(haystack) }
    )

	.needle <- paste(sprintf("(%s)", x), collapse = "|")
  .hits <- purrr::map(haystack, \(bale) which(data.table::`%ilike%`(bale, .needle))) |> 
    unlist(use.names = FALSE) |> 
    unique() |> 
    sort()

	if (rlang::has_length(.hits, 0)){
		cli::cli_alert_warning("No object matches found: exiting ...")
		return()
	}

  .out <- data.table::setattr(haystack[(.hits)], "group_cols"
          , intersect(
              c("database", "table_schema", "schema_name", "tbl_name", "proc_name", "view_name")
              , names(haystack)
              )
  				)

	# Check for hits in the 'col_names' field of the metamap since this will cause other
	# fields to duplicate rows:
  if ("col_name" %in% names(.out)){
  	.out[!is.na(col_name), .(col_names = list(c(col_name))), by = c(attr(.out, "group_cols"))]
  } else {
  	.out[, c(attr(.out, "group_cols")), with = FALSE]
  }
}
