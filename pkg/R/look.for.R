#' Look for a Database Metadata Reference
#'
#' Searches a metadata environment or `metamap` object for the regular
#' expression patterns passed to `x`.
#'
#' @param haystack An object. Either a `metamap` object or a database
#'   environment, for example `DBOE$database`, containing `metamap`.
#' @param x A character vector of regular expression patterns or exact names to
#'   match against database object names.
#'
#' @return A [data.table::data.table()] with matching items, if any are found.
#'
#' @export
`%look.for%` <- function(haystack, x){
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

  .out <- data.table::setattr(
    haystack[(.hits)]
    , "group_cols"
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