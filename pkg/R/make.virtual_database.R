#' Make a Virtual Database of Table Links
#'
#' Creates DBI-backed [dplyr::tbl()] objects in a target environment.
#'
#' @param dboe A [DBOE] object.
#' @param conn A symbol or character string naming a metadata environment
#'   created after calling [get.metadata()].
#' @param ... \code{\link[rlang]{dots_list}}: Symbols, calls, `DBI::Id()` objects, or character strings for
#'   which table pointers will be made. Elements containing `.` are treated as
#'   qualified names, allowing cross-database endpoints to be referenced from a
#'   single connection. 
#'   Nested lists will be flattened with names following the behavior of \code{\link[base]{unlist}}.
#' @param target_env An environment in which created objects should be stored.
#'   If omitted, `dboe[[conn]]` is attached as `dboe_<conn>`.
#' @param sch A symbol or character string giving the target schema to use when
#'   selecting linked objects.
#' @param exclude A character vector of patterns or names to exclude from the
#'   search.
#' @param lazy A boolean indicating whether to include `dboe_lazy_table` objects. 
#' Arguments to the dots list with matching names in the connection environment are overriden.
#'
#' @return Side effects only. DBI-sourced [dplyr::tbl()] objects are assigned to
#'   `target_env`.
#'
#' @export
make.virtual_database <- function(dboe = NULL, conn = NULL, ..., target_env, sch = "dbo", exclude = NA, lazy = TRUE){
  assertive::assert_is_identical_to_true(methods::is(dboe, "DBOE"))  
  conn <- as.character(rlang::enexpr(conn))
  assertive::assert_is_identical_to_true(conn %in% names(dboe$connection.list))

  if (!missing(target_env)){
    assertive::assert_is_environment(target_env)
  } else {
    .env <- paste0("dboe_", conn)

    if (!.env %in% search()){ 
      target_env <- attach(dboe[[conn]], name = .env) 
    } else {
      target_env <- as.environment(.env)
    }
  }

  .conn <- dboe$connection.list[[conn]]
  db <- if (methods::is(.conn, "DuckDB")){ 
      .conn@info$dbname |> fs::path_file() |> fs::path_ext_remove()
    } else if (methods::is(.conn, "duckdb_connection")){
      conn
    } else { .conn@info$dbname }
  
  sch <- as.character(rlang::enexpr(sch))
  
  # Create the assignment queue: ----
  queue <- unlist(rlang::enexprs(..., .ignore_empty = "all"))

  if (lazy){
    .lazy_objects <- purrr::keep(
          mget(ls(dboe[[conn]]), envir = dboe[[conn]])
          , ~methods::is(.x, "DBOE::dboe_lazy_table")
          )
    queue <- c(queue, .lazy_objects)
  }

  queue <- queue |>
    purrr::imap(\(x, nm){
      switch(
        class(x)[1]
        , `DBOE::dboe_lazy_table` = x
        , dboe_lazy_table = x
        , character = check_qualified(x, sch)
        , name = check_qualified(as.character(x), sch)
        , call = eval(x)
        , Id = x
        , `DBI::Id` = x
        , eval(x)
        )
      }) |>
    purrr::compact()
  #
  # Exclude table names matching the pattern in 'exclude': ----
  if (!is.na(exclude)){
    .pattern <- paste(sprintf("(%s)", exclude), collapse = "|") |> unlist()
    assertive::assert_is_of_length(.pattern, 1)
    queue <- purrr::discard(queue, ~any(grepl(.pattern, attr(.x, "name"))))
  }

  # Return: ----
  queue |>
    purrr::iwalk(\(x, nm){
      if (nm == ""){
        if (methods::is(x, "DBOE::dboe_lazy_table")){ 
          nm <- tail(attr(x@name, "name"), 1)
        } else {
          nm <- tail(attr(x, "name"), 1)
        }
      }
      
      x <- if (!methods::is(x, "DBOE::dboe_lazy_table")){
        tryCatch(
          dplyr::tbl(.conn, x)
          , error = \(e){ 
              cli::cli_alert_warning("`{x}` not found or could not be accessed.")
              return(NULL)
            }
          ) 
        } else { x@tbl }
      
      if (!rlang::is_empty(x)) assign(nm, x, envir = target_env)
    }, .progress = TRUE)
}

#' Normalize Qualified Database Identifiers
#'
#' Internal helper that converts schema-qualified names to [DBI::Id()] objects.
#'
#' @param .x A character vector of identifier components or a single qualified
#'   identifier string.
#' @param .sch A character string giving the default schema.
#'
#' @return A [DBI::Id()] object, or the original zero-length input.
#'
#' @keywords internal
check_qualified <- function(.x, .sch){
  if (any(grepl("[.]", .x))){
    .x <- unlist(strsplit(.x, ".", fixed = TRUE))
  }

  res <- if (length(.x) == 1){ 
    rlang::expr(DBI::Id(.sch, !!!.x)) 
    } else if (length(.x) == 0){ 
      .x 
    } else { 
      rlang::expr(DBI::Id(!!!.x)) 
    }

  return(eval(res))
}
