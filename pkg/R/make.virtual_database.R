make.virtual_database <- function(dboe = NULL, conn = NULL, ..., target_env, sch = "dbo", exclude = NA){
  #' Make a Virtual Database of Table Links
  #' 
  #' @param dboe A \code{\link[DBOE]{DBOE}} object.
  #' @param conn (symbol, string) The name of a metadata environment (created after calling \code{get.metadata()}).
  #' @param ... \code{\link[rlang]{dots_list}} Symbols or strings for which table pointers will be made. Elements with a dot (.) indicate qualified names and will be treated as such, allowing cross-database endpoints to be referenced from a single connection.
  #' @param sch (symbol, string) The target schema (e.g., dbo) to use to select linked objects.
  #' @param target_env The environment object where created objects should be stored.  If no value is provided, `self[[conn]]` is attached as `dboe_<conn>`.
  #' @param exclude (string[]) Patterns and names to exclude from search.
  #' 
  #' @return None: DBI-sourced \code{\link[dplyr]{tbl}}s are assigned to \code{target_env}.
  #' 
  #' @export
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
  
  queue <- rlang::enexprs(..., .ignore_empty = "all") |>
    purrr::imap(\(x, nm){
      switch(
        class(x)
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
      if (nm == ""){ nm <- tail(attr(x, "name"), 1) }
      
      tryCatch({
        assign(nm, dplyr::tbl(.conn, x), envir = target_env)
        }, error = \(e){ 
          print(e)
          x <- parent.frame(4)$x |> 
            attr("name") |> 
            paste(collapse = ".")
          
          cli::cli_alert_warning("`{x}` not found or could not be accessed.")
        })
      }, .progress = TRUE)
}

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
