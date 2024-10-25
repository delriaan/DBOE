make.virtual_database <- \(dboe = NULL, conn = NULL, ..., target_env = .GlobalEnv, sch = "dbo", exclude = NA){
  #' Make a Virtual Database of Table Links
  #' 
  #' @param dboe A \code{\link[DBOE]{DBOE}} object.
  #' @param conn (symbol, string) The name of a metadata environment (created after calling \code{get.metadata()}).
  #' @param ... \code{\link[rlang]{dots_list}} Names or patterns of objects for which links will be made.
  #' @param sch (symbo, string) The target schema (e.g., dbo) to use to select linked objects.
  #' @param target_env The environment object where created objects should be stored.
  #' @param exclude (string[]) Patterns and names to exclude from search.
  #' 
  #' @return None: DBI-sourced \code{\link[dplyr]{tbl}}s are assigned to \code{target_env}.
  #' 
  #' @export
  assertive::assert_is_identical_to_true(is(dboe, "DBOE"))
  conn <- as.character(rlang::enexpr(conn))
  sch <- as.character(rlang::enexpr(sch))
  assertive::assert_is_identical_to_true(conn %in% names(dboe$connection.list))
  conn <- dboe$connection.list[[conn]]
  db <- conn@info$dbname
  target_env <- as.environment(target_env)
  
  queue <- rlang::enexprs(...) |> 
    as.character() |>
    sapply(\(x){
      res <- dboe[[db]] %look.for% x

      if (rlang::is_empty(res) || nrow(res) == 0){ 
        NULL 
      } else { 
        res[(schema_name == sch), unique(tbl_name)] 
      }
    }) |>
    purrr::compact() |>
    unlist(use.names = FALSE)

  # Exclude table names matching the pattern in 'exclude':
  if (!is.na(exclude)){
    queue <- grep(paste(sprintf("(%s)", exclude), collapse = "|"), queue, value = TRUE, ignore.case = TRUE, invert = TRUE)
  }

  queue |>
    purrr::walk(\(x){
      this <- DBI::Id(catalog = db, schema = sch, table = x)
      assign(x, dplyr::tbl(conn, this), envir = target_env)
    }, .progress = TRUE)
}
