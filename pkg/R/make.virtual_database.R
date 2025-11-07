make.virtual_database <- \(dboe = NULL, conn = NULL, ..., target_env = .GlobalEnv, sch = "dbo", exclude = NA){
  #' Make a Virtual Database of Table Links
  #' 
  #' @param dboe A \code{\link[DBOE]{DBOE}} object.
  #' @param conn (symbol, string) The name of a metadata environment (created after calling \code{get.metadata()}).
  #' @param ... \code{\link[rlang]{dots_list}} Symbols or strings for which table pointers will be made. Elements with a dot (.) indicate qualified names and will be treated as such, allowing cross-database endpoints to be referenced from a single connection.
  #' @param sch (symbol, string) The target schema (e.g., dbo) to use to select linked objects.
  #' @param target_env The environment object where created objects should be stored.
  #' @param exclude (string[]) Patterns and names to exclude from search.
  #' 
  #' @return None: DBI-sourced \code{\link[dplyr]{tbl}}s are assigned to \code{target_env}.
  #' 
  #' @export
  assertive::assert_is_identical_to_true(methods::is(dboe, "DBOE"))  
  conn <- as.character(rlang::enexpr(conn))
  assertive::assert_is_identical_to_true(conn %in% names(dboe$connection.list))
  assertive::assert_is_environment(target_env)

  .conn <- dboe$connection.list[[conn]]
  db <- if (methods::is(.conn, "DuckDB")){ 
          .conn@info$dbname |> fs::path_file() |> fs::path_ext_remove()
        } else if (methods::is(.conn, "duckdb_connection") | methods::is(.conn, "SQLiteConnection")){
          conn
        } else { .conn@info$dbname }
  
  sch <- as.character(rlang::enexpr(sch))  
  queue <- as.character(rlang::enexprs(..., .ignore_empty = "all"))
  assertive::assert_is_atomic(queue)

  queue <- lapply(queue, \(x){
      # Return qualified paths as identifiers, making this the responsibility
      # of the user to supply a valid string:
      if (grepl("[.]", x)){
        return(do.call(DBI::Id, rlang::splice(strsplit(x, ".", fixed = TRUE)[[1]])))
      }
      
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
    .pattern <- paste(sprintf("(%s)", exclude), collapse = "|") |> unlist()
    assertive::assert_is_of_length(.pattern, 1)
    queue <- grep(.pattern, queue, value = TRUE, ignore.case = TRUE, invert = TRUE)
  }

  # Return:
  queue |>
    purrr::walk(\(x){
      if (!methods::is(x, "Id")){
        if (methods::is(.conn, "SQLiteConnection")){
          this <- DBI::Id(schema = sch, table = x)
        } else {
          this <- DBI::Id(catalog = db, schema = sch, table = x)
        }
      } else {
        this <- x
        x <- x@name |> paste(collapse = ".")
      } 
      assign(x, dplyr::tbl(.conn, this), envir = target_env)
    }, .progress = TRUE)
}
