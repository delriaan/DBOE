get.metadata <- \(dboe){
  #' Get Database Metadata
  #' 
  #' \code{get.metadata()} retrieves metadata information for the database pointed to by argument \code{conns}. Once metadata has been retrieved, metadata can be accessed for tables, views, and stored procedures using the following access method: \code{<DBOE obj>$<database name>$<table/view/proc name>}
  #' 
  #' @param dboe A \code{\link[DBOE]{DBOE}} object
  #'
  #' @return \itemize{
  #' \item{An object named \code{metamap} is created in \code{<DBOE obj>$<database name>} having the following columns: \code{database}, \code{tbl_name}, \code{col_name}, \code{schema_name}, \code{column_id}, \code{data_type}.}
  #' \item{For each table name in \code{metamap}, an object is created in \code{<DBOE obj>$<database name>} containing column metadata. This is essentially the same data, but allows for easier navigation using R's accessor function.}
  #' }
  #' 
  #' @export
  assertive::assert_is_identical_to_true(is(dboe, "DBOE"))
  .info_schema_name_map <- c(
      database = "TABLE_CATALOG"
      , tbl_name = "TABLE_NAME"
      , col_name = "COLUMN_NAME"
      , schema_name = "TABLE_SCHEMA"
      , column_id = "ORDINAL_POSITION"
      , data_type = "DATA_TYPE"
      )

  dboe$connection.list |>
    purrr::iwalk(\(conn, db){      
      assign(db, new.env(), envir = dboe)
      proxy_env <- get(db, envir = dboe)

      if (!methods::is(conn, "SQLiteConnection")){
        res <- dplyr::tbl(conn, DBI::Id("INFORMATION_SCHEMA", "COLUMNS")) |> 
          data.table::as.data.table()
      } else {
        sqlite_master <- dplyr::tbl(conn, "sqlite_master")
        pragma_table_list <- dplyr::tbl(conn, "pragma_table_list")
        master_pragma <- sqlite_master |> 
          dplyr::filter(type == "table") |> 
          dplyr::inner_join(pragma_table_list, by = c("name", "type")) |>
          dplyr::mutate(TABLE_CATALOG = db) |>
          dplyr::select(TABLE_CATALOG, TABLE_NAME = name, TABLE_SCHEMA = schema, sql) |>
          data.table::as.data.table()

        pragma_cols <- dplyr::pull(master_pragma, sql) |>
          stringi::stri_replace_all_regex("[(,)`]|(CREATE TABLE)", "", vectorize_all = FALSE) |>
          lapply(\(x){
            res <- stringi::stri_split_regex(x, "\n", simplify = TRUE) |>
              as.vector() |>
              trimws()

            data.table::data.table(
              res[1]
              , stringi::stri_split_fixed(res[-1], " ", simplify = TRUE)
              ) |>
              data.table::setnames(c("TABLE_NAME", "COLUMN_NAME", "DATA_TYPE")) |>
              _[, ORDINAL_POSITION := .I]
          }) |>
          data.table::rbindlist() |>
          _[COLUMN_NAME != ""]

        res <- master_pragma |>
          dplyr::inner_join(pragma_cols, by = "TABLE_NAME") |>
          dplyr::select(-sql) |>
          data.table::as.data.table()
      }
      
      # Force upper-case just to be safe 
      data.table::setnames(res, toupper(names(res)))
      data.table::setnames(res, .info_schema_name_map, names(.info_schema_name_map))
      data.table::setcolorder(res, names(.info_schema_name_map))
      data.table::setkey(res, tbl_name, column_id)
      
      # Populate the appropriate 'DBOE' environment with table metadata:
      split(res, by = c("schema_name", "tbl_name")) |>
        purrr::imap(\(x, nm){
          # browser()
          tbl_name <- x$tbl_name[1]
          assign(tbl_name, split(x, by = "col_name"), envir = proxy_env)
        })
      
      # Set the expected 'metamap' format:
      assign("metamap", res[
        , cbind(
            .SD[, database:data_type]
            , col_meta = {
                .meta <- purrr::discard_at(.SD, names(.info_schema_name_map))
                if (rlang::is_empty(.meta)) {
                  NULL
                } else {
                  apply(.meta, 1, \(x) as.list(x) |> jsonlite::toJSON())
                }
              }
            )
        ], envir = proxy_env)
    }, .progress = TRUE)
}
