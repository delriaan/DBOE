#' Get Database Metadata
#'
#' Retrieves metadata information for each database connection stored in a
#' [DBOE] object. 
#'
#' @param dboe A [DBOE] object.
#' @param ... (\code{\link[rlang]{dots_list}}) One or more symbols/names of connection environments 
#' if they already exist in the object environment.
#' 
#' For each database connection, an environment is
#' created within `dboe` containing a `metamap` object with columns `database`,
#' `tbl_name`, `col_name`, `schema_name`, `column_id`, and `data_type`.
#' Additionally, \code{\link{dboe_lazy_table}} objects are created for each table endpoint.
#'
#' @return Side effects only: warnings are thrown for non-existent connection environment names passed to \code{...}.
#'
#' @export
get.metadata <- \(dboe, ...){
  assertive::assert_is_identical_to_true(is(dboe, "DBOE"))
    dbs <- if (...length() > 0){ 
        rlang::enexprs(...) |> as.character() 
        } else { names(dboe$connection.list) }
    
    if (...length() > 0){
      purrr::walk(dbs, ~if (!.x %in% ls(dboe)){ cli::cli_alert_warning("{(.x)} not found!") })
    
      dbs <- dbs[dbs %in% ls(dboe) ]
      if (length(dbs) == 0){
        cli::cli_abort("None of the provided connection environments exist: exiting ...") 
      }
    }

  dboe$connection.list[dbs] |>
    purrr::iwalk(\(conn, db){
      .info_schema_name_map <- c(
          database = "TABLE_CATALOG"
          , tbl_name = "TABLE_NAME"
          , col_name = "COLUMN_NAME"
          , schema_name = "TABLE_SCHEMA"
          , column_id = "ORDINAL_POSITION"
          , data_type = "DATA_TYPE"
          )
      
      assign(db, new.env(), envir = dboe)
      proxy_env <- get(db, envir = dboe)
      # assign(".conn", conn, envir = proxy_env)
      makeActiveBinding(".conn", \() parent.env(environment())$conn, env = proxy_env)

      # res <- data.table::as.data.table(DBI::dbGetQuery(conn, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS"))
      res <- data.table::as.data.table(dplyr::tbl(conn, DBI::Id("INFORMATION_SCHEMA", "COLUMNS")))
      
      # Force upper-case just to be safe:
      data.table::setnames(res, toupper(names(res)))
      data.table::setnames(res, .info_schema_name_map, names(.info_schema_name_map))
      data.table::setcolorder(res, names(.info_schema_name_map))
      data.table::setkey(res, tbl_name, column_id)
      
      # Populate the appropriate 'DBOE' environment with table metadata:
      split(res, by = c("schema_name", "tbl_name")) |>
        purrr::imap(\(x, nm){
          tbl_name <- x$tbl_name[1]
          schema_name <- x$schema_name[1]
          assign(
            tbl_name
            , dboe_lazy_table(
                env = proxy_env
                , name = DBI::Id(schema_name, tbl_name)
                , meta = split(x, by = "col_name")
                )
            , envir = proxy_env
            )
        })
      #
      # Set the expected 'metamap' format:
      assign(
        "metamap"
        , res[
          , cbind(
              .SD[, database:data_type]
              , col_meta = purrr::discard_at(.SD, names(.info_schema_name_map)) |>
                  apply(1, \(x) as.list(x) |> jsonlite::toJSON())
              )
          ]
        , envir = proxy_env
        )
    }, .progress = TRUE)
}

#' DBOE Lazy Table
#'
#' \code{dboe_lazy_table} is a simple S7 class to represent a database table with attached metadata. 
#'
#' @slot env An environment containing the database connection and other relevant information.
#' @slot name The qualified name of the table (e.g., `DBI::Id("schema", "table")`).
#' @slot tbl A property that lazily evaluates to a `dplyr::tbl()` when accessed.
#' @slot meta A list of metadata information for the table, structured as a list of column metadata.
#'
#' @keywords internal
dboe_lazy_table <- S7::new_class(
  name = "dboe_lazy_table"
  , package = "DBOE"
  , properties = list(
      env = S7::class_environment
      , name = S7::class_any
      , tbl = S7::new_property(
          class = S7::class_any
          , getter = function(self){ dplyr::tbl(self@env[[".conn"]], self@name) }
          )
      , meta = S7::new_property(
          class = S7::class_list
          , setter = function(self, value){ self@meta <- value; self; }
          )
      )
  )
