make.db_connection <- \(..., drv = odbc::odbc(), default){
  #' Make an ODBC MSSQL Database Connection
  #' 
  #' This is a helper function that wraps \code{\link[DBI]{dbConnect}}
  #' 
  #' @param drv A DBI-compliant driver
  #' @param ... Valid values passed to \code{\link[DBI]{dbConnect}}.
  #' 
  #' @return A \code{\link[DBI]{DBI}}-compliant connection object.
  #' @export
  .odbc_string <- if (!missing(default)){ 
      as.list(default) 
    } else { 
      rlang::dots_list(..., .named = TRUE, .ignore_empty = "all", .homonyms = "last") 
    }

  if (!hasName(.odbc_string, "drv")){
    .odbc_string$drv <- drv
  }

  do.call(DBI::dbConnect, args = .odbc_string)
}
