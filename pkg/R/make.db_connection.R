make.db_connection <- \(..., drv = odbc::odbc(), default){
  #' Make a Database Connection
  #' 
  #' This is a helper function that wraps \code{\link[DBI]{dbConnect}}.
  #' 
  #' @param drv A DBI-compliant driver
  #' @param ... Valid values passed to \code{\link[DBI]{dbConnect}}.
  #' 
  #' @return A \code{\link[DBI]{DBI}}-compliant connection object with attribute \code{odbc_string}
  #' @export
  
  conn_string <- if (!missing(default)){
      as.list(default)
    } else {
      rlang::dots_list(..., .named = TRUE, .ignore_empty = "all", .homonyms = "last")
    }

  if (!hasName(conn_string, "drv")){ conn_string$drv <- drv }

  expr <- rlang::expr(do.call(DBI::dbConnect, args = !!conn_string))
  
  magrittr::set_attr(eval(expr), "call", expr)
}
