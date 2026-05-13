#' Make a Database Connection
#'
#' Helper that wraps [DBI::dbConnect()].
#'
#' @param ... Named arguments passed to [DBI::dbConnect()].
#' @param drv A DBI-compliant driver object.
#' @param default An optional named list of default connection arguments.
#'
#' @return A DBI-compliant connection object with a `call` attribute storing the
#'   connection expression.
#'
#' @export
make.db_connection <- \(..., drv = odbc::odbc(), default){
  conn_string <- if (!missing(default)){
      as.list(default)
    } else {
      rlang::dots_list(..., .named = TRUE, .ignore_empty = "all", .homonyms = "last")
    }

  if (!hasName(conn_string, "drv")){ conn_string$drv <- drv }

  expr <- rlang::expr(do.call(DBI::dbConnect, args = !!conn_string))
  
  magrittr::set_attr(eval(expr), "call", expr)
}
