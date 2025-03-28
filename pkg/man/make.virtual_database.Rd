% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/make.virtual_database.R
\name{make.virtual_database}
\alias{make.virtual_database}
\title{Make a Virtual Database of Table Links}
\usage{
make.virtual_database(
  dboe = NULL,
  conn = NULL,
  ...,
  target_env = .GlobalEnv,
  sch = "dbo",
  exclude = NA
)
}
\arguments{
\item{dboe}{A \code{\link[DBOE]{DBOE}} object.}

\item{conn}{(symbol, string) The name of a metadata environment (created after calling \code{get.metadata()}).}

\item{...}{\code{\link[rlang]{dots_list}} Names or patterns of objects for which links will be made.}

\item{target_env}{The environment object where created objects should be stored.}

\item{sch}{(symbo, string) The target schema (e.g., dbo) to use to select linked objects.}

\item{exclude}{(string[]) Patterns and names to exclude from search.}
}
\value{
None: DBI-sourced \code{\link[dplyr]{tbl}}s are assigned to \code{target_env}.
}
\description{
Make a Virtual Database of Table Links
}
