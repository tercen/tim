% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/serialisation_utils.R
\name{deserialise_from_string}
\alias{deserialise_from_string}
\title{Deserialise a string}
\usage{
deserialise_from_string(str64)
}
\arguments{
\item{str64}{String to be deserialised.}
}
\description{
This function allows you to deserialise a string to an object.
}
\examples{
str <- serialise_to_string(iris)
iris2 <- deserialise_from_string(str)
}
\keyword{utils}
