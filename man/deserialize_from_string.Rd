% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/serialisation_utils.R
\name{deserialize_from_string}
\alias{deserialize_from_string}
\title{Deserialize a string}
\usage{
deserialize_from_string(str64)
}
\arguments{
\item{str64}{String to be deserialized.}
}
\description{
This function allows you to deserialize a string to an object.
}
\examples{
str <- serialize_to_string(iris)
iris2 <- deserialize_from_string(str)
}
\keyword{utils}
