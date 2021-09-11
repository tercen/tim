#' Serialize an object.
#'
#' This function allows you to serialize an object to a string.
#' 
#' @param object Object to be serialized.
#' @keywords utils
#' @export
#' @examples
#' serialize_to_string(iris)
serialize_to_string <- function(object) {
  con <- rawConnection(raw(0), "r+")
  saveRDS(object, con)
  str64 <- base64enc::base64encode(rawConnectionValue(con))
  close(con)
  return(str64)
}


#' Serialise an object.
#'
#' This function allows you to serialise an object to a string.
#' 
#' @param object Object to be serialised.
#' @keywords utils
#' @export
#' @examples
#' serialise_to_string(iris)
serialise_to_string <- function(object) {
  return(serialize_to_string(object))
} 


#' Deserialize a string
#'
#' This function allows you to deserialize a string to an object.
#' 
#' @param str64 String to be deserialized.
#' @keywords utils
#' @export
#' @examples
#' str <- serialize_to_string(iris)
#' iris2 <- deserialize_from_string(str)
deserialize_from_string <- function(str64) {
  con <- rawConnection(base64enc::base64decode(str64), "r+")
  object <- readRDS(con)
  close(con)
  return(object)
}


#' Deserialise a string
#'
#' This function allows you to deserialise a string to an object.
#' 
#' @param str64 String to be deserialised.
#' @keywords utils
#' @export
#' @examples
#' str <- serialise_to_string(iris)
#' iris2 <- deserialise_from_string(str)
deserialise_from_string <- function(str64) {
  return(deserialize_from_string(str64))
} 

#' Get a serialized tercen result
#'
#' This function allows you to prepare an object to be sent back to Tercen.
#' 
#' @param df Data frame containing results.
#' @param object Object to be serialized.
#' @param object_name Name of the object.
#' @param ctx Tercen context.
#' @keywords utils
#' @export
#' @examples
#' str <- serialise_to_string(iris)
#' iris2 <- deserialise_from_string(str)
get_serialized_result <- function(df, object, object_name, ctx) {
  
  df$.object <- object_name
  
  # explicitly create relation with .ci and input data
  columnTable <- ctx$cselect() %>%
    mutate(.ci = 0:(nrow(.) - 1))
  
  leftTable <- data.frame(df) %>%
    ctx$addNamespace() %>%
    left_join(columnTable, by = ".ci") %>%
    select(-.ci) %>%
    tercen::dataframe.as.table()
  leftTable$properties$name = 'left'
  
  leftRelation <- SimpleRelation$new()
  leftRelation$id <- leftTable$properties$name
  
  # the factor where the binary data base64 encoded is stored MUST start by a "." character so it wont be displayed to the user
  # the factor used in the join relation MUST have a different name then the one used in the leftTable 
  rightTable <- data.frame(
    model = object_name,
    .base64.serialized.r.model = c(serialize_to_string(object))
  ) %>%
    ctx$addNamespace() %>%
    tercen::dataframe.as.table()
  
  rightTable$properties$name <- 'right'
  rightRelation <- SimpleRelation$new()
  rightRelation$id <- rightTable$properties$name
  
  pair <- ColumnPair$new()
  pair$lColumns <- list(".object") # column name of the leftTable to use for the join
  pair$rColumns = list(rightTable$columns[[1]]$name) # column name of the rightTable to use for the join (note : namespace has been added)

  join.model = JoinOperator$new()
  join.model$rightRelation = rightRelation
  join.model$leftPair = pair
  
  # create the join relationship using a composite relation (think at a star schema)
  compositeRelation = CompositeRelation$new()
  compositeRelation$id = "compositeRelation"
  compositeRelation$mainRelation = leftRelation
  compositeRelation$joinOperators = list(join.model)
  
  pair_2 <- ColumnPair$new()
  pair_2$lColumns <- unname(ctx$cnames)
  pair_2$rColumns = unname(ctx$cnames) 
  
  join = JoinOperator$new()
  join$rightRelation = compositeRelation
  join$leftPair = pair_2
  
  result = OperatorResult$new()
  result$tables = list(leftTable, rightTable)
  result$joinOperators = list(join)

  return(result)
}