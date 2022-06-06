#' Set Tercen credentials.
#'
#' @param force Whether to force credentials update or not (default: FALSE).
#'
#' @export
set_tercen_credentials <- function(force = FALSE) {

  username <- getOption("tercen.username")
  if(is.null(username) | username == "admin" | force) options("tercen.username" = rstudioapi::askForPassword("Tercen username"))
  
  password <- getOption("tercen.password")
  if(is.null(password) | password == "admin" | force) options("tercen.password" = rstudioapi::askForPassword("Tercen password"))
  
  return(NULL)
  
}

#' Set workflow and data step IDs from URL.
#'
#' @param step_url Data step URL.
#' @param set_credentials Whether to set credentials (default is TRUE).
#' @param serviceUri Service URI.

#' @export
set_workflow_step_ids <- function(step_url, set_credentials = TRUE, serviceUri = NULL) {
  
  parsed_url <- httr::parse_url(step_url)
  if(parsed_url$hostname == "tercen.com") {
    options("tercen.serviceUri"= "https://tercen.com/api/v1/")
  } else if(is.null(serviceUri)) {
    stop("Please specify serviceUri")
  } else {
    options("tercen.serviceUri"= serviceUri)
  }
  
  splitted_url <- strsplit(parsed_url$path, "/")[[1]]
  workflowId <- splitted_url[which(splitted_url == "w") + 1]
  stepId <- splitted_url[which(splitted_url == "ds") + 1]
  
  if(any(is.na(c(workflowId, stepId)))) stop("Invalid URL.")
  options("tercen.workflowId"= workflowId)
  options("tercen.stepId"= stepId)
  
  if(set_credentials) set_tercen_credentials()
  
  return(NULL)
}






