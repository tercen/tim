#' Populate GitHub workflows
#'
#' Based on the output table and input projection from an operator, builds
#' the necessary files for a test case.
#' 
#' @param path Output path (default).
#' @keywords test
#' @export
#' @examples
#' populate_gh_workflow(type = "docker")
populate_gh_workflow <- function(type, output_path = "./.github/workflows") {
  if(!type %in% c("R", "docker")) stop("Type must be R or docker")
  
  if(type == "docker") {
    release_path <- system.file("extdata", "release_docker_operator.yml", package = "tim")
    ci_path <- system.file("extdata", "ci_docker_operator.yml", package = "tim")
  } else if(type == "R") {
    release_path <- system.file("extdata", "release_R_operator.yml", package = "tim")
    ci_path <- system.file("extdata", "ci_R_operator.yml", package = "tim")
  } else {
    stop("Error.")
  }
  
  if(!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
    warning("Output directory did not exist and has been created.")
  } 
  
  release_output_path <- paste0(output_path, "/release.yml")
  if(file.exists(release_output_path)) warning("Release workflow will be overwritten.")
  file.copy(
    release_path,
    release_output_path,
    overwrite = TRUE,
    recursive = FALSE,
    copy.mode = TRUE,
    copy.date = FALSE
  )
  
  ci_output_path <- paste0(output_path, "/ci.yml")
  if(file.exists(ci_output_path)) warning("CI workflow will be overwritten.")
  file.copy(
    ci_path,
    ci_output_path,
    overwrite = TRUE,
    recursive = FALSE,
    copy.mode = TRUE,
    copy.date = FALSE
  )
  
  return(NULL)
}