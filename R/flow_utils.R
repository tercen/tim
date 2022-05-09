#' Convert a matrix into a flowFrame object.
#'
#' This function allows you to convert a matrix obtained from a tercen context
#' into a flowFrame object.
#' 
#' @param object Object to be serialized.
#' @keywords utils
#' @export
#' @examples
#' serialize_to_string(iris)
#' @importFrom Biobase AnnotatedDataFrame pData varMetadata
#' @importFrom matrixStats colMins colMaxs
#' @importFrom flowCore flowFrame
matrix_to_flowFrame <- function(data) { 
  
  minRange <- matrixStats::colMins(data)
  maxRange <- matrixStats::colMaxs(data)
  rnge <- maxRange - minRange
  
  df_params <- data.frame(
    name = colnames(data),
    desc = colnames(data),
    range = rnge,
    minRange = minRange,
    maxRange = maxRange
  )
  
  params <- Biobase::AnnotatedDataFrame()
  Biobase::pData(params) <- df_params
  Biobase::varMetadata(params) <- data.frame(
    labelDescription = c("Name of Parameter",
                         "Description of Parameter",
                         "Range of Parameter",
                         "Minimum Parameter Value after Transformation",
                         "Maximum Parameter Value after Transformation")
  )
  flowFrame <- flowCore::flowFrame(data, params)
  
  return(flowFrame)
}