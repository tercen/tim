#' Downloads and unzip files with cache support.
#'
#' Searches the /tmp/documentId folder for flags indicating that the data 
#' associated with the documentId has previously been loaded.
#' If it has not, downloads the file and, if it is a ZIP archive, unzips it.
#' The cache is cleared by the GarbageCollector
#' 
#'  
#' 
#' @param ctx Tercen context.
#' @param documentId Identifier for file to be loaded.
#' @param force_load Downloads and unzips data regardless of cache
#' @keywords file
#' @export
#' @examples
#' load_data(ctx, docId)
#' @import tools
load_data <- function(ctx, documentId, force_load=FALSE){
  tempFolder <- file.path('/tmp', documentId, documentId)
  
  if( !dir.exists(tempFolder) ){
    dir.create(tempFolder, recursive = TRUE)
  }
  
  doc = ctx$client$fileService$get(documentId)
  
  filename <- file.path(tempFolder, doc$name  )
  
  if( !file.exists(file.path(tempFolder, '.downloaded')) || 
      force_load == TRUE  ){
    writeBin(ctx$client$fileService$download(documentId), filename)
    
    file.create(file.path(tempFolder, '.downloaded')  )
  }
  
  isZip <- length(grep(".zip", doc$name)) > 0
  
  if(isZip  && 
     (!file.exists(file.path(tempFolder, '.extracted')) || force_laod == TRUE) ) {
    unzip(filename, exdir = tempFolder,
          overwrite = TRUE)
    
    unlink( filename ) # ZIP file will no longer be needed
    
    file.create(file.path(tempFolder, '.extracted')  )
  }
  
  # If desired file is ZIP archive, get a list, otherwise return the filename
  if( isZip  ){
    return(list.files(
      file.path(list.files(tempFolder, full.names = TRUE)), 
      full.names = TRUE, all.files = FALSE,
      recursive = TRUE))
  }else{
    return(filename)
  }
}