


#' Create unit tests based on a workflow step for an operator
#'
#' Based on the output table and input projection from an operator, builds
#' the necessary files for a test case.
#' 
#' @param out_table operator output table with namespace
#' @param ctx Tercen context.
#' @param test_name Name identifying the test.
#' @param test_folder Path where test files are stored.
#' @param version Corresponding operator version when test was created.
#' @param absTol Absolute tolerance parameter.
#' @param relTol Relative tolerance parameter.
#' @param r2 Squared correlation parameter.
#' @param docIdMapping mapping between document Id and filename used for testing.
#' @param props Array containing properties to be set for specific stepIds.
#' @keywords test
#' @export
#' @examples
#' build_test_data( tbl, ctx, paste0(step_name, "_absTol"),
#' version = '0.0.1',
#' absTol = 0.001)
#' 
#' build_test_data( tbl, ctx, paste0(step_name, "_absTol"),
#' version = '0.0.1',
#' absTol = 0.001,
#' docIdMapping = c("32d4b2986b98f3ebd5b5baa990000148"="hospitals.csv.zip"))
#' @import stringr
#' @importFrom jsonlite toJSON 
build_test_data <- function( out_table, ctx, test_name, 
                                   test_folder = NULL, version = '',
                                   absTol=NULL, relTol=NULL, r2=NULL,
                                   docIdMapping=c(),
                                   props=c()){
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  
  
  unbox <- tercen:::unbox
  
  testTbl <- out_table
  save(testTbl,file= file.path(test_folder, paste0(test_name, '.Rda')) )
  
  namespace <- ctx$namespace
  proj_names <- ctx$names
  
  # .y, .ci and .ri are always present even if there is nothing set for them
  select_names <- c(".y", ".ci",".ri")
  yAxis <- 'y_values'
  xAxis <- ''
  
  has_y <- TRUE  
  has_x <- FALSE  
  
  # Check whether x axis is set
  if( ".x" %in% proj_names){
    select_names <- append(select_names, ".x")  
    xAxis <- "x_values"
    has_x <- TRUE
  }
  
  labels <- unname(unlist(ctx$labels))
  if(!is.null(labels) ){
    for(i in seq(1, length(labels))){
      select_names <- append(select_names, labels[[i]])
    }
  }
  
  ctx_colors <- unname(unlist(ctx$colors))
  if(length(ctx_colors) > 0 && ctx_colors != ""){
    if( any(unlist(lapply(ctx$names, function(x){
      ".colorLevels" == x
    })) ) ){
      select_names <- append(select_names, ".colorLevels")  
    }
  }
  
  in_tbl <- ctx$select(select_names) 
  in_rtbl <- ctx$rselect()
  in_ctbl <- ctx$cselect()
  
 
  
  if(has_y == TRUE) {
    in_tbl <- in_tbl %>% rename("y_values"=".y") 
  } else {
    in_tbl <- in_tbl %>% select(-".y")
  }
  
  if(has_x == TRUE) {
    in_tbl <- in_tbl %>% rename("x_values"=".x") 
  }
  
  has_row_tbl <- FALSE
  has_col_tbl <- FALSE
  
  has_clr_tbl <- FALSE
  has_lbl_tbl <- FALSE
  
  # .all -> Empty row or column projection table
  if( length(names(in_rtbl)) > 0 || names(in_rtbl) != ".all" ){
    in_rtbl <- in_rtbl %>% 
      mutate( .ri=seq(0,nrow(.)-1) )
    
    in_tbl <- dplyr::full_join( in_tbl, in_rtbl, by=".ri" ) %>%
      select(-".ri") 
    
    has_row_tbl <- TRUE
  }else{
    in_tbl <- select(in_tbl, -".ri")
    tryCatch({
      out_table <- out_table %>% select(-".ri")
    }, error=function(cond){
      # Ignore, no .ri column in result
    })
  }
  
  if( length(names(in_ctbl)) > 1 || names(in_ctbl) != ".all" ){
    in_ctbl <- in_ctbl %>% mutate( .ci=seq(0,nrow(.)-1) )
    in_tbl <- dplyr::full_join( in_tbl, in_ctbl, by=".ci" ) %>%
      select(-".ci")
    
    has_col_tbl <- TRUE
  }else{
    in_tbl <- select(in_tbl, -".ci")
    tryCatch({
      out_table <- out_table %>% select(-".ci")
    }, error=function(cond){
      # Ignore, no .ci column in result
    })
  }
  
  if(length(ctx_colors) > 0 && ctx_colors != ""){
    for(i in seq(1,length(ctx_colors))){
      in_tbl <- cbind(in_tbl, ctx$select(ctx_colors[[i]]) )
    }
    
    if( any(unlist(lapply(ctx$names, function(x){
      ".colorLevels" == x
    })) ) ){
      in_tbl <- in_tbl %>% select(-".colorLevels")
    }
  }
  
  # Find documentId instances and replace them
  if( length(docIdMapping) > 0 ){
    for( i in seq(1, length(docIdMapping))  ){
      in_tbl <- in_tbl %>%
        mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
    }
  }
  
  # If no version is supplied, try to get the latest version in the repo,
  # and failing that, the latest commit
  if(version == ''){
    version <- system("git describe --tags", intern = TRUE)
  }
  
  if(length(version) == 0){
    version <- system("git rev-parse --short HEAD", intern = TRUE)
  }
  
  
  # @TODO
  # Add support for lists of tables & associated row/cols
  out_tbl_files <- c()
  
  out_tbl_files <- append( out_tbl_files, 
                           unbox(paste0(test_name, '_out_1.csv') ))
  write.csv(out_table,
            file.path(test_folder, paste0(test_name, '_out_1.csv') ) ,
            row.names = FALSE)
  
  
  if(has_col_tbl == TRUE){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_2.csv')) )
    out_ctbl <- in_ctbl
    if( length(docIdMapping) > 0 ){
      # Find documentId instances and replace them
      for( i in seq(1, length(docIdMapping))  ){
        out_ctbl <- out_ctbl %>%
          mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
      }
    }
    
    
    write.csv(out_ctbl %>% select(-".ci"),
              file.path(test_folder, paste0(test_name, '_out_2.csv') ) ,
              row.names = FALSE)
  }
  
  if(has_row_tbl == TRUE){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_3.csv')) )
    
    write.csv(in_rtbl %>% select(-".ri"),
              file.path(test_folder, paste0(test_name, '_out_3.csv') ) ,
              row.names = FALSE)
  }
  
  unbox_cnames <- lapply( c(unname(unlist(ctx$cnames))), function(x){
    unbox(x)
  })
  
  unbox_rnames <- lapply( c(unname(unlist(ctx$rnames))), function(x){
    unbox(x)
  })
  
  
  unbox_labels <- lapply( labels, function(x){
    unbox(x)
  })
  
  unbox_colors <- lapply( ctx_colors, function(x){
    unbox(x)
  })
  
  
  propVals = c()
  if( length(props) > 0){
    propVals <- data.frame( kind=c("PropertyValue"),
                            name=names(props),
                            value=unlist(unname(props)))
  }
  
  
  in_tbl_file <- file.path(test_folder, paste0(test_name, "_in", '.csv'))
  write.csv(in_tbl, in_tbl_file, row.names = FALSE)
  
  json_data = list("kind"=unbox("OperatorUnitTest"),
                   "name"=unbox(test_name),
                   "namespace"=unbox(namespace),
                   "inputDataUri"=unbox(basename(in_tbl_file)),
                   "outputDataUri"=out_tbl_files,
                   "columns"=if(length(ctx$cnames) == 1 && unname(unlist(ctx$cnames)) == "") list() else unbox_cnames,
                   "rows"=if(length(ctx$rnames) == 1 && unname(unlist(ctx$rnames)) == "") list() else c(unname(unlist(ctx$rnames))),
                   "colors"=if(length(ctx_colors) == 1 && ctx_colors == "") list() else unbox_colors,
                   "labels"=if(is.null(labels) ) list() else unbox_labels,
                   "yAxis"=unbox(yAxis),
                   "xAxis"=unbox(xAxis),
                   "propertyValues"=propVals,
                   "generatedOn"=unbox(format(Sys.time(), "%x %X %Y")),
                   "version"=unbox(version))
  
  if( length(docIdMapping) > 0 ){
    fileUris <- unname((docIdMapping))
    json_data <- c(json_data, "inputFileUris"=list(fileUris))
  }
  
  if( !is.null( absTol )){
    json_data <- c(json_data, list("absTol"=unbox(absTol)) )
  }
  
  if( !is.null( relTol )){
    json_data <- c(json_data, list("relTol"=unbox(relTol)))
  }
  if( !is.null( r2 )){
    json_data <- c(json_data, list("r2"=unbox(r2)))
    json_data <- c(json_data, list("equalityMethod"=unbox("R2")))
  }
  
  
  json_data <- toJSON(json_data, pretty=TRUE, auto_unbox = FALSE,
                      digits=16)
  
  json_file <- file.path(test_folder, paste0(test_name, '.json'))
  
  write(json_data, json_file) 
  
  
  
}




#' Check an operator result against a previously saved run
#'
#' Base don a test name, compares a computed table agianst a previously saved one.
#' 
#' @param out_table operator output table with namespace
#' @param test_name Name identifying the test.
#' @param metric Which comparison to use.
#' @param absTol Absolute tolerance parameter.
#' @param relTol Relative tolerance parameter.
#' @param r2 Squared correlation parameter.
#' @keywords test
#' @export
#' @examples
#' check_test_local(tbl, paste0(step_name, "_absTol"),
#' absTol = 0.001)
#' 
#' @import stringr
check_test_local <- function( out_table, test_name, test_folder = NULL, 
                              metric="eq",
                              absTol=0, relTol=0, r2=0 ){
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  # Loads a variable called testTbl
  load(file.path(test_folder, paste0(test_name, '.Rda')) )
  
  
  compare_results(out_table, testTbl, metric = metric,
                  absTol=absTol, relTol=relTol, r2=r2)
  
  
  # If no error occurs ...
  print( paste0("Test ", test_name, " completed successfuly.\n")  )
}

# metric: c("eq", "tol", "r2")
compare_results <- function( newTbl, testTbl, metric="eq",
                             absTol=0, relTol=0, r2=0){
  newCols <- names(newTbl)
  testCols <- names(testTbl)
  
  if( length(newCols) != length(testCols) ){
    err_msg <- paste0("ERROR: Mismatch between number of columns\n\n",
                      "The new result has ", length(newCols), "columns [",
                      paste(newCols, collapse = ", "), "]\n\n",
                      "The saved result has ", length(testCols), "columns [",
                      paste(testCols, collapse = ", "), "]\n\n")
    
    
    stop(err_msg)
  }
  
  if( nrow(newTbl) != nrow(testTbl) ){
    err_msg <- paste0("ERROR: Mismatch between number of rows.\n\n",
                      "The new result has ", nrow(newTbl), "rows.\n\n",
                      "The saved result has ", nrow(testTbl), "rows.\n\n")
    stop(err_msg)
  }
  
  if(metric == "eq"){
    comparetbl_eq_tol(newTbl, testTbl, newCols, relTol=NULL, absTol=0) 
  }
  
  if(metric == "tol"){
    comparetbl_eq_tol(newTbl, testTbl, newCols, relTol=relTol, absTol=absTol) 
  }
  
  if(metric == "r2"){
    comparetbl_r2(newTbl, testTbl, newCols, test_r2=r2)
  }
}



comparetbl_r2 <- function(newTbl, testTbl, newCols, test_r2=0){
  dt <- sapply(newTbl, class)
  for(i in seq(1, length(newCols))){
    if( !startsWith(newCols[i], ".") && (
      dt == "integer" || dt == "numeric")){
      
      r2 <- cor( newTbl[,i], testTbl[,i]  )[[1]] ** 2
      
      if( all(newTbl[,i]==0) && all(testTbl[,i]==0) ){
        # If both contain only 0's, cor will return NA
        # For the purposes of this test, this will be considered 1
        r2 <- 1
      }
      
      if( r2 <  test_r2 ){
        err_msg <- paste0('Correlation test failed for column ',
                          newCols[i], " --- ",
                          "R2 = ", r2)
        
        stop(err_msg)
      }
      
    }
  }
}


comparetbl_eq_tol <- function(newTbl, testTbl, newCols, relTol=NULL, absTol=0){
  # For the sake of clarity, run the test row-wise
  # Might update this if performance becomes a problem in large tables
  
  dt <- sapply(newTbl, class)
  
  for(i in seq(1, nrow(newTbl))){
    newRow <- newTbl[i,]
    testRow <- testTbl[i,]
    
    for( j in seq(1, length(newCols)) ){
      dtCol <- dt[j]
      if( dtCol == "integer" || dtCol == "numeric" ){
        deltaAbs <- abs(newRow[j] - testRow[j]  )[[1]]
        deltaRel <- abs(1- (newRow[j][[1]]/testRow[j][[1]]  ))
        
        deltaRelInTol <- is.null(relTol) || (
          (newRow[j] == 0 && testRow[j] == 0) ||
            (deltaRel <= relTol)
        )
        
        deltaAbsInTol <- deltaAbs <= absTol
        
        isEq <- deltaAbsInTol && deltaRelInTol
      }else{
        isEq <- newRow[j] == testRow[j]
      }
      
      if( !isEq ){
        err_msg <- paste0('Comparison test failed at row ',
                          i, " column ", newCols[j], "\n\n.",
                          "Values: ", newRow[j], "  --  ",
                          testRow[j])
        
        stop(err_msg)
      }
    }
  }
}



