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
build_test_data <- function( res_table, ctx, test_name, 
                                   test_folder = NULL, version = '',
                                   absTol=NULL, relTol=NULL, r2=NULL,
                                   docIdMapping=c(),
                                   props=c()){
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  
  in_proj <- create_input_projection(ctx, docIdMapping)
  
  
  # Check if operator output is a table, list of tables or relation
  if( class(res_table)[[1]] == "JoinOperator"){
    out_tbl_files <- build_test_data_for_schema( in_proj, res_table, ctx, test_name, 
                                                 test_folder = test_folder, 
                                                 docIdMapping=docIdMapping)
    
  }else if( class(res_table)[[1]] == "list"){
    out_tbl_files <- build_test_for_table_list(in_proj, res_table, ctx, test_name, 
                                               test_folder = test_folder, 
                                               docIdMapping=docIdMapping)  
    
  }else{
    out_tbl_files <- build_test_for_table(in_proj, res_table, ctx, test_name, 
                                          test_folder = test_folder, 
                                          docIdMapping=docIdMapping)  
  }
  
  build_test_input( in_proj, out_tbl_files, ctx, test_name, 
                    test_folder = test_folder, version = version,
                    absTol=absTol, relTol=relTol, r2=r2,
                    docIdMapping=docIdMapping,
                    props=props)
  

  # Save output tables for running the tests locally when needed
  saved_tbls <- list()
  for( tbl_file in out_tbl_files){
    saved_tbls <- append( saved_tbls, list(read.csv( paste0(test_folder, "/", tbl_file) )) )
  }
  save(saved_tbls,file= file.path(test_folder, paste0(test_name, '.Rda')) )
}


# This function reads in the input projection and return the necessary 
# variables to be added to the testing files
create_input_projection <- function( ctx, docIdMapping ){
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
  
  
  if(has_y == TRUE){
    in_tbl <- in_tbl %>%
      rename("y_values"=".y") 
  }
  
  if(has_x == TRUE){
    in_tbl <- in_tbl %>%
      rename("x_values"=".x") 
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
  }
  
  if( length(names(in_ctbl)) > 1 || names(in_ctbl) != ".all" ){
    in_ctbl <- in_ctbl %>% mutate( .ci=seq(0,nrow(.)-1) )
    in_tbl <- dplyr::full_join( in_tbl, in_ctbl, by=".ci" ) %>%
      select(-".ci")
    
    has_col_tbl <- TRUE
  }else{
    in_tbl <- select(in_tbl, -".ci")
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
  
  
  return(list(in_tbl=in_tbl, 
              has_col_tbl=has_col_tbl, 
              in_ctbl=in_ctbl, 
              has_row_tbl=has_row_tbl, 
              in_rtbl=in_rtbl, 
              ctx_colors=ctx_colors, 
              labels=labels, 
              yAxis=yAxis, 
              xAxis=xAxis))
}

build_test_for_table <- function( in_proj, res_table, ctx, test_name, 
                                  test_folder = NULL, version = '',
                                  absTol=NULL, relTol=NULL, r2=NULL,
                                  docIdMapping=c(),
                                  props=c()
){
  in_tbl <- in_proj$in_tbl
  in_ctbl <- in_proj$in_ctbl
  has_col_tbl <- in_proj$has_col_tbl
  in_rtbl <- in_proj$in_rtbl
  has_row_tbl <- in_proj$has_row_tbl
  
  
  out_tbl_files <- c()
  
  tidx <- 1
  out_tbl_files <- append( out_tbl_files, 
                           unbox(paste0(test_name, '_out_', tidx, '.csv') ))
  write.csv(res_table,
            file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
            row.names = FALSE)
  
  tidx <- tidx + 1
  
  
  if(has_col_tbl == TRUE){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', tidx, '.csv')) )
    out_ctbl <- in_ctbl
    if( length(docIdMapping) > 0 ){
      # Find documentId instances and replace them
      for( i in seq(1, length(docIdMapping))  ){
        out_ctbl <- out_ctbl %>%
          mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
      }
    }
    
    
    write.csv(out_ctbl %>% select(-".ci"),
              file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
              row.names = FALSE)
    
    tidx <- tidx + 1
  }
  
  if(has_row_tbl == TRUE){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', tidx, '.csv')) )
    
    out_rtbl <- in_rtbl
    if( length(docIdMapping) > 0 ){
      # Find documentId instances and replace them
      for( i in seq(1, length(docIdMapping))  ){
        out_rtbl <- out_rtbl %>%
          mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
      }
    }
    
    
    write.csv(out_rtbl %>% select(-".ri"),
              file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
              row.names = FALSE)
    
  }
  
  return(out_tbl_files)
}


build_test_for_table_list <- function( in_proj, res_table_list, ctx, test_name, 
                                       test_folder = NULL, version = '',
                                       absTol=NULL, relTol=NULL, r2=NULL,
                                       docIdMapping=c(),
                                       props=c()
){
  in_tbl <- in_proj$in_tbl
  in_ctbl <- in_proj$in_ctbl
  has_col_tbl <- in_proj$has_col_tbl
  in_rtbl <- in_proj$in_rtbl
  has_row_tbl <- in_proj$has_row_tbl
  
  
  out_tbl_files <- c()
  
  
  tidx <- 1
  for( i in seq(1, length(res_table_list))){
    res_table <- res_table_list[[i]]
    
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', tidx, '.csv') ))
    write.csv(res_table,
              file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
              row.names = FALSE)
    tidx <- tidx + 1
    
    has_ci <- any(unlist(lapply( names(res_table), function(x) { x == '.ci' } ) ))
    has_ri <- any(unlist(lapply( names(res_table), function(x) { x == '.ri' } ) ))
    
    if(has_ci == TRUE){
      out_tbl_files <- append( out_tbl_files, 
                               unbox(paste0(test_name, '_out_', tidx, '.csv')) )
      out_ctbl <- in_ctbl
      if( length(docIdMapping) > 0 ){
        # Find documentId instances and replace them
        for( i in seq(1, length(docIdMapping))  ){
          out_ctbl <- out_ctbl %>%
            mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
        }
      }
      write.csv(out_ctbl %>% select(-".ci"),
                file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
                row.names = FALSE)
      tidx <- tidx + 1
    }
    
    if(has_ri == TRUE){
      out_tbl_files <- append( out_tbl_files, 
                               unbox(paste0(test_name, '_out_', tidx, '.csv')) )
      
      out_rtbl <- in_rtbl
      if( length(docIdMapping) > 0 ){
        # Find documentId instances and replace them
        for( i in seq(1, length(docIdMapping))  ){
          out_rtbl <- out_rtbl %>%
            mutate_all( ~str_replace(., unlist(names(docIdMapping[i])), unname(docIdMapping[i])) )
        }
      }
      
      write.csv(out_rtbl %>% select(-".ri"),
                file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
                row.names = FALSE)
      tidx <- tidx + 1
    }
  }
  
  
  return(out_tbl_files)
}


build_test_data_for_schema <- function( in_proj, res_table, ctx, test_name, 
                                        test_folder = NULL,
                                        docIdMapping=c() ){
  in_ctbl <- in_proj$in_ctbl
  has_col_tbl <- in_proj$has_col_tbl
  in_rtbl <- in_proj$in_rtbl
  has_row_tbl <- in_proj$has_row_tbl
  
  # Main output table
  rr <- res_table$rightRelation
  jo <- rr$joinOperators
  
  out_tbls <- c()
  
  # Convert from Environment(inMemoryTable) to a data.frame
  out_tbls <- append( out_tbls, list(env_to_df(rr$mainRelation$inMemoryTable) ))
  
  
  w <- 0
  for( k in seq(1, length(jo))){
    jo_rr <- jo[[k]]$rightRelation
    
    if( is.null(jo_rr$mainRelation)){
      
      out_tbls <- append( out_tbls, 
                          list(env_to_df( jo_rr$inMemoryTable) ) 
      )
    }else{
      out_tbls <- append( out_tbls, 
                          list(env_to_df( jo_rr$mainRelation$inMemoryTable ) ) 
      )
      
      jo_rel <- jo_rr$joinOperators
      
      for( r in seq(1,length(jo_rel))){
        if( r == 1){
          
          sch <- as.data.frame(ctx$selectSchema( jo_rel[[r]]$rightRelation )   )
        }else{
          sch <- cbind(sch, 
                       as.data.frame(ctx$selectSchema( jo_rel[[r]]$rightRelation )))
        }
      }
      
      out_tbls <- append( out_tbls, 
                          list(sch) )
    }
  }
  
  
  out_tbl_files <- c()
  
  for( k in seq(1, length(out_tbls))){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', k ,'.csv') ))
    write.csv(as.data.frame(out_tbls[[k]]),
              file.path(test_folder, paste0(test_name, '_out_', k ,'.csv') ) ,
              row.names = FALSE)
  }
  
  return(out_tbl_files)
}


build_test_input <- function( in_proj, out_tbl_files, ctx, test_name, 
                              test_folder = NULL, version = '',
                              absTol=NULL, relTol=NULL, r2=NULL,
                              docIdMapping=c(),
                              props=c()){
  
  in_tbl <- in_proj$in_tbl
  ctx_colors <- in_proj$ctx_colors
  labels <- in_proj$labels
  yAxis <- in_proj$yAxis
  xAxis <- in_proj$xAxis
  
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
                   "namespace"=unbox(ctx$namespace),
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
#' @param res_table operator output table with namespace
#' @param ctx Tercen context
#' @param test_name Name identifying the test.
#' @param absTol Absolute tolerance parameter.
#' @param relTol Relative tolerance parameter.
#' @param r2 Squared correlation parameter.
#' @keywords test
#' @export
#' @examples
#' check_test_local(tbl, ctx, paste0(step_name, "_absTol"), absTol = 0.001)
#' 
#' @import stringr
run_local_test <- function( res_table, ctx, test_name, 
                            test_folder = NULL, 
                            absTol=NULL, relTol=NULL, r2=NULL,
                            docIdMapping=c()){
  
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  
  tmp_folder <- tempdir()
  
  
  print(paste0("Running local test against ", test_name))
  
  in_proj <- create_input_projection(ctx, docIdMapping)
  
  
  # Check if operator output is a table, list of tables or relation
  # Then build the tables as they are in Tercen
  if( class(res_table)[[1]] == "JoinOperator"){
    out_tbl_files <- build_test_data_for_schema( in_proj, res_table, ctx, test_name, 
                                                 test_folder = tmp_folder, 
                                                 docIdMapping=docIdMapping)
    
  }else if( class(res_table)[[1]] == "list"){
    out_tbl_files <- build_test_for_table_list(in_proj, res_table, ctx, test_name, 
                                               test_folder = tmp_folder, 
                                               docIdMapping=docIdMapping)  
    
  }else{
    out_tbl_files <- build_test_for_table(in_proj, res_table, ctx, test_name, 
                                          test_folder = tmp_folder, 
                                          docIdMapping=docIdMapping)  
  }
  
  res_table_list <- list()
  for( i in seq(1, length(out_tbl_files))){
    res_table_list <- append(res_table_list, 
                             list(read.csv(paste0(tmp_folder, "/", out_tbl_files[i]  ))))
    
    unlink(paste0(tmp_folder, "/", out_tbl_files[i]  ))
  }
  
  # loads saved_tbls
  load( file.path(test_folder, paste0(test_name, '.Rda')) ) 
  
  assert( length(res_table_list),
          length(saved_tbls), metric="eq",
          msg_fail="Number of resulting tables differ.")
  
  
  for( i in seq(1, length(out_tbl_files))){
    out_tbl <- res_table_list[[i]]
    svd_tbl <- saved_tbls[[i]]
    
    colnames <- names(out_tbl)
    
    assert( nrow(out_tbl),
            nrow(svd_tbl), metric="eq",
            msg_fail=paste0("Number of rows for table ", i, " do not match."))
    
    for( j in colnames){
      x <- out_tbl[[j]]
      y <- svd_tbl[[j]]
      assert( class(x[[1]]),
              class(y[[1]]), metric="eq",
              msg_fail=paste0("Datatypes for table ", i, ", column ", colnames[[j]], " do not match."))
      
      if( class(x[[1]]) == 'character' || class(x[[1]]) == 'factor'){
        assert( x,
                y, metric="eq",
                msg_fail=paste0("Comparison failed for table ", i, ", column ", colnames[[j]], "."))
      }else{
        if( !is.null(absTol)){
          
          assert( x,
                  y, metric="abstol", absTol=absTol,
                  msg_fail=paste0("Comparison failed for table ", i, ", column ", colnames[[j]], "."))
        }
        
        if( !is.null(relTol)){
          assert( x,
                  y, metric="reltol", relTol=relTol,
                  msg_fail=paste0("Comparison failed for table ", i, ", column ", colnames[[j]], "."))
        }
        
        if( !is.null(r2)){
          assert( x,
                  y, metric="r2", r2=r2,
                  msg_fail=paste0("Comparison failed for table ", i, ", column ", colnames[[j]], "."))
        }
      }
    }
  }
  print("Test completed succesfully")
}




#------------------------------------------
#
# Helper functions
#
#------------------------------------------
assert <- function( x, y, 
                    msg_fail = '',
                    metric='eq',
                    absTol=Inf, 
                    relTol=1, 
                    r2=0){
  if( metric == 'eq' ){
    if( !all( x == y ) ){
      stop( paste0("Comparison failed: ", msg_fail) )
    }
  }
  
  
  if( metric == 'r2' ){
    pc <- cor(x, y, method = "pearson")**2
    if( pc < r2){
      stop( paste0("Correlation < accepted", msg_fail) )
    }
  }
  
  if( metric == 'abstol' ){
    d <- abs(x - y)
    if( any(d > absTol)){
      stop( paste0("Difference > accepted [abs]", msg_fail) )
    }
  }
  
  if( metric == 'abstol' ){
    d <- x/y
    d <- abs(1-d[which(!is.nan(d))])
    if( any(d > relTol)){
      stop( paste0("Difference > accepted [rel]", msg_fail) )
    }
  }
  
  
}


#' Utility function to extract the step name given an workflow and step IDs
#'
#' 
#' @param ctx Tercen context.
#' @param wkfId Workflow Identifier
#' @param stepId Step Identifier
#' @keywords test
#' @export
get_step_name <- function(ctx, wkfId, stepId){
  wkf <- ctx$client$workflowService$get(wkfId)
  steps <- wkf$steps
  
  current_step <- lapply(steps, function(x){
    if( x$id == stepId ){
      return(x$name)
    }else{
      return(NULL)
    }
  } )
  
  step_name <- unlist(current_step[vapply(current_step, Negate(is.null), NA)])
  return(step_name)
}

env_to_df <- function(env){
  tson <- env$toTson()
  n <- lapply( tson$columns, function(x){  x$name    } )
  dff<-data.frame((lapply( tson$columns, function(x){    x$values    } )))
  names(dff)<- n
  
  return(dff)
}