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
#' @param gen_schema Should schema files for output tabls be generated [default: FALSE]
#' @param skipCols Column names (without namespace) to be skipped during comparisons.
#' @param props Array containing properties to be set for specific stepIds.
#' @param forcejoincol Logical array matching num. of out_table to force relation to column (when join was not made through .ci).
#' @param forcejoinrow Logical array matching num. of out_table to force relation to row (when join was not made through .ri).
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
#' forcejoincol = c(TRUE, FALSE))
#' @import stringr
#' @import stringi
#' @import tercen
#' @import jsonlite
build_test_data <- function( res_table, ctx, test_name, 
                             test_folder = NULL, version = '',
                             absTol=NULL, relTol=NULL, r2=NULL,
                             gen_schema=FALSE,
                             skipCols=c(),
                             props=c(),
                             join_col_names=NULL,
                             join_row_names=NULL){
  
  n_out_tables <- length(res_table$rightRelation$joinOperators) + 1
  if(!is.null(join_col_names) && length(join_col_names) != n_out_tables){
    stop(paste0("join_col_names must have the same length as there are output tables(", 
                length(res_table), ")"))
  }
  
  if(!is.null(join_row_names) && length(join_row_names) != n_out_tables){
    stop(paste0("join_row_names must have the same length as there are output tables(", 
                length(res_table), ")"))
  }
  
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  
  if( dir.exists(test_folder)){
    unlink(test_folder,recursive = TRUE)
    
  }
  
  dir.create(test_folder)
  
  in_proj <- create_input_projection(ctx, test_folder)
  
  
  # Check if operator output is a table, list of tables or relation
  if( class(res_table)[[1]] == "JoinOperator"){
    out_tbl_files <- build_test_data_for_schema( in_proj, res_table, ctx, test_name, 
                                                 test_folder = test_folder, 
                                                 gen_schema=gen_schema,
                                                 join_col_names,join_row_names)
    
  }else if( class(res_table)[[1]] == "list"){
    out_tbl_files <- build_test_for_table_list(in_proj, res_table, ctx, test_name, 
                                               test_folder = test_folder, 
                                               gen_schema=gen_schema,
                                               join_col_names,join_row_names)  
    
  }else{
    out_tbl_files <- build_test_for_table(in_proj, res_table, ctx, test_name, 
                                          test_folder = test_folder, 
                                          gen_schema=gen_schema,
                                          join_col_names,join_row_names)  
  }
  
  build_test_input( in_proj, out_tbl_files, ctx, test_name, 
                    test_folder = test_folder, version = version,
                    absTol=absTol, relTol=relTol, r2=r2,
                    skipCols=skipCols,
                    props=props)
  
  
  # Save output tables for running the tests locally when needed
  saved_tbls <- list()
  for( tbl_file in out_tbl_files){
    saved_tbls <- append( saved_tbls, list(read.csv( paste0(test_folder, "/", tbl_file) )) )
  }
  save(saved_tbls,file= file.path(test_folder, paste0(test_name, '.Rda')) )
}

# Checks the presence of documentId columns, and their values
get_documentId_info <- function( ctx, tbl="main" ){
  tnames <- ctx$names
  
  if(tbl == "col"){
    tnames <- ctx$cnames  
  }
  
  if(tbl == "row"){
    tnames <- ctx$rnames  
  }
  
  docIdCheck <- unlist(lapply(tnames, function(x){
    cn <- str_split(x, "\\.")[[1]]
    cn <- cn[length(cn)]
    cn == "documentId"
  }))
  
  has_docId <- any( docIdCheck == TRUE )
  
  if( has_docId ){
    docIdCols <- list()
    docIdVals <- list()
    
    for( dn in names(docIdCheck[which(docIdCheck==TRUE)])  ){
      docIdCols <- append( docIdCols,dn )
      if( tbl== "main"){
        docIdVals <- append( docIdVals, ctx$select(dn, nr=1) )
      }
      
      if( tbl== "col"){
        docIdVals <- append( docIdVals, ctx$cselect(dn, nr=1) )
      }
      
      if( tbl== "row"){
        docIdVals <- append( docIdVals, ctx$rselect(dn, nr=1) )
      }
    }
    
    return(list(TRUE,  docIdCols, docIdVals))
  }else{
    return(list(FALSE, list(), list()))
  }
}


# This function reads in the input projection and return the necessary 
# variables to be added to the testing files
create_input_projection <- function( ctx, test_folder ){
  namespace <- ctx$namespace
  proj_names <- ctx$names
  
  
  docIdInfo <- get_documentId_info( ctx, tbl="main" )
  
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
      dplyr::rename("y_values"=".y") 
  }
  
  if(has_x == TRUE){
    in_tbl <- in_tbl %>%
      dplyr::rename("x_values"=".x") 
  }
  
  has_row_tbl <- FALSE
  has_col_tbl <- FALSE
  
  has_clr_tbl <- FALSE
  has_lbl_tbl <- FALSE
  
  # .all -> Empty row or column projection table
  if( length(names(in_rtbl)) > 1 || names(in_rtbl) != ".all" ){
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
      
      if( !(ctx_colors[[i]] %in% names(in_tbl))){
        in_tbl <- cbind(in_tbl, ctx$select(ctx_colors[[i]]) )
      }
    }
    
    if( any(unlist(lapply(ctx$names, function(x){
      ".colorLevels" == x    })) ) ){
      
      in_tbl <- in_tbl %>% select(-".colorLevels")
    }
  }
  
  # Find documentId instances and downloads the files
  if( docIdInfo[[1]] == TRUE ){
    docIds <- docIdInfo[[3]]
    for( docId in docIds ){
      doc <- ctx$client$fileService$get(docId)
      
      test_fname = paste0(test_folder, "/", docId)
      writeBin(ctx$client$fileService$download(docId), test_fname)
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
                                  test_folder = NULL, 
                                  gen_schema=FALSE,
                                  join_col_names=NULL,
                                  join_row_names=NULL){
  in_tbl <- in_proj$in_tbl
  in_ctbl <- in_proj$in_ctbl
  has_col_tbl <- in_proj$has_col_tbl
  in_rtbl <- in_proj$in_rtbl
  has_row_tbl <- in_proj$has_row_tbl
  
  
  out_tbl_files <- c()
  
  tidx <- 1
  out_tbl_files <- append( out_tbl_files, 
                           unbox(paste0(test_name, '_out_', tidx, '.csv') ))
  
  if(gen_schema==TRUE){
    build_table_schema(as.data.frame(res_table),
                       file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ))
  }
  
  
  tidx <- tidx + 1
  
  if(has_col_tbl == TRUE || 
     (!is.null(join_col_names) && join_col_names[1] == TRUE )){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', tidx, '.csv')) )
    out_ctbl <- in_ctbl
    
    if(gen_schema==TRUE){
      build_table_schema(out_ctbl %>% dplyr::select(-".ci"),
                         file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) )
    }
    
    write.csv(out_ctbl %>% dplyr::select(-".ci"),
              file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
              row.names = FALSE)
    
    tidx <- tidx + 1
  }else{
    if( ".ci" %in% names(res_table) ){
      res_table <- dplyr::select(res_table, -".ci")
    }
  }
  
  if(has_row_tbl == TRUE ||
     (!is.null(join_row_names) && join_row_names[i] == TRUE )){
    out_tbl_files <- append( out_tbl_files, 
                             unbox(paste0(test_name, '_out_', tidx, '.csv')) )
    
    out_rtbl <- in_rtbl
    
    if(gen_schema==TRUE){
      build_table_schema(out_rtbl %>% dplyr::select(-".ri"),
                         file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) )
    }
    
    write.csv(out_rtbl %>% dplyr::select(-".ri"),
              file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
              row.names = FALSE)
    
  }else{
    
    if( ".ri" %in% names(res_table) ){
      res_table <- dplyr::select(res_table, -".ri")
    }
  }
  
  
  write.csv(res_table,
            file.path(test_folder, paste0(test_name, '_out_', 1, '.csv') ) ,
            row.names = FALSE)
  
  return(out_tbl_files)
}


build_test_for_table_list <- function( in_proj, res_table_list, ctx, test_name, 
                                       test_folder = NULL, 
                                       gen_schema=FALSE,
                                       join_col_names=NULL,
                                       join_row_names=NULL){
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
    
    if(gen_schema==TRUE){
      build_table_schema(as.data.frame(res_table),
                         file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ))
    }
    
    
    # Keep index to save main table later after checking for column and row tables
    tidx_prev <- tidx
    
    tidx <- tidx + 1
    
    
    has_ci <- any(unlist(lapply( names(res_table), function(x) { x == '.ci' } ) ))
    has_ri <- any(unlist(lapply( names(res_table), function(x) { x == '.ri' } ) ))
    
    if(has_ci == TRUE || 
       (!is.null(join_col_names) && join_col_names[i] == TRUE ) ){
      out_tbl_files <- append( out_tbl_files, 
                               unbox(paste0(test_name, '_out_', tidx, '.csv')) )
      out_ctbl <- in_ctbl
      
      if( ".ci" %in% names(out_ctbl)){
        out_ctbl <- out_ctbl %>% 
          dplyr::select(-".ci")
      }
      
      if(gen_schema==TRUE){
        build_table_schema(out_ctbl,
                           file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) )
        
      }
      
      write.csv(out_ctbl,
                file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
                row.names = FALSE)
      tidx <- tidx + 1
    }else{
      if( ".ci" %in% names(res_table) ){
        res_table <- dplyr::select(res_table, -".ci")
      }
    }
    
    if(has_ri == TRUE ||
       (!is.null(join_row_names) && join_row_names[i] == TRUE )){
      out_tbl_files <- append( out_tbl_files, 
                               unbox(paste0(test_name, '_out_', tidx, '.csv')) )
      
      out_rtbl <- in_rtbl
      if( ".ri" %in% names(out_rtbl)){
        out_rtbl <- out_rtbl %>% 
          dplyr::select(-".ri")
      }
      
      if(gen_schema==TRUE){
        build_table_schema(out_rtbl,
                           file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) )
      }
      
      write.csv(out_rtbl,
                file.path(test_folder, paste0(test_name, '_out_', tidx, '.csv') ) ,
                row.names = FALSE)
      tidx <- tidx + 1
    }else{
      
      if( ".ri" %in% names(res_table) ){
        res_table <- dplyr::select(res_table, -".ri")
      }
    }
    
    write.csv(res_table,
              file.path(test_folder, paste0(test_name, '_out_', tidx_prev, '.csv') ) ,
              row.names = FALSE)
  }
  
  
  return(out_tbl_files)
}


build_test_data_for_schema <- function( in_proj, res_table, ctx, test_name, 
                                        test_folder = NULL,
                                        gen_schema=FALSE,
                                        join_col_names=NULL,
                                        join_row_names=NULL ){
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
    
    if(class(jo_rr)[1] == "InMemoryRelation"){
      out_tbls <- append(out_tbls,
                         list(env_to_df(jo_rr$inMemoryTable)))
    }else if( is.null(jo_rr$mainRelation)){
      lp <- jo[[k]]$leftPair
      
      if(length(lp$lColumns) > 0){
        lcols <- unlist(lp$lColumns)
        rcols <- unlist(lp$rColumns)
        
        for( c in seq(1, length(lcols))){
          if( (is.null(join_col_names) && lcols[c] == ".ci") || (lcols[c] %in% join_col_names[k])){
            out_tbls <- append( out_tbls, list(dplyr::select(in_ctbl, -".ci")) )
          }
          
          if( (is.null(join_row_names) && lcols[c] == ".ri") || (lcols[c] %in% join_row_names[k])){
            out_tbls <- append( out_tbls, list(dplyr::select(in_rtbl, -".ri")) )
          }
        }
      }else{
        next
      }
      
      
    }else if( !is.null(jo_rr$mainRelation)){
      out_tbls <- append(out_tbls,
                         list(env_to_df(jo_rr$inMemoryTable)))
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
    if(gen_schema==TRUE){
      build_table_schema(as.data.frame(out_tbls[[k]]),
                         file.path(test_folder, paste0(test_name, '_out_', k ,'.csv') ))
    }
    
  }
  
  return(out_tbl_files)
}


build_test_input <- function( in_proj, out_tbl_files, ctx, test_name, 
                              test_folder = NULL, version = '',
                              absTol=NULL, relTol=NULL, r2=NULL,
                              skipCols=c(),
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
                   "generatedOn"=unbox(format(Sys.time(), "%x %X %Y")),
                   "version"=unbox(version))
  
  if( length(propVals) >0 ){
    json_data <- c(json_data, "propertyValues"=list(propVals))
  }  
  
  if( length(skipCols) > 0 ){
    # Get the column names from the saved files to get, if any, the namespace
    # This is easier than parsing again the resulting tables
    skipColsWithNamespace <- get_skip_cols_with_namespace(ctx, skipCols, out_tbl_files, test_folder)
    
    json_data <- c(json_data, "skipColumns"=list(skipColsWithNamespace))
  }
  
  docIdInfoMain <- get_documentId_info( ctx, tbl="main" )
  docIdInfoCol <- get_documentId_info( ctx, tbl="col" )
  docIdInfoRow <- get_documentId_info( ctx, tbl="row" )
  
  if( docIdInfoMain[[1]] == TRUE ||
      docIdInfoCol[[1]] == TRUE ||
      docIdInfoRow[[1]] == TRUE){
    
    fileUris <- append( docIdInfoMain[[3]], docIdInfoCol[[3]])
    fileUris <- append( fileUris, docIdInfoRow[[3]])
    
    fileUris <- unname(unlist(unique( fileUris)))
    
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
#' Based on a test name, compares a computed table against a previously saved one.
#' 
#' @param res_table operator output table with namespace
#' @param ctx Tercen context
#' @param test_name Name identifying the test.
#' @param absTol Absolute tolerance parameter.
#' @param relTol Relative tolerance parameter.
#' @param r2 Squared correlation parameter.
#' @param skipCols Column names to be skipped during testing
#' @param join_col_names Logical array matching num. of out_table to force relation to column (when join was not made through .ci).
#' @param join_row_names Logical array matching num. of out_table to force relation to row (when join was not made through .ri).
#' @keywords test
#' @export
#' @examples
#' run_local_test(tbl, ctx, paste0(step_name, "_absTol"), absTol = 0.001)
#' 
#' @import stringr
#' @import stringi
#' @import tercen
#' @import jsonlite
run_local_test <- function( res_table, ctx, test_name, 
                            test_folder = NULL, 
                            absTol=NULL, relTol=NULL, r2=NULL,
                            skipCols=c(),
                            join_col_names=NULL,
                            join_row_names=NULL){
  
  if(!is.null(join_col_names) && length(join_col_names) != length(res_table)){
    stop(paste0("join_col_names must have the same length as there are output tables(", 
                length(res_table), ")"))
  }
  
  if(!is.null(join_row_names) && length(join_row_names) != length(res_table)){
    stop(paste0("join_row_names must have the same length as there are output tables(", 
                length(res_table), ")"))
  }
  
  if( is.null(test_folder)){
    test_folder <- paste0( getwd(), '/tests' )
  }
  
  tmp_folder <- tempdir()
  
  
  print(paste0("Running local test against ", test_name))
  
  in_proj <- create_input_projection(ctx, test_folder)
  
  
  # Check if operator output is a table, list of tables or relation
  # Then build the tables as they are in Tercen
  if( class(res_table)[[1]] == "JoinOperator"){
    out_tbl_files <- build_test_data_for_schema( in_proj, res_table, ctx, test_name, 
                                                 test_folder = tmp_folder, 
                                                 join_col_names = join_col_names,
                                                 join_row_names = join_row_names)
    
  }else if( class(res_table)[[1]] == "list"){
    out_tbl_files <- build_test_for_table_list(in_proj, res_table, ctx, test_name, 
                                               test_folder = tmp_folder, 
                                               join_col_names = join_col_names,
                                               join_row_names = join_row_names)  
    
  }else{
    out_tbl_files <- build_test_for_table(in_proj, res_table, ctx, test_name, 
                                          test_folder = tmp_folder, 
                                          join_col_names = join_col_names,
                                          join_row_names = join_row_names)  
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
  
  skipColsWithNamespace <- get_skip_cols_with_namespace(ctx, skipCols, out_tbl_files, test_folder)
  for( i in seq(1, length(out_tbl_files))){
    out_tbl <- res_table_list[[i]]
    svd_tbl <- saved_tbls[[i]]
    
    colnames <- names(out_tbl)
    
    assert( nrow(out_tbl),
            nrow(svd_tbl), metric="eq",
            msg_fail=paste0("Number of rows for table ", i, " do not match."))
    
    for( j in seq(1, length(colnames))){
      if( colnames[[j]] %in% skipColsWithNamespace ){
        next
      }
      
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
#' @import tercen
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


build_table_schema <- function(  tbl, tbl_file ){
  colnames <- names(tbl)
  
  
  table_schema <- c()
  
  for( i in seq(1, length(colnames))) {
    entry <- list("kind"=unbox("ColumnSchema"),
                  "name"=unbox(colnames[[i]]),
                  "type"=unbox(get_column_type(class(tbl[[colnames[[i]]]]))) )
    
    table_schema <- append( table_schema, list(entry) )
    
  }
  
  json_data <- list("kind"=unbox("TableSchema"),
                    "columns"=table_schema)
  
  
  
  json_data <- toJSON(json_data, pretty=TRUE, auto_unbox = FALSE,
                      digits=16)
  
  write(json_data, str_replace(tbl_file, ".csv", ".csv.schema") )
}

get_column_type <- function(coltype){
  schematype <- NULL
  schematype <- switch(coltype,
                       "character"="string",
                       "numeric"="double",
                       "integer"="int32")
  
  return(schematype)
}


get_skip_cols_with_namespace <- function(ctx, skipCols, tbl_files, test_folder){
  col_names <- c()
  for( tbl_file in tbl_files ){
    tbl<-read.csv( file.path(test_folder, tbl_file) )
    col_names <- append(col_names, unlist(names(tbl))  )
  }
  
  # Remove namespace, but keep input column and row projection names
  in_names <- unname(unlist(c(names(ctx$rnames), names(ctx$cnames))))
  
  # Convert white space in names to . (which is how tercen control handles this situation in csv files)
  in_names <- unlist(lapply(in_names, function(x){
    str_replace_all(x, " ", "\\.")
  }))
  
  col_names_nn <- unlist(lapply( col_names, function(x){
    if( (x %in% in_names) == TRUE || 
        stri_startswith(x,fixed = ".") || 
        grepl(".", x, fixed=TRUE) == FALSE){
      return(x)
    }else{
      return(str_split_fixed(x, "\\.", 2)[[2]])
    }
  }))
  
  
  skipColsWithNamespace<-unlist(lapply(skipCols, function(x){
    col <- grepl( x, col_names_nn, fixed= TRUE)
    
    unique(col_names[ which(col) ])
  }))
  
  return(skipColsWithNamespace)
}
