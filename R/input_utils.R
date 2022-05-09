#' Check for the presence of inputs.
#'
#' This function checks whether the specified list of inputs has been populated.
#' 
#' @param ctx Tercen context.
#' @param what Which factor should be checked? Any of x, .x, y, .y, row, rows,
#' column, columns, colors, labels
#' @keywords utils
#' @export
#' @examples
#' ctx <- tercen::tercenCtx()
#' check_ctx(ctx, c("colors"))
#' @import tercen
check_ctx <- function(ctx, what, min_n = 1) {
  
  if(!"OperatorContextDev" %in% class(ctx)) stop("ctx object is not a Tercen OperatorContextDev.")
  
  check <- unlist(sapply(what, function(i, min_n) {
    switch(
      i,
      x = length(ctx$xAxis) >= min_n,
      .x = length(ctx$xAxis) >= min_n,
      y = length(ctx$yAxis) >= min_n,
      .y = length(ctx$yAxis) >= min_n,
      column = (length(ctx$cnames) >= min_n & ctx$cnames[[1]]!= ""),
      columns = (length(ctx$cnames) >= min_n & ctx$cnames[[1]] != ""),
      row = (length(ctx$rnames) >= min_n & ctx$rnames[[1]] != ""),
      rows = (length(ctx$rnames) >= min_n & ctx$rnames[[1]] != ""),
      color = length(ctx$colors) >= min_n,
      colors = length(ctx$colors) >= min_n,
      label = length(ctx$labels) >= min_n,
      labels = length(ctx$labels) >= min_n
    )
  }, min_n = min_n))

  if(any(is.null(check))) stop("Invalid argument.")
  if(any(!check)) stop(paste("Missing factors:", what[!check]))

  return(NULL)
  
}
