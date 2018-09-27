library(reticulate)
use_condaenv("reticulate-r-base")

source_python("daskr.py")

submit <- function(f, args) {
  # f_str <- rlang::as_closure(f)
  f_str <- serialize(f, NULL)
  py_lambda(f_str, args)
}


inc <- function(x){
  result <- x + 1
  return(result)
}

future <- submit(inc, 1)
print(future)
print(future$done())
print(future$result())
