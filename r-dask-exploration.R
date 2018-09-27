library(reticulate)
use_condaenv("reticulate-r-base")

source_python("assorted-python-reticulate-functions.py")

client <- start_cluster()

dd_ts <- random_dd_ts()
dd_ts$head()
dd_ts$groupby('name')$x$std()$compute()

futures <- client$scatter(c(5,9,3))
np <- import("numpy")
future <- client$submit(np$sum, future)
print(future$done())
print(future$result())


# R data.frame -> Dask Distributed 
data(mtcars)
head(mtcars)
dd_mtcars <- r_to_dd(mtcars)
dd_mtcars$head()

# client$`__dict__`
# foo <- client$submit(np$add, a)
# op <- import("operator")