library(reticulate)
library(dplyr)
library(purrr)

source_python('cudar-interface.py')

filter_.dask_cudf.core.DataFrame <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ...)
  env <- lazyeval::common_env(dots)
  expr <- lapply(dots, `[[`, "expr")
  return(.data$query(toString(expr)))
}

select_.dask_cudf.DataFrame <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ...)
  env <- lazyeval::common_env(dots)
  expr <- lapply(dots, `[[`, "expr")
  s_expr <- toString(expr)
  expr_list <- flatten(strsplit(s_expr, ', '))
  return(.data[expr_list])
}


select_.dask_cudf.core.DataFrame <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ...)
  env <- lazyeval::common_env(dots)
  expr <- lapply(dots, `[[`, "expr")
  s_expr <- toString(expr)
  expr_list <- flatten(strsplit(s_expr, ', '))
  return(.data[expr_list])
}

select_.cudf.dataframe.dataframe.DataFrame <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ...)
  env <- lazyeval::common_env(dots)
  expr <- lapply(dots, `[[`, "expr")
  s_expr <- toString(expr)
  expr_list <- flatten(strsplit(s_expr, ', '))
  return(.data[expr_list])
}


filter_.dask_cudf.core.DataFrame <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ...)
  env <- lazyeval::common_env(dots)
  expr <- lapply(dots, `[[`, "expr")
  return(.data$query(toString(expr)))
}


# data <- '/home/nfs/bzaitlen/nyctaxi-with-alldata.csv'
# gdf <- cuda_read_csv(data)
# gdf <- cuda_read_csv('/home/nfs/mrocklin/data/nyc/yellow_tripdata_2017-*.csv')
# a <- gdf %>% select(passenger_count, total_amount)
# a <- gdf %>% select(passenger_count, total_amount) %>% filter(passenger_count < 2)
# gdf$passenger_count$sum()$compute()

#library(reticulate)
#library(dplyr)
#source('cudar.R')
#data <- '/home/nfs/bzaitlen/nyctaxi-with-alldata.csv'
#gdf <- cuda_read_csv(data)

# cudf <- import("cudf")
# cudf$datasets$timeseries(freq='10d')
