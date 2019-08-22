import cudf
import dask_cudf
import dask.distributed as dd

def cuda_read_csv(filepath):
    """
    '/home/nfs/mrocklin/data/nyc/yellow_tripdata_2017-*.csv'
    """
    return cudf.read_csv(filepath)

def dask_cuda_read_csv(filepath):
    """
    '/home/nfs/mrocklin/data/nyc/yellow_tripdata_2017-*.csv'
    """
    return dask_cudf.read_csv(filepath)
