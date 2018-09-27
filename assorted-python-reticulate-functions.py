import os
import subprocess

import pandas as pd
import numpy as np
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd


def random_ts():
    """
    ts <- random_ts()
    """
    rng = pd.date_range('1/1/2018', periods=5, freq='H')
    ts = pd.Series(np.random.randn(len(rng)), index=rng)
    return ts


def random_df(periods=30):
    """
    df <- random_df(2)
    """
    rng = pd.date_range('1/1/2018', periods=periods, freq='H')
    df = pd.DataFrame(list(zip(np.random.randn(len(rng)), rng)), columns=['vals', 'dates'])
    return df

def start_cluster(nworkers=2):
    bin_dir = "/Users/quasiben/anaconda/envs/reticulate-r-base/bin/"
    sch_bin = os.path.join(bin_dir, "dask-scheduler")
    worker_bin = os.path.join(bin_dir, "dask-worker")
    scheduler_cmd = ["%s" % (sch_bin)]
    worker_cmd = [worker_bin, "127.0.0.1:8786", "--nprocs=%d" % int(nworkers)]
    
    try:
        subprocess.Popen(scheduler_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        print(str(e))

    try:
        subprocess.Popen(worker_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        print(str(e))

    client = Client("127.0.0.1:8786")
    return client


def random_dd_ts():
    df = dd.demo.make_timeseries('2000-01-01', '2000-12-31', freq='10s', partition_freq='1M',
                             dtypes={'name': str, 'id': int, 'x': float, 'y': float})
    return df


def r_to_dd(df):
    return dd.from_pandas(df, npartitions=4)


def dd_groupby():
    df = random_dd_ts()
