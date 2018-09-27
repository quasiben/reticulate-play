import rpy2
import pandas as pd
import numpy as np
import dask.dataframe as dd

from dask.distributed import Client
client = Client('127.0.0.1:8786')

def wrap_r(_func):
    def _r(*args, **kwargs):
        import rpy2.robjects as robjects
        rfunc = robjects.r(_func)
        return rfunc(*args)
    return _r
    

def unserialize(_func):
    def _r(*args):
        import rpy2.robjects as robjects
        rfunc = robjects.rinterface.unserialize(_func)
        return rfunc(*args)
    return _r


def start_r():
    import rpy2.robjects as robjects
    initr = robjects.rinterface.initr()

def deserialize_and_run(byte_str, *args):
    import rpy2.robjects as robjects
    # initr = robjects.rinterface.initr()
    print("ARGS: ", args)
    print(type(byte_str))
    print(type(bytes(byte_str)))
    rfunc = robjects.rinterface.unserialize(bytes(byte_str), 3)
    print(rfunc)
    # need to inspect types here
    # rpy2.rinterface.FloatSexpVector
    result = rfunc(*args)[0]
    print(result)
    # robjects.rinterface.endr(initr)
    return result

def START_R():
    client.run(start_r)

def py_lambda(_func, *args):
    # client.submit(wrap_r(_func), args)
    result = client.submit(deserialize_and_run, _func, *args)
    return result
