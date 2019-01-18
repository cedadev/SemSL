__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL.Frontends.SL_netCDF4 import slDataset as Dataset
from SemSL._slCacheManager import slCacheManager
from SemSL._slCacheDB import slCacheDB
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager
import numpy as np
import os
from glob import glob
import cProfile

sl_cache = slCacheManager()

def create_file():
    DIMSIZE = 80
    VARNAME = 'var'

    f = Dataset('s3://caringo/mjones07/testnc.nc', 'w',format='NETCDF4')
    f.test = 'Created for SemSL tests'

    dim1 = f.createDimension('T', DIMSIZE)
    dim1d = f.createVariable('T', 'i4', ('T',))
    dim1d[:] = range(DIMSIZE)
    dim2 = f.createDimension('Z', DIMSIZE)
    dim2d = f.createVariable('Z', 'i4', ('Z',))
    dim2d[:] = range(DIMSIZE)
    dim3 = f.createDimension('Y', DIMSIZE)
    dim3d = f.createVariable('Y', 'i4', ('Y',))
    dim3d[:] = range(DIMSIZE)
    dim4 = f.createDimension('X', DIMSIZE)
    dim4d = f.createVariable('X', 'i4', ('X',))
    dim4d[:] = range(DIMSIZE)
    dim1d.axis = "T"
    dim2d.axis = "Z"
    dim3d.axis = "Y"
    dim4d.axis = "X"
    var = f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
    np.random.seed(0)
    var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
    var.setncattr('units', 'test unit')
    f.close()

    sl_cache._clear_cache()


def read_file():
    f = Dataset('s3://caringo/mjones07/testnc.nc', 'r')
    v = f.variables['var']
    data = v[:]
    f.close()


def cleanup():
    sl_cache = slCacheManager()
    sl_config = slConfig()
    slDB = slCacheDB()
    # print(slDB.get_all_fids())
    cache_loc = sl_config['cache']['location']
    conn_man = slConnectionManager(sl_config)
    conn = conn_man.open("s3://caringo")
    sl_cache._clear_cache()
    s3 = conn.get()
    s3.delete_object(Bucket='mjones07', Key='testnc.nc')
    #s3.delete_bucket(Bucket='databucket')


create_file()
read_file()
cleanup()