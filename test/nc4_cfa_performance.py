__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL.Frontends.SL_netCDF4 import slDataset as Dataset
from SemSL._slCacheManager import slCacheManager
import numpy as np
import os
from glob import glob
import cProfile

def create_file():
    DIMSIZE = 80
    VARNAME = 'var'
    
    f = Dataset('./testnc.nc','w')
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

def read_file():
    f = Dataset('./testnc.nc', 'r')
    v = f.variables['var']
    data = v[:]
    f.close()

def cleanup():
    os.remove('./testnc.nc')
    subfiles = glob('./testnc/*.nc')
    print('subfile count = {}'.format(len(subfiles)))
    for f in subfiles:
        os.remove(f)
    os.rmdir('testnc')

    sl_cache = slCacheManager()
    sl_cache._clear_cache()

create_file()
read_file()
cleanup()