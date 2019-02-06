''' This file tests the SemSL netcdf4 frontend.
'''
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import unittest
import numpy as np
import os
import sys
from glob import glob

from SemSL.Frontends.SL_netCDF4 import slDataset as Dataset
from SemSL._slCacheManager import slCacheManager
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager

from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB

import logging

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler()) # Writes to console
logger.setLevel(logging.DEBUG)
logging.getLogger('boto3').propagate=False
logging.getLogger('botocore').propagate=False
logging.getLogger('s3transfer').propagate=False
logging.getLogger('urllib3').propagate=False

FNAME = './testfile.nc'
VARNAME = 'var'
DIMSIZE = 20


class test_set1_variables(unittest.TestCase):
    def test_multicfavar(self):
        # Create test dataset
        f = Dataset('./testnc_multivar.nc', 'w')
        f.setncattr('test', 'Created for SemSL tests')

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
        np.random.seed(0)
        var = f.createVariable('var', 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        var.setncattr('units', 'test unit')
        var2 = f.createVariable('var2', 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        var2[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        var2.setncattr('units', 'test unit')
        f.close()

        # Now remove the files
        os.remove('./testnc_multivar.nc')
        subfiles = glob('./testnc_multivar/*.nc')
        # print('IN TEARDOWN {}'.format(subfiles))
        for f in subfiles:
            os.remove(f)
        os.rmdir('./testnc_multivar')
        self.assertFalse(os.path.exists('./testnc_multivar/'))


    def test_changearrayvalues(self):
        # create file in backend
        self.f = Dataset('s3://minio/databucket/testnc_varchange.nc', 'w')
        self.f.setncattr('test', 'Created for SemSL tests')

        dim1 = self.f.createDimension('T', DIMSIZE)
        dim1d = self.f.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = self.f.createDimension('Z', DIMSIZE)
        dim2d = self.f.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        dim3 = self.f.createDimension('Y', DIMSIZE)
        dim3d = self.f.createVariable('Y', 'i4', ('Y',))
        dim3d[:] = range(DIMSIZE)
        dim4 = self.f.createDimension('X', DIMSIZE)
        dim4d = self.f.createVariable('X', 'i4', ('X',))
        dim4d[:] = range(DIMSIZE)
        dim1d.axis = "T"
        dim2d.axis = "Z"
        dim3d.axis = "Y"
        dim4d.axis = "X"
        self.var = self.f.createVariable('var', 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.setncattr('units', 'test unit')
        self.var[:] = np.zeros((DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE))
        self.f.close()

        # remove the files in cache
        sl_cache = slCacheManager()
        sl_cache._clear_cache()

        # Now reopen
        f = Dataset('s3://minio/databucket/testnc_varchange.nc', 'a')
        var = f.variables['var']
        self.assertEqual(var[0,0,0,0], 0)

        var[0,0,0,:] = np.ones((1,1,1,DIMSIZE))
        self.assertEqual(var[0,0,0,0], 1)
        f.close()

        # remove the files in cache
        sl_cache = slCacheManager()
        sl_cache._clear_cache()

        # now reopen again and check the change
        f = Dataset('s3://minio/databucket/testnc_varchange.nc', 'r')
        var = f.variables['var']
        self.assertEqual(var[0, 0, 0, 0], 1)
        f.close()

        # cleanup
        sl_config = slConfig()
        slDB = slCacheDB()
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache = slCacheManager()
        sl_cache._clear_cache()
        s3 = conn.get()
        subfiles = s3.list_objects(Bucket='databucket', Prefix='testnc_varchange/')['Contents']
        for sf in subfiles:
            s3.delete_object(Bucket='databucket', Key=sf['Key'])
        s3.delete_object(Bucket='databucket', Key='testnc_varchange.nc')
        s3.delete_bucket(Bucket='databucket')


if __name__ == '__main__':
    unittest.main()
