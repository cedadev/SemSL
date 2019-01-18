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

class test_set0_ReadWrite(unittest.TestCase):

    def test_01_posix_write(self):

        self.f = Dataset('./testnc.nc','w')
        self.f.test = 'Created for SemSL tests'

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
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.var.setncattr('units', 'test unit')
        self.f.close()



    def test_04_s3_write(self):
        self.f = Dataset('s3://minio/databucket/testnc.nc', 'w')
        self.f.test = 'Created for SemSL tests'

        sl_config = slConfig()
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
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.units = 'test unit'
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.f.close()
        # Check what is in the cacheDB and cache area
        slDB = slCacheDB()
        allcachedfids = slDB.get_all_fids()
        cachedir = sl_config['cache']['location']
        cacheareafiles = glob('{}/*.nc'.format(cachedir))
        cacheareasubfiles = glob('{}/testnc/*.nc'.format(cachedir))

        all_cache = []
        for i in cacheareasubfiles:
            all_cache.append(i.split('/')[-1])
        for i in cacheareafiles:
            all_cache.append(i.split('/')[-1])

        allcached = []
        for i in allcachedfids:
            allcached.append(i.split('/')[-1])

        all_cache.sort()
        #print(all_cache)
        allcached.sort()
        #print(allcached)
        self.assertEqual(all_cache,allcached)
        # remove from cache
        sl_cache = slCacheManager()
        sl_cache._clear_cache()

    def test_02_read_posix(self):
        self.f = Dataset('./testnc.nc', 'r')

        v = self.f.getVariable('var')

        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_05_read_s3(self):
        self.f = Dataset('s3://minio/databucket/testnc.nc', 'r')
        v = self.f.getVariable('var')
        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_03_remove_posix(self):
        os.remove('./testnc.nc')
        subfiles = glob('./testnc/*.nc')
        for f in subfiles:
            os.remove(f)
        os.rmdir('testnc')

        sl_cache = slCacheManager()
        sl_cache._clear_cache()

    def test_06_remove_s3(self):
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        #print(slDB.get_all_fids())
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        subfiles = s3.list_objects(Bucket='databucket',Prefix='testnc/')['Contents']
        for sf in subfiles:
            s3.delete_object(Bucket='databucket',Key=sf['Key'])
        s3.delete_object(Bucket='databucket',Key='testnc.nc')
        s3.delete_bucket(Bucket='databucket')

    def test_07_write_none_cfa(self):
        self.f = Dataset('s3://minio/databucket/testnc_noncfa.nc', 'w', format='NETCDF4')
        self.f.test = 'Created for SemSL tests'

        sl_config = slConfig()
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
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.units = 'test unit'
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

        slDB = slCacheDB()
        allcachedfids = slDB.get_all_fids()
        cachedir = sl_config['cache']['location']
        cacheareafiles = glob('{}/*.nc'.format(cachedir))
        cacheareasubfiles = glob('{}/testnc_noncfa/*.nc'.format(cachedir))

        all_cache = []
        for i in cacheareasubfiles:
            all_cache.append(i.split('/')[-1])
        for i in cacheareafiles:
            all_cache.append(i.split('/')[-1])

        allcached = []
        for i in allcachedfids:
            allcached.append(i.split('/')[-1])

        all_cache.sort()
        # print(all_cache)
        allcached.sort()
        # print(allcached)
        self.assertEqual(all_cache, allcached)
        # remove from cache
        sl_cache = slCacheManager()
        sl_cache._clear_cache()

    def test_08_read_none_cfa(self):
        self.f = Dataset('s3://minio/databucket/testnc_noncfa.nc', 'r')
        v = self.f.variables['var']
        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_09_remove_none_cfa(self):
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        #print(slDB.get_all_fids())
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket',Key='testnc_noncfa.nc')
        s3.delete_bucket(Bucket='databucket')

    def test_10_posix_chunk_write(self):

        self.f = Dataset('./testnc.nc','w')
        self.f.test = 'Created for SemSL tests'

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
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), chunksizes=(5,5,5,5))
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.var.setncattr('units', 'test unit')
        self.f.close()
        # check that the subfile has the right chunking
        f = Dataset('./testnc/testnc_var_[0].nc')
        v = f.variables['var']
        self.assertEqual([5,5,5,5],v.chunking())
        f.close()

    def test_11_posix_chunk_remove(self):
        os.remove('./testnc.nc')
        subfiles = glob('./testnc/*.nc')
        for f in subfiles:
            os.remove(f)
        os.rmdir('testnc')

        sl_cache = slCacheManager()
        sl_cache._clear_cache()


if __name__ == '__main__':
    unittest.main()