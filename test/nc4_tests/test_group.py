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

class test_groups_posix_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        f = Dataset('./testnc_group.nc', 'w',format='NETCDF4')
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
        f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc_group.nc')

    def test_onegroup(self):
        f = Dataset('./testnc_group.nc', 'a', format='NETCDF4')
        # create group
        g = f.createGroup('testgroup')

        v = g.createVariable('var','f8',('T','Z','Y','X',))
        v[:] = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)

        f.close()

    def test_twogroup(self):
        f = Dataset('./testnc_group.nc', 'a', format='NETCDF4')
        # create group
        g = f.createGroup('testgroup')

        v = g.createVariable('var', 'f8', ('T', 'Z', 'Y', 'X',))
        v[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)

        g2 = f.createGroup('testgroup2')

        v2 = g2.createVariable('var', 'f8', ('T', 'Z', 'Y', 'X',))
        v2[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)

        f.close()

class test_groups_posix_cfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        f = Dataset('./testnc_group.nc', 'w')
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
        f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc_group.nc')
        subfiles = glob('./testnc_group/*.nc')
        # print('IN TEARDOWN {}'.format(subfiles))
        for f in subfiles:
            os.remove(f)
        os.rmdir('./testnc_group')
        self.assertFalse(os.path.exists('./testnc_group/'))

    def test_onegroup(self):
        f = Dataset('./testnc_group.nc', 'a', format='NETCDF4')
        # create group
        g = f.createGroup('testgroup')

        v = g.createVariable('var','f8',('T','Z','Y','X',))
        # TODO I think this throws an error because it can't find the dimensions that belong to the rootgroup
        v[:] = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)

        f.close()

    def test_twogroup(self):
        f = Dataset('./testnc_group.nc', 'a', format='NETCDF4')
        # create group
        g = f.createGroup('testgroup')

        v = g.createVariable('var','f8',('T','Z','Y','X',))
        v[:] = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)

        g2 = f.createGroup('testgroup2')

        v2 = g2.createVariable('var', 'f8', ('T', 'Z', 'Y', 'X',))
        v2[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)

        f.close()

if __name__ == '__main__':
    unittest.main()