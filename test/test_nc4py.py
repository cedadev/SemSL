#!/usr/bin/env python2.7

''' This file tests the netcdf4-python library. it is used to build the other tests for SemSL off, in itself it is
    not very useful...'''

import unittest
import numpy as np
import os
from netCDF4 import Dataset

FNAME = 'testfile.nc'
FNAMECC = 'testfileCC.nc'
VARNAME = 'var'
DIMSIZE = 20

class TestConnection(unittest.TestCase):

    def setUp(self):
        self.fid = open('./testfile','w')

    def tearDown(self):
        os.remove('./testfile')

    def test_connection(self):
        self.fid.write('test')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STANDARD FILE TESTS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

class TestFileCreate(unittest.TestCase):

    def setUp(self):

        # create 4 d file with dimension variables and attributes
        self.f = Dataset(FNAME, 'w', format='NETCDF4')
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
        self.var.units = 'test unit'

    def tearDown(self):
        self.f.close()

    def test_set_data(self):
        data = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.var[:] = data
        self.assertEqual(np.count_nonzero(self.var[:]),160000,'Data not written correctly -- array contains zeros')

    def test_dim_count(self):
        self.assertEqual(len(self.f.dimensions),4)

    def test_dim_name(self):
        self.assertEqual([self.f.dimensions['T'].name, self.f.dimensions['Z'].name, self.f.dimensions['Y'].name,
                          self.f.dimensions['X'].name], ['T', 'Z', 'Y', 'X'])

    def test_dim_size(self):
        self.assertEqual([self.f.dimensions['T'].size,self.f.dimensions['Z'].size,self.f.dimensions['Y'].size,self.f.dimensions['X'].size], [20,20,20,20])

    def test_var_shape(self):
        self.assertEqual(self.f.variables[VARNAME].shape, (20,20,20,20))

    def test_var_size(self):
        self.assertEqual(self.f.variables[VARNAME].size, 160000)


class TestGetFileAttr(unittest.TestCase):

    def setUp(self):
        self.f = Dataset(FNAME, 'r', format='NETCDF4')

    def tearDown(self):
        self.f.close()

    def test_global_attr(self):
        self.assertEqual(self.f.test,'Created for SemSL tests')

    def test_var_attr(self):
        self.assertEqual(self.f.variables[VARNAME].units,'test unit')


class TestGetVar(unittest.TestCase):

    def setUp(self):
        self.f = Dataset(FNAME, 'r', format='NETCDF4')

    def tearDown(self):
        self.f.close()

    def test_get_var(self):
        var = self.f.variables[VARNAME]


class TestGetData(unittest.TestCase):
    def setUp(self):
        self.f = Dataset(FNAME, 'r', format='NETCDF4')

    def tearDown(self):
        self.f.close()

    def test_get_data(self):
        var = self.f.variables[VARNAME]
        data = var[:]
        self.assertEqual(np.count_nonzero(data), 160000)

    def test_subset_data(self):
        var = self.f.variables[VARNAME]
        subset  = var[5:15,5:15,5:15,5:15]
        self.assertEqual(np.count_nonzero(subset), 10000)


class TestAppendData(unittest.TestCase):#?? supporting this?

    pass


class TestChangeMeta(unittest.TestCase):#?? supporting this?

    pass

class TestRemove(unittest.TestCase):

    def test_removefile(self):
        os.remove('./testfile.nc')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~CHUNKED AND COMPRESSED FILE TESTS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

class TestFileCreateCC(unittest.TestCase):

    def setUp(self):

        # create 4 d file with dimension variables and attributes
        self.f = Dataset(FNAMECC, 'w', format='NETCDF4')
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
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=False, chunksizes=(5,5,5,5),
                                         zlib=True, complevel=9)
        self.var.units = 'test unit'

    def tearDown(self):
        self.f.close()

    def test_set_data(self):
        data = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.var[:] = data
        self.assertEqual(np.count_nonzero(self.var[:]),160000,'Data not written correctly -- array contains zeros')

    def test_dim_count(self):
        self.assertEqual(len(self.f.dimensions),4)

    def test_dim_name(self):
        self.assertEqual([self.f.dimensions['T'].name, self.f.dimensions['Z'].name, self.f.dimensions['Y'].name,
                          self.f.dimensions['X'].name], ['T', 'Z', 'Y', 'X'])

    def test_dim_size(self):
        self.assertEqual([self.f.dimensions['T'].size,self.f.dimensions['Z'].size,self.f.dimensions['Y'].size,self.f.dimensions['X'].size], [20,20,20,20])

    def test_var_shape(self):
        self.assertEqual(self.f.variables[VARNAME].shape, (20,20,20,20))

    def test_var_size(self):
        self.assertEqual(self.f.variables[VARNAME].size, 160000)

    def test_chunksize(self):
        self.assertEqual(self.f.variables[VARNAME].chunking(),[5,5,5,5])


class TestRemoveCC(unittest.TestCase):

    def test_removefile(self):
        os.remove('./testfileCC.nc')

if __name__ == '__main__':
    unittest.main()