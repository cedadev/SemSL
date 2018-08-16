#!/usr/bin/env python2.7

''' This file tests the s3netcdf4 library.
'''

import unittest
import numpy as np
import os
import sys
HOME=os.environ["HOME"]
sys.path.append(os.path.expanduser(HOME+"/s3netcdf/S3-netcdf-python/"))
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset

FNAME = os.getcwd()+'/testfile.nc'
VARNAME = 'var'
DIMSIZE = 20

class TestConnection(unittest.TestCase):

    def setUp(self):
        self.fid = open('./testfileconnection','w')

    def tearDown(self):
        os.remove('./testfileconnection')

    def test_connection(self):
        self.fid.write('test')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~POSIX FILE TESTS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

class TestFileCreate(unittest.TestCase):

    def setUp(self):

        # create 4 d file with dimension variables and attributes
        self.f = Dataset(FNAME, 'w', format='CFA4')
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
        self.f.close()

    def tearDown(self):
        try:
            self.f.close()
        except:
            pass

    def test_set_data(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        data = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.f.getVariable(VARNAME)[:] = data
        self.assertEqual(np.count_nonzero(self.f.getVariable(VARNAME))[:],160000,'Data not written correctly -- array contains zeros')
        self.f.close()

    def test_dim_count(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        self.assertEqual(len(self.f.dimensions),4)

    def test_dim_name(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        self.assertEqual([self.f.dimensions['T'].name, self.f.dimensions['Z'].name, self.f.dimensions['Y'].name,
                          self.f.dimensions['X'].name], ['T', 'Z', 'Y', 'X'])

    def test_dim_size(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        self.assertEqual([self.f.dimensions['T'].size,self.f.dimensions['Z'].size,self.f.dimensions['Y'].size,self.f.dimensions['X'].size], [20,20,20,20])

    def test_var_shape(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        self.assertEqual(self.f.getVariable(VARNAME).shape, [20,20,20,20])

    def test_var_size(self):
        self.f = Dataset(FNAME, 'r', format='CFA4')
        self.assertEqual(self.f.getVariable(VARNAME).size, 160000)


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
        var = self.f.getVariable(VARNAME)


class TestGetData(unittest.TestCase):
    def setUp(self):
        self.f = Dataset(FNAME, 'r', format='NETCDF4')

    def tearDown(self):
        self.f.close()

    def test_get_data(self):
        var = self.f.getVariable(VARNAME)
        data = var[:]
        self.assertEqual(np.count_nonzero(data), 160000)

    def test_subset_data(self):
        var = self.f.getVariable(VARNAME)
        subset  = var[5:15,5:15,5:15,5:15]
        self.assertEqual(np.count_nonzero(subset), 10000)


class TestAppendData(unittest.TestCase):#?? supporting this?

    pass


class TestChangeMeta(unittest.TestCase):#?? supporting this?

    pass

class TestRemove(unittest.TestCase):

    def test_removefile(self):
        pass
        #os.system('python2.7 ~/s3netcdf/S3-netcdf-python/bin/s3_cfa_rm.py ~/s3netcdf/test/testfile.nc')
        #os.rmdir('~/s3netcdf/test/testfile')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


if __name__ == '__main__':
    unittest.main()