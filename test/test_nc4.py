#!/usr/bin/env python2.7

''' This file tests the s3netcdf4 library.
'''

import unittest
import numpy as np
import os
import sys
from glob import glob

from SemSL.Frontends.SL_netCDF4 import s3Dataset as Dataset
from SemSL._slCacheManager import slCacheManager
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager

from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB

FNAME = './testfile.nc'
VARNAME = 'var'
DIMSIZE = 20

class testReadWrite(unittest.TestCase):

    def test_1_posix_write(self):
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
        self.var.units = 'test unit'
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

    def test_4_s3_write(self):
        self.f = Dataset('s3://minio/databucket/testnc.nc', 'w')
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
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.f.close()
        # TODO: Check what is in the cacheDB and cache area?
        slDB = slCacheDB()
        allcachedfids = slDB.get_all_fids()
        cacheareafiles = glob('/home/matthew/cachearea/*.nc')
        cacheareasubfiles = glob('/home/matthew/cachearea/testnc/*.nc')

        all_cache = []
        for i in cacheareasubfiles:
            all_cache.append(i.split('/')[-1])
        for i in cacheareafiles:
            all_cache.append(i.split('/')[-1])

        allcached = []
        for i in allcachedfids:
            allcached.append(i.split('/')[-1])

        all_cache.sort()
        print(all_cache)
        allcached.sort()
        print(allcached)
        self.assertEqual(all_cache,allcached)
        # remove from cache
        sl_cache = slCacheManager()
        sl_cache._clear_cache()

    def test_2_read_posix(self):
        self.f = Dataset('./testnc.nc', 'r')

        v = self.f.getVariable('var')

        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_5_read_s3(self):
        self.f = Dataset('s3://minio/databucket/testnc.nc', 'r')
        v = self.f.getVariable('var')
        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_3_remove_posix(self):
        os.remove('./testnc.nc')
        subfiles = glob('./testnc/*.nc')
        for f in subfiles:
            os.remove(f)
        os.rmdir('testnc')

        sl_cache = slCacheManager()
        sl_cache._clear_cache()

    def test_6_remove_s3(self):
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        print(slDB.get_all_fids())
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

class TestMethods_posix(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('./testnc.nc', 'w')
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
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc.nc')
        subfiles = glob('./*.nc')
        for f in subfiles:
            os.remove(f)
        os.rmdir('testnc')

class TestMethods_s3(unittest.TestCase):
    def setUp(self):
        pass

"""    

Questions:
    - do subfiles need to retain the attributes of the variable in the master file? 
    - what happens when the varible name is changed?? Change in sub files as well -- retain all the variable information in the sub files as well
    
Ideas for tests
    - multi variables
    - read a non CFA file??? (I think I removed this functionality)
    - which methods? (don't need to test the ones passed straight through)  
        Dataset:
            getVariables
            ncattrs
            createDimension
            createVariable
            close
            flush
            sync
            cmptypes
            createCompoundType
            createEnumType
            createGroup
            createVLType
            data_model
            delncattr
            dimensions
            disk_format
            enumtypes
            file_format
            filepath
            get_variables_by_attribute
            getncattr
            parent
            path
            renameAttribute
            renameDimension
            renameGroup
            renameVariable
            set_auto_chartostring
            set_auto_mask
            set_auto_maskandscale
            set_auto_scale
            set_fill_off
            set_fill_on
            setncattr
            setncattr_string
            setncattrs
        Variable:
            _accessed_subfiles
            __repr__
            __array__
            __unicode__
            name
            name
            datatype
            shape
            _shape
            _size
            _dimensions
            group
            ncattrs
            setncattr
            setncattr_string
            setncatts
            getncattr
            delncattr
            filters
            endian
            chunking
            get_var_chunk_cache
            set_var_chunk_cache
            __delattr__
            __setattr__
            __getattr__
            renameAttribute
            __len__
            assignValue
            getValue
            set_auto_chartostring
            set_auto_maskandscale
            set_auto_scale
            set_auto_mask
            __reduce__
            __getitem__
            __setitem__
    


"""


""" This refers to old lib
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~POSIX FILE TESTS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

class TestFileCreate(unittest.TestCase):

    def setUp(self):

        # create 4 d file with dimension variables and attributes
        self.f = Dataset(FNAME, 'w')
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
        self.f = Dataset(FNAME, 'a')
        data = np.random.rand(DIMSIZE,DIMSIZE,DIMSIZE,DIMSIZE)
        self.f.getVariable(VARNAME)[:] = data
        self.assertEqual(np.count_nonzero(self.f.getVariable(VARNAME)),160000,'Data not written correctly -- array contains zeros')
        self.f.close()

    def test_dim_count(self):
        self.f = Dataset(FNAME, 'r')
        self.assertEqual(len(self.f.dimensions),4)

    def test_dim_name(self):
        self.f = Dataset(FNAME, 'r')
        self.assertEqual([self.f.dimensions['T'].name, self.f.dimensions['Z'].name, self.f.dimensions['Y'].name,
                          self.f.dimensions['X'].name], ['T', 'Z', 'Y', 'X'])

    def test_dim_size(self):
        self.f = Dataset(FNAME, 'r')
        self.assertEqual([self.f.dimensions['T'].size,self.f.dimensions['Z'].size,self.f.dimensions['Y'].size,self.f.dimensions['X'].size], [20,20,20,20])

    def test_var_shape(self):
        self.f = Dataset(FNAME, 'r')
        self.assertEqual(self.f.getVariable(VARNAME).shape, (20,20,20,20))

    def test_var_size(self):
        self.f = Dataset(FNAME, 'r')
        self.assertEqual(self.f.getVariable(VARNAME).size, 160000)


class TestGetFileAttr(unittest.TestCase):

    def setUp(self):
        self.f = Dataset(FNAME, 'r')

    def tearDown(self):
        self.f.close()

    def test_global_attr(self):
        self.assertEqual(self.f.test,'Created for SemSL tests')

    def test_var_attr(self):
        self.assertEqual(self.f.variables[VARNAME].units,'test unit')


class TestGetVar(unittest.TestCase):

    def setUp(self):
        self.f = Dataset(FNAME, 'r')

    def tearDown(self):
        self.f.close()

    def test_get_var(self):
        var = self.f.getVariable(VARNAME)


class TestGetData(unittest.TestCase):
    def setUp(self):
        self.f = Dataset(FNAME, 'r')

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
"""

if __name__ == '__main__':
    unittest.main()