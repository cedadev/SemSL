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

class test_set1_ReadWrite(unittest.TestCase):

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
        #print(all_cache)
        allcached.sort()
        #print(allcached)
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

    def test_7_write_none_cfa(self):
        self.f = Dataset('s3://minio/databucket/testnc_noncfa.nc', 'w', format='NETCDF4')
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
        # TODO: Check what is in the cacheDB and cache area?
        slDB = slCacheDB()
        allcachedfids = slDB.get_all_fids()
        cacheareafiles = glob('/home/matthew/cachearea/*.nc')
        cacheareasubfiles = glob('/home/matthew/cachearea/testnc_noncfa/*.nc')

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

    def test_8_read_none_cfa(self):
        self.f = Dataset('s3://minio/databucket/testnc_noncfa.nc', 'r')
        v = self.f.getVariable('var')
        data = v[:]
        self.assertTrue(data[0,0,0,0])
        self.f.close()

    def test_9_remove_none_cfa(self):
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

class test_set4_Methods_posix(unittest.TestCase):
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

    def test_pass(self):
        pass

    def tearDown(self):
        # remove test file
        os.remove('./testnc.nc')
        subfiles = glob('./testnc/*.nc')
        for f in subfiles:
            os.remove(f)
        os.rmdir('./testnc')

class test_set2_Methods_posix_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('./testnc_methods.nc', 'w',format='NETCDF4')
        self.f.setncattr('test','Created for SemSL tests')

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
        os.remove('./testnc_methods.nc')

    def test_getVariables(self):
        # Create new variable to test that 2 are returned
        f = Dataset('./testnc_methods.nc', 'a')
        f.createVariable('test_var', 'f8', ('X','Y'))
        f.close()
        # Check the number and names of vars
        f = Dataset('./testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars),6)
        f.close()

    def test_dataset_ncattrs(self):
        # ncattrs returns global variables when called on the dataset
        # So need to add some attributes to the file
        f = Dataset('./testnc_methods.nc', 'a')
        f.setncattr('test_attr','test_attr_value')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        attr_val = f.getncattr('test_attr')
        self.assertEqual(attr_val,'test_attr_value')
        f.close()

    def test_createDimension(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.createDimension('h', 10)
        dim = f.createVariable('h', 'i4', ('h',))
        dim[:] = np.ones(10)
        dim.axis = "Z"
        f.close()
        # Check the number and names of vars
        f = Dataset('./testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 6)
        # Get the dimensions
        dims = f.dimensions
        self.assertEqual(len(dims),5)
        f.close()

    def test_createVariable(self):
        # Create new variable to test that 2 are returned
        f = Dataset('./testnc_methods.nc', 'a')
        f.createVariable('test_var', 'f8', ('X', 'Y'))
        f.close()
        # Check the number and names of vars
        f = Dataset('./testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 6)
        f.close()

    def test_close(self):
        #TODO how do I test this?? is using it all the time enough?
        # with the other filescan affirm uploads. and pushing info to sub files?
        f = Dataset('./testnc_methods.nc', 'r')
        f.close()

    def test_flush(self):
        #TODO again this is more important with the other test blocks
        f = Dataset('./testnc_methods.nc', 'r')
        f.flush()
        f.close()

    def test_sync(self):
        # TODO again this is more important with the other test blocks
        f = Dataset('./testnc_methods.nc', 'r')
        f.sync()
        f.close()

    def test_createCompoundType_cmptypes(self):
        # from netcdf4-python docs
        f = Dataset("./complex.nc", "w", format='NETCDF4')
        size = 3  # length of 1-d complex array
         # create sample complex data.
        datac = np.exp(1j * (1. + np.linspace(0, np.pi, size)))
         # create complex128 compound data type.
        complex128 = np.dtype([("real", np.float64), ("imag", np.float64)])
        complex128_t = f.createCompoundType(complex128, "complex128")
         # create a variable with this data type, write some data to it.
        f.createDimension("x_dim", None)
        v = f.createVariable("cmplx_var", complex128_t, "x_dim")
        data = np.empty(size, complex128)  # numpy structured array
        data["real"] = datac.real
        data["imag"] = datac.imag
        v[:] = data  # write numpy structured array to netcdf compound var
         # close and reopen the file, check the contents.
        f.close()
        f = Dataset("./complex.nc")
        v = f.variables["cmplx_var"]
        datain = v[:]  # read in all the data into a numpy structured array
         # create an empty numpy complex array
        datac2 = np.empty(datain.shape, np.complex128)
         # .. fill it with contents of structured array.
        datac2.real = datain["real"]
        datac2.imag = datain["imag"]

        cmpt = f.cmptypes
        self.assertEqual(cmpt['complex128'].name, 'complex128')

        f.close()
        os.remove('./complex.nc')

    def test_createEnumType_enumtypes(self):
        # from netcdf4-python docs
        nc = Dataset('./clouds.nc', 'w',format='NETCDF4')
        # python dict with allowed values and their names.
        enum_dict = {u'Altocumulus': 7, u'Missing': 255,
                         u'Stratus': 2, u'Clear': 0,
        u'Nimbostratus': 6, u'Cumulus': 4, u'Altostratus': 5,
        u'Cumulonimbus': 1, u'Stratocumulus': 3}
        # create the Enum type called 'cloud_t'.
        cloud_type = nc.createEnumType(np.uint8, 'cloud_t', enum_dict)
        self.assertEqual(cloud_type.name,'cloud_t')

        nc.close()
        nc = Dataset('./clouds.nc', 'r')
        self.assertEqual(nc.enumtypes['cloud_t'].name,'cloud_t')
        nc.close()
        os.remove('./clouds.nc')

    def test_createGroup(self):
        # create group and add varaible and dims to group
        f = Dataset('./test_groups.nc','w',format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        f.close()
        os.remove('./test_groups.nc')

    def test_createVLType_VLTypes(self):
        # from netcdf4-python docs
        f = Dataset("./tst_vlen.nc", "w", format='NETCDF4')
        vlen_t = f.createVLType(np.int32, "phony_vlen")
        x = f.createDimension("x", 3)
        y = f.createDimension("y", 4)
        vlvar = f.createVariable("phony_vlen_var", vlen_t, ("y", "x"))
        import random
        data = np.empty(len(y) * len(x), object)
        rand_shape = []
        for n in range(len(y) * len(x)):
            rand = random.randint(1, 10)
            rand_shape.append(rand)
            data[n] = np.arange(rand, dtype="int32") + 1
        data = np.reshape(data, (len(y), len(x)))
        vlvar[:] = data
        n = 0
        for i in range(len(y)):
            for j in range(len(x)):
                self.assertEqual(len(vlvar[i,j]),rand_shape[n])
                n+=1

        self.assertEqual(f.VLTypes['phony_vlen'].name,'phony_vlen')
        f.close()
        os.remove('./tst_vlen.nc')

    def test_data_model(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.data_model,'NETCDF4')
        f.close()

    def test_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        # create new attr
        f.setncattr('test_to_del','string')
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        self.assertEqual(f.getncattr('test_to_del'),'string')
        f.delncattr('test_to_del')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        test_bool = False
        try:
            attr = f.getncattr('test_to_del')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        f.close()

    def test_dimensions(self):
        # Check the number and names of dims
        f = Dataset('./testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 5)
        # Get the dimensions
        dims = f.dimensions
        self.assertEqual(len(dims), 4)
        f.close()

    def test_disk_format(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.disk_format,'HDF5')
        f.close()

    def test_file_format(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.data_model, 'NETCDF4')
        f.close()

    def test_filepath(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.filepath(),'./testnc_methods.nc')
        f.close()

    def test_get_variables_by_attribute(self):
        f = Dataset('./testnc_methods.nc', 'r')
        # get all variables with x axis
        varx = f.get_variables_by_attribute(axis='X')
        self.assertEqual(varx[0].name, 'X')

        var = f.get_variables_by_attribute(units='test unit')
        self.assertEqual(var[0].name, 'var')
        f.close()

    def test_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'r')
        attr = f.getncattr('test')
        self.assertEqual(attr, 'Created for SemSL tests')
        f.close()

    def test_parent(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.parent, None)
        f.close()
        # create group and add varaible and dims to group
        f = Dataset('./test_groups.nc', 'w', format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        subfg = fg.createGroup('subgrouptest')
        self.assertEqual(f.parent, None)
        self.assertEqual(fg.parent.path,'/')
        self.assertEqual(subfg.parent.path, '/testgroup')
        f.close()
        os.remove('./test_groups.nc')

    def test_path(self):
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.path, '/')
        f.close()

    def test_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.renameAttribute('test','renamedattr')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        test_bool = False
        try:
            attr = f.getncattr('test')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(f.getncattr('renamedattr'), 'Created for SemSL tests')
        f.close()

    def test_renameDimension(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.renameDimension('X', 'renamedX')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertNotEqual(list(f.dimensions.keys()), ['T','Z','Y','X'])
        self.assertEqual(list(f.dimensions.keys()), ['T','Z','Y','renamedX'])
        f.close()

    def test_renameGroup(self):
        # create group and add varaible and dims to group
        f = Dataset('./test_groups.nc', 'w', format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        f.close()

        f = Dataset('./test_groups.nc', 'a')
        f.renameGroup('testgroup','renamedgroup')
        var = f.groups['renamedgroup'].variables['var']
        self.assertEqual(var.shape, (20,20))

        f.close()

        os.remove('./test_groups.nc')

    def test_renameVariable(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.renameVariable('var', 'renamedvar')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertNotEqual(list(f.variables.keys()), ['T', 'Z', 'Y', 'X', 'var'])
        self.assertEqual(list(f.variables.keys()), ['T', 'Z', 'Y', 'X', 'renamedvar'])
        f.close()

    def test_set_auto_chartostring(self):
        f = Dataset('./testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].chartostring)
        f.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_set_auto_mask(self):
        f = Dataset('./testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].mask)
        f.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_set_auto_maskandscale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        f.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_set_auto_scale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].scale)
        f.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_set_fill_off(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.set_fill_off()
        f.close()

    def test_set_fill_on(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.set_fill_on()
        f.close()

    def test_setncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.setncattr('newtest', 'newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_setncattrs(self):
        f = Dataset('./testnc_methods.nc', 'a')
        f.setncattrs({'newtest':'newtestvalue','secondnew':'secondnewval'})
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(f.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_name(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.name,'var')
        f.close()

    def test_datatype(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(str(v.datatype), 'float64')
        f.close()

    def test_shape(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(list(v.shape), [20,20,20,20])
        f.close()

    def test_size(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.size, 20**4)
        f.close()

    def test_dimensions(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(list(v.dimensions), ['T','Z','Y','X'])
        f.close()

    def test_group(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        grp = v.group()
        self.assertEqual(grp.test,'Created for SemSL tests')
        f.close()

    def test_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'r')
        attrs = f.ncattrs()
        self.assertEqual(attrs, ['test'])
        f.close()

    def test_var_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['units'])
        f.close()

    def test_var_setncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'),'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_filters(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.filters(),{'zlib': False, 'shuffle': False, 'complevel': 0, 'fletcher32': False})
        f.close()

    def test_endian(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.endian(), 'little')
        f.close()

    def test_chunking(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.chunking(), 'contiguous')
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    def test_assignValue_getValue(self):
        # asingns value to scalar variable
        f = Dataset('./testnc_methods.nc', 'a')
        sv = f.createVariable('scalarvar','f8')
        sv.assignValue(1)
        self.assertEqual(sv[:],[1])
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['scalarvar']
        data = v.getValue()
        self.assertEqual(data,[1])
        f.close()

    def test_var_set_auto_chartostring(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].chartostring)
        v.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_var_set_auto_mask(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        v.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_var_set_auto_maskandscale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_var_set_auto_scale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()


class test_set5_Methods_s3(unittest.TestCase):
    def setUp(self):
        pass

class test_set3_Methods_s3_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('s3://minio/databucket/testnc_methods.nc', 'w', format='NETCDF4')
        self.f.setncattr('test','Created for SemSL tests')

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
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_methods.nc')
        s3.delete_bucket(Bucket='databucket')

    def test_getVariables(self):
        # Create new variable to test that 2 are returned
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.createVariable('test_var', 'f8', ('X','Y'))
        f.close()
        # Check the number and names of vars
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars),6)
        f.close()

    def test_dataset_ncattrs(self):
        # ncattrs returns global variables when called on the dataset
        # So need to add some attributes to the file
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.setncattr('test_attr','test_attr_value')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        attr_val = f.getncattr('test_attr')
        self.assertEqual(attr_val,'test_attr_value')
        f.close()

    def test_createDimension(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.createDimension('h', 10)
        dim = f.createVariable('h', 'i4', ('h',))
        dim[:] = np.ones(10)
        dim.axis = "Z"
        f.close()
        # Check the number and names of vars
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 6)
        # Get the dimensions
        dims = f.dimensions
        self.assertEqual(len(dims),5)
        f.close()

    def test_createVariable(self):
        # Create new variable to test that 2 are returned
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.createVariable('test_var', 'f8', ('X', 'Y'))
        f.close()
        # Check the number and names of vars
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 6)
        f.close()

    def test_close(self):
        #TODO how do I test this?? is using it all the time enough?
        # with the other filescan affirm uploads. and pushing info to sub files?
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        f.close()

    def test_flush(self):
        #TODO again this is more important with the other test blocks
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        f.flush()
        f.close()

    def test_sync(self):
        # TODO again this is more important with the other test blocks
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        f.sync()
        f.close()

    def test_createCompoundType_cmptypes(self):
        # from netcdf4-python docs
        f = Dataset("s3://minio/databucket/complex.nc", "w", format='NETCDF4')
        size = 3  # length of 1-d complex array
         # create sample complex data.
        datac = np.exp(1j * (1. + np.linspace(0, np.pi, size)))
         # create complex128 compound data type.
        complex128 = np.dtype([("real", np.float64), ("imag", np.float64)])
        complex128_t = f.createCompoundType(complex128, "complex128")
         # create a variable with this data type, write some data to it.
        f.createDimension("x_dim", None)
        v = f.createVariable("cmplx_var", complex128_t, "x_dim")
        data = np.empty(size, complex128)  # numpy structured array
        data["real"] = datac.real
        data["imag"] = datac.imag
        v[:] = data  # write numpy structured array to netcdf compound var
         # close and reopen the file, check the contents.
        f.close()
        f = Dataset("s3://minio/databucket/complex.nc")
        v = f.variables["cmplx_var"]
        datain = v[:]  # read in all the data into a numpy structured array
         # create an empty numpy complex array
        datac2 = np.empty(datain.shape, np.complex128)
         # .. fill it with contents of structured array.
        datac2.real = datain["real"]
        datac2.imag = datain["imag"]

        cmpt = f.cmptypes
        self.assertEqual(cmpt['complex128'].name, 'complex128')

        f.close()
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='complex.nc')

    def test_createEnumType_enumtypes(self):
        # from netcdf4-python docs
        nc = Dataset('s3://minio/databucket/clouds.nc', 'w',format='NETCDF4')
        # python dict with allowed values and their names.
        enum_dict = {u'Altocumulus': 7, u'Missing': 255,
                         u'Stratus': 2, u'Clear': 0,
        u'Nimbostratus': 6, u'Cumulus': 4, u'Altostratus': 5,
        u'Cumulonimbus': 1, u'Stratocumulus': 3}
        # create the Enum type called 'cloud_t'.
        cloud_type = nc.createEnumType(np.uint8, 'cloud_t', enum_dict)
        self.assertEqual(cloud_type.name,'cloud_t')

        nc.close()
        nc = Dataset('s3://minio/databucket/clouds.nc', 'r')
        self.assertEqual(nc.enumtypes['cloud_t'].name,'cloud_t')
        nc.close()
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='clouds.nc')

    def test_createGroup(self):
        # create group and add varaible and dims to group
        f = Dataset('s3://minio/databucket/testnc_groups.nc','w',format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        f.close()
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_groups.nc')

    def test_createVLType_VLTypes(self):
        # from netcdf4-python docs
        f = Dataset('s3://minio/databucket/tst_vlen.nc', "w", format='NETCDF4')
        vlen_t = f.createVLType(np.int32, "phony_vlen")
        x = f.createDimension("x", 3)
        y = f.createDimension("y", 4)
        vlvar = f.createVariable("phony_vlen_var", vlen_t, ("y", "x"))
        import random
        data = np.empty(len(y) * len(x), object)
        rand_shape = []
        for n in range(len(y) * len(x)):
            rand = random.randint(1, 10)
            rand_shape.append(rand)
            data[n] = np.arange(rand, dtype="int32") + 1
        data = np.reshape(data, (len(y), len(x)))
        vlvar[:] = data
        n = 0
        for i in range(len(y)):
            for j in range(len(x)):
                self.assertEqual(len(vlvar[i,j]),rand_shape[n])
                n+=1

        self.assertEqual(f.VLTypes['phony_vlen'].name,'phony_vlen')
        f.close()
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='tst_vlen.nc')

    def test_data_model(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.data_model,'NETCDF4')
        f.close()

    def test_delncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        # create new attr
        f.setncattr('test_to_del','string')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        self.assertEqual(f.getncattr('test_to_del'),'string')
        f.delncattr('test_to_del')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        test_bool = False
        try:
            attr = f.getncattr('test_to_del')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        f.close()

    def test_dimensions(self):
        # Check the number and names of dims
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        vars = f.getVariables()
        self.assertEqual(len(vars), 5)
        # Get the dimensions
        dims = f.dimensions
        self.assertEqual(len(dims), 4)
        f.close()

    def test_disk_format(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.disk_format,'HDF5')
        f.close()

    def test_file_format(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.data_model, 'NETCDF4')
        f.close()

    def test_filepath(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        sl_config = slConfig()
        self.assertEqual(f.filepath(),sl_config['cache']['location']+'testnc_methods.nc')
        f.close()

    def test_get_variables_by_attribute(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        # get all variables with x axis
        varx = f.get_variables_by_attribute(axis='X')
        self.assertEqual(varx[0].name, 'X')

        var = f.get_variables_by_attribute(units='test unit')
        self.assertEqual(var[0].name, 'var')
        f.close()

    def test_getncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        attr = f.getncattr('test')
        self.assertEqual(attr, 'Created for SemSL tests')
        f.close()

    def test_parent(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.parent, None)
        f.close()
        # create group and add varaible and dims to group
        f = Dataset('s3://minio/databucket/testnc_groups.nc', 'w', format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        subfg = fg.createGroup('subgrouptest')
        self.assertEqual(f.parent, None)
        self.assertEqual(fg.parent.path,'/')
        self.assertEqual(subfg.parent.path, '/testgroup')
        f.close()
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_groups.nc')

    def test_path(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.path, '/')
        f.close()

    def test_renameAttribute(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.renameAttribute('test','renamedattr')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        test_bool = False
        try:
            attr = f.getncattr('test')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(f.getncattr('renamedattr'), 'Created for SemSL tests')
        f.close()

    def test_renameDimension(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.renameDimension('X', 'renamedX')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertNotEqual(list(f.dimensions.keys()), ['T','Z','Y','X'])
        self.assertEqual(list(f.dimensions.keys()), ['T','Z','Y','renamedX'])
        f.close()

    def test_renameGroup(self):
        # create group and add varaible and dims to group
        f = Dataset('s3://minio/databucket/testnc_groups.nc', 'w', format='NETCDF4')
        fg = f.createGroup('testgroup')

        dim1 = fg.createDimension('T', DIMSIZE)
        dim1d = fg.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = fg.createDimension('Z', DIMSIZE)
        dim2d = fg.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        var = fg.createVariable(VARNAME, 'f8', ('T', 'Z'), contiguous=True)

        f.close()

        f = Dataset('s3://minio/databucket/testnc_groups.nc', 'a')
        f.renameGroup('testgroup','renamedgroup')
        var = f.groups['renamedgroup'].variables['var']
        self.assertEqual(var.shape, (20,20))

        f.close()

        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_groups.nc')

    def test_renameVariable(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.renameVariable('var', 'renamedvar')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertNotEqual(list(f.variables.keys()), ['T', 'Z', 'Y', 'X', 'var'])
        self.assertEqual(list(f.variables.keys()), ['T', 'Z', 'Y', 'X', 'renamedvar'])
        f.close()

    def test_set_auto_chartostring(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].chartostring)
        f.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_set_auto_mask(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].mask)
        f.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_set_auto_maskandscale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        f.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_set_auto_scale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        self.assertTrue(f.variables['var'].scale)
        f.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_set_fill_off(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.set_fill_off()
        f.close()

    def test_set_fill_on(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.set_fill_on()
        f.close()

    def test_setncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.setncattr('newtest', 'newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_setncattr_string(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_setncattrs(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        f.setncattrs({'newtest':'newtestvalue','secondnew':'secondnewval'})
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        self.assertEqual(f.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(f.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_name(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.name,'var')
        f.close()

    def test_datatype(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(str(v.datatype), 'float64')
        f.close()

    def test_shape(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(list(v.shape), [20,20,20,20])
        f.close()

    def test_size(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.size, 20**4)
        f.close()

    def test_dimensions(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(list(v.dimensions), ['T','Z','Y','X'])
        f.close()

    def test_group(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        grp = v.group()
        self.assertEqual(grp.test,'Created for SemSL tests')
        f.close()

    def test_ncattrs(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        attrs = f.ncattrs()
        self.assertEqual(attrs, ['test'])
        f.close()

    def test_var_ncattrs(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['units'])
        f.close()

    def test_var_setncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'),'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_filters(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.filters(),{'zlib': False, 'shuffle': False, 'complevel': 0, 'fletcher32': False})
        f.close()

    def test_endian(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.endian(), 'little')
        f.close()

    def test_chunking(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.chunking(), 'contiguous')
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    def test_assignValue_getValue(self):
        # asingns value to scalar variable
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        sv = f.createVariable('scalarvar','f8')
        sv.assignValue(1)
        self.assertEqual(sv[:],[1])
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['scalarvar']
        data = v.getValue()
        self.assertEqual(data,[1])
        f.close()

    def test_var_set_auto_chartostring(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].chartostring)
        v.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_var_set_auto_mask(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        v.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_var_set_auto_maskandscale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_var_set_auto_scale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()

"""    

Problems TODO
    - name of file needs a path, even just ./test.nc not just test.nc otherwise it looks for an alias -- needs fixing
    - setting and getting attr with f.attrname not working

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

if __name__ == '__main__':
    unittest.main()