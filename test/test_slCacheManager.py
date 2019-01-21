__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL._slCacheManager import slCacheManager
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager
import unittest
import  pickle
import os
import time
import glob
import collections
import datetime

FID_IN_CACHE = 'testincache'
FID_NOT_IN_CACHE = 'testnotincache'
EXTRA_FID_NOT_IN_CACHE = 'testextranotincache'

class TestCacheDB(unittest.TestCase):
    def setUp(self):
        self.sl_cache = slCacheManager()
        # add example file to db
        self.FID_IN_CACHE = 's3://test/testbucket/{}'.format(FID_IN_CACHE)
        self.FID_NOT_IN_CACHE = 's3://test/testbucket/{}'.format(FID_NOT_IN_CACHE)
        self.EXTRA_FID_NOT_IN_CACHE = 's3://test/testbucket/{}'.format(EXTRA_FID_NOT_IN_CACHE)
        self.sl_cache.DB.add_entry(self.FID_IN_CACHE,size=60*10**6)
        self.sl_config = slConfig()

    def tearDown(self):
        self.sl_cache._clear_cache()

    def test_read_existing_DB(self):
        db_value = self.sl_cache.DB.get_entry(self.FID_IN_CACHE)
        self.assertTrue(db_value)

    def test_read_nonexistant(self):
        db_value = self.sl_cache.DB.get_entry(self.FID_NOT_IN_CACHE)
        self.assertEqual(db_value,None)

    def test_return_fid(self):
        self.assertEqual(self.sl_cache.DB.get_fid(self.FID_IN_CACHE), self.FID_IN_CACHE)

    def test_return_creation_time(self):
        self.assertEqual(type(self.sl_cache.DB.get_creation_time(self.FID_IN_CACHE)),datetime.datetime)

    def test_return_access_time(self):
        self.assertEqual(type(self.sl_cache.DB.get_access_time(self.FID_IN_CACHE)),datetime.datetime)

    def test_get_path_from_cache(self):
        if self.sl_config['cache']['location'][-1] == '/':
            path = '{}{}'.format(self.sl_config['cache']['location'],self.FID_IN_CACHE.split('/')[-1])
        else:
            path = '{}/{}'.format(self.sl_config['cache']['location'],self.FID_IN_CACHE.split('/')[-1])

        self.assertEqual(self.sl_cache.DB.get_cache_loc(self.FID_IN_CACHE),path)

    def test_update_access_time(self):
        time.sleep(1)
        self.sl_cache.DB.update_access_time(self.FID_IN_CACHE)
        td = self.sl_cache.DB.get_access_time(self.FID_IN_CACHE)-self.sl_cache.DB.get_creation_time(self.FID_IN_CACHE)
        self.assertGreaterEqual(td.total_seconds(),1)

    def test_check_cache(self):
        self.assertTrue(self.sl_cache.DB.check_cache(self.FID_IN_CACHE))
        self.assertFalse(self.sl_cache.DB.check_cache(self.FID_NOT_IN_CACHE))

    def test_add_to_DB(self):
        self.sl_cache.DB.add_entry(self.FID_NOT_IN_CACHE)
        db_value = self.sl_cache.DB.get_entry(self.FID_NOT_IN_CACHE)
        self.assertTrue(db_value)

    def test_get_total_size(self):
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(),60*10**6)
        self.sl_cache.DB.add_entry(self.FID_NOT_IN_CACHE,60*10**6)
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(),120*10**6)

    def test_get_least_recent(self):
        # add new files and return original
        self.sl_cache.DB.add_entry(self.FID_NOT_IN_CACHE,0)
        self.assertEqual(self.sl_cache.DB.get_least_recent(),self.FID_IN_CACHE)
        # update original file timestamp and return original
        self.sl_cache.DB.update_access_time(self.FID_IN_CACHE)
        self.assertEqual(self.sl_cache.DB.get_least_recent(),self.FID_NOT_IN_CACHE)

    def test_get_all_fids(self):
        self.sl_cache.DB.add_entry(self.FID_NOT_IN_CACHE)
        self.sl_cache.DB.add_entry(self.EXTRA_FID_NOT_IN_CACHE)
        all_fids = self.sl_cache.DB.get_all_fids()
        all_fids = list(all_fids)
        self.assertEqual(collections.Counter(all_fids),
                         collections.Counter(['s3://test/testbucket/testextranotincache',
                                              's3://test/testbucket/testincache',
                                              's3://test/testbucket/testnotincache']))

    def test_get_fid(self):
        self.assertEqual(self.sl_cache.DB.get_fid(self.FID_IN_CACHE),self.FID_IN_CACHE)

class TestCacheManager(unittest.TestCase):
    # Test ability to take a file which isn't currently in the cache and put it in there then return the file path
    def setUp(self):
        self.sl_cache = slCacheManager()
        # add example file to db
        self.FID_IN_CACHE = 's3://test/cachetest/{}'.format(FID_IN_CACHE)
        self.FID_NOT_IN_CACHE = 's3://test/cachetest/{}'.format(FID_NOT_IN_CACHE)
        self.EXTRA_FID_NOT_IN_CACHE = 's3://test/cachetest/{}'.format(EXTRA_FID_NOT_IN_CACHE)
        self.sl_cache.DB.add_entry(self.FID_IN_CACHE,size=60*10**6)
        self.sl_config = slConfig()
        self.cache_loc = self.sl_config['cache']['location']
        with open('{}/{}'.format(self.cache_loc, self.FID_IN_CACHE.split('/')[-1]), 'w') as f:
            f.write('testwrite')
        with open('{}/{}'.format(self.cache_loc, self.FID_NOT_IN_CACHE.split('/')[-1]), 'w') as f:
            f.write('testwrite')
        with open('{}/{}'.format(self.cache_loc, self.EXTRA_FID_NOT_IN_CACHE.split('/')[-1]), 'w') as f:
            f.write('testwrite')
        conn_man = slConnectionManager(self.sl_config)
        conn = conn_man.open("s3://test")
        s3 = conn.get()
        s3.create_bucket(Bucket='cachetest')
        s3.upload_file('{}/{}'.format(self.cache_loc, self.FID_IN_CACHE.split('/')[-1]),
                        'cachetest',
                        '{}'.format(self.FID_IN_CACHE.split('/')[-1]))
        s3.upload_file('{}/{}'.format(self.cache_loc, self.FID_NOT_IN_CACHE.split('/')[-1]),
                        'cachetest',
                        '{}'.format(self.FID_NOT_IN_CACHE.split('/')[-1]))
        s3.upload_file('{}/{}'.format(self.cache_loc, self.EXTRA_FID_NOT_IN_CACHE.split('/')[-1]),
                        'cachetest',
                        '{}'.format(self.EXTRA_FID_NOT_IN_CACHE.split('/')[-1]))
        os.remove('{}/{}'.format(self.cache_loc, self.FID_NOT_IN_CACHE.split('/')[-1]))
        os.remove('{}/{}'.format(self.cache_loc, self.EXTRA_FID_NOT_IN_CACHE.split('/')[-1]))

    def tearDown(self):
        try:
            self.sl_cache._clear_cache()
        except:
            print("Couldn't clear cache in tearDown?")
        conn_man = slConnectionManager(self.sl_config)
        conn = conn_man.open("s3://test")
        s3 = conn.get()
        s3.delete_object(Bucket='cachetest',
                       Key='{}'.format(self.FID_IN_CACHE.split('/')[-1]))
        s3.delete_object(Bucket='cachetest',
                       Key='{}'.format(self.FID_NOT_IN_CACHE.split('/')[-1]))
        s3.delete_object(Bucket='cachetest',
                       Key='{}'.format(self.EXTRA_FID_NOT_IN_CACHE.split('/')[-1]))
        s3.delete_bucket(Bucket='cachetest')


    def test_space_in_cache(self):
        self.assertTrue(self.sl_cache._space_in_cache(60*10**6))
        self.assertFalse(self.sl_cache._space_in_cache(100*10**6))

    def test_remove_oldest(self):
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(),60*10**6)
        self.sl_cache._write_to_cache(self.FID_NOT_IN_CACHE,test=True,file_size=30*10**6)
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(), 90 * 10 ** 6)
        self.sl_cache._remove_oldest(90*10**6)
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(),30*10**6)

    def test_write_to_cache(self):
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(), 60 * 10 ** 6)
        self.sl_cache._write_to_cache(self.FID_NOT_IN_CACHE, test=True, file_size=30 * 10 ** 6)
        fid = '{}/{}'.format(self.cache_loc, self.FID_NOT_IN_CACHE.split('/')[-1])
        f = open(fid,'w')
        f.close()
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(), 90 * 10 ** 6)
        self.sl_cache._write_to_cache(self.EXTRA_FID_NOT_IN_CACHE,test=True, file_size=30*10**6)
        f = open('{}/{}'.format(self.cache_loc, self.FID_NOT_IN_CACHE.split('/')[-1]))
        f.close()
        self.assertEqual(self.sl_cache.DB.get_total_cache_size(), 120 * 10 ** 6)

    def test_get_fid_in_cache(self):
        cache_loc = self.sl_cache.open(self.FID_IN_CACHE,'r',test=True)
        with open(cache_loc,'r') as f:
            self.assertEqual(f.read(),'testwrite')

    def test_get_fid_not_in_cache(self):
        cache_loc = self.sl_cache.open(self.FID_NOT_IN_CACHE,'r',test=True)
        with open(cache_loc, 'r') as f:
            self.assertEqual(f.read(), 'testwrite')

    def test_fid_from_cache_a(self):
        # open new file as append
        self.sl_cache.open(self.FID_IN_CACHE,access_type='a')
        self.sl_cache.close(self.FID_IN_CACHE,'a')

    def test_fid_from_cache_r(self):
        # open new file as append
        self.sl_cache.open(self.FID_IN_CACHE,access_type='r')
        self.sl_cache.close(self.FID_IN_CACHE,'r')

    def test_fid_from_cache_w(self):
        # open new file as append
        self.sl_cache.open(self.FID_IN_CACHE,access_type='w')
        self.sl_cache.close(self.FID_IN_CACHE,'w')

    # I've removed this test for now because I don't think it is sensible:  the cache open won't create a file, so
    # what am I checking for??
    # def test_put_fid_not_from_cache(self):
    #     fid = 's3://test/newtestbucket/filehead/filesub.nc'
    #     nc = self.sl_cache.open(fid,access_type='w',test=True)
    #     try:
    #         self.sl_cache.close(fid, 'w', test=True)
    #     except FileNotFoundError:
    #         pass
    #     nc = self.sl_cache.open(fid, access_type='r', test=True)
    #     self.sl_cache.close(fid, 'w', test=True)
    #     conn_man = slConnectionManager(self.sl_config)
    #     conn = conn_man.open("s3://test")
    #     s3 = conn.get()
    #     s3.delete_object(Bucket='newtestbucket',
    #                      Key='filehead/filesub.nc')
    #     s3.delete_bucket(Bucket='newtestbucket')


    def test_read_fail(self):
        # read should fail when file doesn't exist
        try:
            fid = 's3://test/newtestbucket/filehead/filesub.nc'
            nc = self.sl_cache.open(fid, access_type='r', test=True)
            read = True
        except ValueError:
            read = False
        self.assertFalse(read)
        # append should fail when file doesn't exist
        try:
            fid = 's3://test/newtestbucket/filehead/filesub.nc'
            nc = self.sl_cache.open(fid, access_type='a', test=True)
            read = True
        except ValueError:
            read = False
        self.assertFalse(read)

    def test_remove_file(self):
        self.sl_cache._remove_file(self.FID_IN_CACHE)
        try:
            f = open('{}/{}'.format(self.cache_loc, self.FID_IN_CACHE.split('/')[-1]),'r')
            opened_file = True
        except IOError:
            opened_file = False
        self.assertFalse(opened_file)

    def test_clear_cache(self):
        cache_loc1 = self.sl_cache.open(self.FID_IN_CACHE,'r',test=True)
        cache_loc2 = self.sl_cache.open(self.FID_NOT_IN_CACHE,'w',test=True)
        cache_loc3 = self.sl_cache.open(self.EXTRA_FID_NOT_IN_CACHE,'w',test=True)
        self.assertTrue(cache_loc1)
        self.assertTrue(cache_loc2)
        self.assertTrue(cache_loc3)
        self.sl_cache._clear_cache()
        #self.assertTrue(self.sl_cache.check_db_empty())
        if self.cache_loc[-1] != '/':
            self.assertEqual(glob.glob('{}/test*'.format(self.cache_loc)),[])
        else:
            self.assertEqual(glob.glob('{}test*'.format(self.cache_loc)),[])


class TestCacheManagerUtils(unittest.TestCase):

    def setUp(self):
        self.sl_cache = slCacheManager()
        self.sl_config = slConfig()
        self.cache_loc = self.sl_config['cache']['location']
        with open('{}test'.format(self.cache_loc),'w') as f:
            f.write('test')
    def tearDown(self):
        os.remove('{}test'.format(self.cache_loc))

    def test_posix_check_file_exists(self):
        self.assertTrue(self.sl_cache._check_whether_posix('{}test'.format(self.cache_loc),'r'))
    def test_posix_check_file_not_exists(self):
        self.assertFalse(self.sl_cache._check_whether_posix('{}testfail'.format(self.cache_loc),'r'))
    def test_posix_check_path_exists(self):
        self.assertTrue(self.sl_cache._check_whether_posix('{}'.format(self.cache_loc),'w'))
    def test_posix_check_path_not_exists(self):
        self.assertFalse(self.sl_cache._check_whether_posix('fail','w'))
    def test_posix_check_alias_not_exists(self):
        self.assertFalse(self.sl_cache._check_whether_posix('s3://munuom/test','r'))
    def test_posix_check_alias_exists(self):
        self.assertTrue(self.sl_cache._check_whether_posix('s3://test/test','r'))

# class TestWithS3_realdata(unittest.TestCase):
#     def setUp(self):
#         pass
#
#     def tearDown(self):
#         pass
#
#     def test_get(self):
#         pass
#
#     def test_put(self):
#         pass

if __name__ == '__main__':
    unittest.main()