from SemSL._slCacheManager import slCacheManager
from SemSL._slConfigManager import slConfig
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
        self.FID_IN_CACHE = 's3store/{}'.format(FID_IN_CACHE)
        self.FID_NOT_IN_CACHE = 's3store/{}'.format(FID_NOT_IN_CACHE)
        self.EXTRA_FID_NOT_IN_CACHE = 's3store/{}'.format(EXTRA_FID_NOT_IN_CACHE)
        self.sl_cache.add_entry(self.FID_IN_CACHE,size=60*10**6)
        self.sl_config = slConfig()

    def tearDown(self):
        self.sl_cache._clear_cache()

    def test_read_existing_DB(self):
        db_value = self.sl_cache.get_entry(self.FID_IN_CACHE)
        self.assertTrue(db_value)

    def test_read_nonexistant(self):
        db_value = self.sl_cache.get_entry(self.FID_NOT_IN_CACHE)
        self.assertEqual(db_value,None)

    def test_return_fid(self):
        self.assertEqual(self.sl_cache.get_fid(self.FID_IN_CACHE), self.FID_IN_CACHE)

    def test_return_creation_time(self):
        self.assertEqual(type(self.sl_cache.get_creation_time(self.FID_IN_CACHE)),datetime.datetime)

    def test_return_access_time(self):
        self.assertEqual(type(self.sl_cache.get_access_time(self.FID_IN_CACHE)),datetime.datetime)

    def test_get_path_from_cache(self):
        if self.sl_config['cache']['location'][-1] == '/':
            path = '{}{}'.format(self.sl_config['cache']['location'],self.FID_IN_CACHE.split('/')[-1])
        else:
            path = '{}/{}'.format(self.sl_config['cache']['location'],self.FID_IN_CACHE.split('/')[-1])

        self.assertEqual(self.sl_cache.get_cache_loc(self.FID_IN_CACHE),path)

    def test_update_access_time(self):
        time.sleep(1)
        self.sl_cache.update_access_time(self.FID_IN_CACHE)
        td = self.sl_cache.get_access_time(self.FID_IN_CACHE)-self.sl_cache.get_creation_time(self.FID_IN_CACHE)
        self.assertGreaterEqual(td.total_seconds(),1)

    def test_check_cache(self):
        self.assertTrue(self.sl_cache.check_cache(self.FID_IN_CACHE))
        self.assertFalse(self.sl_cache.check_cache(self.FID_NOT_IN_CACHE))

    def test_add_to_DB(self):
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE)
        db_value = self.sl_cache.get_entry(self.FID_NOT_IN_CACHE)
        self.assertTrue(db_value)

    def test_get_total_size(self):
        self.assertEqual(self.sl_cache.get_total_cache_size(),60*10**6)
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE,60*10**6)
        self.assertEqual(self.sl_cache.get_total_cache_size(),120*10**6)

    def test_get_least_recent(self):
        # add new files and return original
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE,0)
        self.assertEqual(self.sl_cache.get_least_recent(),self.FID_IN_CACHE)
        # update original file timestamp and return original
        self.sl_cache.update_access_time(self.FID_IN_CACHE)
        self.assertEqual(self.sl_cache.get_least_recent(),self.FID_NOT_IN_CACHE)

    def test_get_all_fids(self):
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE)
        self.sl_cache.add_entry(self.EXTRA_FID_NOT_IN_CACHE)
        all_fids = self.sl_cache.get_all_fids()
        all_fids = list(all_fids)
        self.assertEqual(collections.Counter(all_fids),collections.Counter(['s3store/testextranotincache', 's3store/testincache', 's3store/testnotincache']))

    def test_get_fid(self):
        self.assertEqual(self.sl_cache.get_fid(self.FID_IN_CACHE),self.FID_IN_CACHE)


class TestCacheManager(unittest.TestCase):
    # Test ability to take a file which isn't currently in the cache and put it in there then return the file path
    def setUp(self):
        self.sl_cache = slCacheManager()
        # add example file to db
        self.FID_IN_CACHE = 's3store/{}'.format(FID_IN_CACHE)
        self.FID_NOT_IN_CACHE = 's3store/{}'.format(FID_NOT_IN_CACHE)
        self.EXTRA_FID_NOT_IN_CACHE = 's3store/{}'.format(EXTRA_FID_NOT_IN_CACHE)
        self.sl_cache.add_entry(self.FID_IN_CACHE,size=60*10**6)
        self.sl_config = slConfig()
        self.cache_loc = self.sl_config['cache']['location']
        with open('{}/{}'.format(self.cache_loc, self.FID_IN_CACHE.split('/')[-1]), 'w') as f:
            f.write('testwrite')

    def tearDown(self):
        try:
            self.sl_cache._clear_cache()
        except:
            print("Couldn't clear cache in tearDown?")
        # self.sl_cache.remove_entry(self.FID_IN_CACHE)
        # self.sl_cache.remove_entry(self.FID_NOT_IN_CACHE)
        # self.sl_cache.remove_entry(self.EXTRA_FID_NOT_IN_CACHE)
        #self.assertTrue(self.sl_cache.check_db_empty())
        # self.sl_cache._remove_file(self.FID_IN_CACHE,silent=True)
        # self.sl_cache._remove_file(self.FID_NOT_IN_CACHE,silent=True)
        # self.sl_cache._remove_file(self.EXTRA_FID_NOT_IN_CACHE,silent=True)


    def test_space_in_cache(self):
        self.assertTrue(self.sl_cache._space_in_cache(60*10**6))
        self.assertFalse(self.sl_cache._space_in_cache(100*10**6))

    def test_remove_oldest(self):
        self.assertEqual(self.sl_cache.get_total_cache_size(),60*10**6)
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE,30*10**6)
        self.assertEqual(self.sl_cache.get_total_cache_size(), 90 * 10 ** 6)
        self.sl_cache._remove_oldest(90*10**6)
        self.assertEqual(self.sl_cache.get_total_cache_size(),30*10**6)

    def test_write_to_cache(self):
        self.assertEqual(self.sl_cache.get_total_cache_size(), 60 * 10 ** 6)
        self.sl_cache.add_entry(self.FID_NOT_IN_CACHE, 30 * 10 ** 6)
        self.assertEqual(self.sl_cache.get_total_cache_size(), 90 * 10 ** 6)
        self.sl_cache._write_to_cache(self.EXTRA_FID_NOT_IN_CACHE,test=True)
        self.assertEqual(self.sl_cache.get_total_cache_size(), 120 * 10 ** 6)

    def test_get_fid_in_cache(self):
        cache_loc = self.sl_cache.get(self.FID_IN_CACHE)
        with open(cache_loc,'r') as f:
            self.assertEqual(f.read(),'testwrite')

    def test_get_fid_not_in_cache(self):
        cache_loc = self.sl_cache.get(self.FID_NOT_IN_CACHE,test=True)
        with open(cache_loc, 'r') as f:
            self.assertEqual(f.read(), 'testwrite')

    def test_put_fid_from_cache(self):
        cache_loc = self.sl_cache.put(self.FID_IN_CACHE)
        with open(cache_loc,'r') as f:
            self.assertEqual(f.read(),'testwrite')

    def test_put_fid_not_from_cache(self):
        cache_loc = self.sl_cache.put(self.FID_NOT_IN_CACHE,test=True)
        with open(cache_loc, 'r') as f:
            self.assertEqual(f.read(), 'testwrite')

    def test_remove_file(self):
        self.sl_cache._remove_file(self.FID_IN_CACHE)
        try:
            f = open('{}/{}'.format(self.cache_loc, self.FID_IN_CACHE.split('/')[-1]),'r')
            opened_file = True
        except IOError:
            opened_file = False
        self.assertFalse(opened_file)

    def test_clear_cache(self):
        cache_loc1 = self.sl_cache.get(self.FID_IN_CACHE,test=True)
        cache_loc2 = self.sl_cache.get(self.FID_NOT_IN_CACHE,test=True)
        cache_loc3 = self.sl_cache.get(self.EXTRA_FID_NOT_IN_CACHE,test=True)
        self.assertTrue(cache_loc1)
        self.assertTrue(cache_loc2)
        self.assertTrue(cache_loc3)
        self.sl_cache._clear_cache()
        #self.assertTrue(self.sl_cache.check_db_empty())
        if self.cache_loc[-1] != '/':
            self.assertEqual(glob.glob('{}/test*'.format(self.cache_loc)),[])
        else:
            self.assertEqual(glob.glob('{}test*'.format(self.cache_loc)),[])


class TestWithS3(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get(self):
        pass

    def test_put(self):
        pass

if __name__ == '__main__':
    unittest.main()