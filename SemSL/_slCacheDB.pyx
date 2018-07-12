import lmdb
import sqlite3
import os
import time
import sys
import numpy as np
import cPickle as pickle
from SemSL._slConfigManager import slConfig


# We need a database interface to not use lmdb potentially without a refactor
class slCacheDB(object):

    def __init__(self):
        # initilise the 'database'
        # load if there currently exists a copy on disk
        pass

    def check_cache(self,fid):
        # checks whether file is in db, if it is returns true
        raise NotImplementedError

    def update_access_time(self,fid):
        # updates the last cache accessed time
        raise NotImplementedError

    def remove_entry(self,fid):
        # removes an entry in the db defined by the file id key
        raise NotImplementedError

    def add_entry(self,fid,size=0):
        # adds an entry to the db defined by the file id
        # also adds the required fields
        raise NotImplementedError

    def remove_db(self):
        # removes the db from disk permanently
        raise NotImplementedError

    def check_db_empty(self):
        # checks that the database is empty
        raise NotImplementedError

    def get_entry(self,fid):
        # returns the whole entry given by the file id
        raise NotImplementedError

    def get_fid(self,fid):
        # returns the file id from the specific entry?
        raise NotImplementedError

    def get_cache_loc(self,fid):
        # returns the location of the file in cache
        raise NotImplementedError

    def get_access_time(self,fid):
        # returns the last accessed time for the cache file
        raise NotImplementedError

    def get_creation_time(self,fid):
        # returns the creation time for the cache file
        raise NotImplementedError

    def get_total_cache_size(self):
        # returns the total size of the files in cache
        raise NotImplementedError

    def get_least_recent(self):
        # returns the least recently accessed files, returns fid
        raise NotImplementedError

    def get_all_fids(self):
        # returns a list off all the file ids in the database
        raise NotImplementedError

class __slDBObject(object):
    '''
    Base object for storing cache fileobject data in lmdb
    '''
    def __init__(self,fid,cacheloc):
        self._fid = fid # aka object path
        self._cacheloc = cacheloc+'/'+fid.split('/')[-1]

    def setAccessTime(self):
        self._cachetimeaccessed = time.time()
    def setCreateTime(self):
        self._cachecreated = time.time()
    def setFileSize(self,size):
        self._filesize = size

class slCacheDB_lmdb(slCacheDB):
    def __init__(self):

        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.cdb = lmdb.open(self.cache_loc+'/semslcachedb',map_size=10*10485760)
        else:
            self.cdb = lmdb.open(self.cache_loc+'semslcachedb',map_size=10*10485760)


    def add_entry(self,fid,size=0):

        with self.cdb.begin(write=True) as txn:
           datum = __slDBObject(fid,self.cache_loc)
           datum.setCreateTime()
           datum.setAccessTime()
           datum.setFileSize(size)
           txn.put(fid.encode('ascii'),pickle.dumps(datum))

    def remove_entry(self,fid):
        with self.cdb.begin(write=True) as txn:
            txn.delete(fid)

    def update_access_time(self,fid):
        with self.cdb.begin(write=True) as txn:
            value = txn.get(fid)
            datum = pickle.loads(value)
            datum.setAccessTime()
            txn.delete(fid)
            txn.put(fid.encode('ascii'), pickle.dumps(datum))

    def check_cache(self,fid):
        with self.cdb.begin() as txn:
            value = txn.get(fid)
            if not value == None:
                return True
            else:
                return False

    def check_db_empty(self):
        env = self.cdb
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                if key == None:
                    continue
                else:
                    return False
            return True

    def get_entry(self,fid):
        env = self.cdb
        with env.begin() as txn:
            value = txn.get(fid)
            try:
                slDB = pickle.loads(value)
                return slDB
            except TypeError:
                return None

    def get_fid(self,fid):
        dbobj = self.get_entry(fid)
        return dbobj._fid

    def get_cache_loc(self,fid):
        dbobj = self.get_entry(fid)
        return dbobj._cacheloc

    def get_access_time(self,fid):
        dbobj = self.get_entry(fid)
        return dbobj._cachetimeaccessed

    def get_creation_time(self,fid):
        dbobj = self.get_entry(fid)
        return dbobj._cachecreated

    def get_total_cache_size(self):
        env = self.cdb
        size_tot = 0
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                dbobj = pickle.loads(value)
                size_tot += dbobj._filesize
        return size_tot

    def get_least_recent(self):
        env = self.cdb
        least_recent = None
        access_time = sys.float_info.max
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                dbobj = pickle.loads(value)
                obj_accessed = dbobj._cachetimeaccessed
                if obj_accessed < access_time:
                    least_recent = key
                    access_time = obj_accessed
        return least_recent

    def get_all_fids(self):
        env = self.cdb
        file_list = []
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                file_list.append(key)
        return file_list

    def close_db(self):
        self.cdb.close()

# We need a database interface to not use lmdb potentially without a refactor
class slCacheDB_sql(object):

    def __init__(self):
        # initilise the 'database'
        # load if there currently exists a copy on disk
        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.db = sqlite3.connect(self.cache_loc+'/semslcachedb')
        else:
            self.db = sqlite3.connect(self.cache_loc+'semslcachedb')
        cursor = self.db.cursor()
        try:
            cursor.execute('''
                CREATE TABLE cache(fid TEXT PRIMARY KEY, 
                                  create_time REAL,
                                  access_time REAL,
                                  cache_loc TEXT,
                                  file_size REAL)
                ''')
        except sqlite3.OperationalError as e:
            pass
        self.db.commit()

    def check_cache(self,fid):
        # checks whether file is in db, if it is returns true
        cursor = self.db.cursor()
        cursor.execute(''' SELECT fid FROM cache WHERE fid = ?''',(fid,))
        if not cursor.fetchone() == None:
            return True
        else:
            return False

    def get_fid(self,fid):
        # returns the original file destination as a string
        cursor = self.db.cursor()
        cursor.execute('''SELECT fid FROM cache WHERE fid = ?''',(fid,))
        selected = cursor.fetchall()
        assert len(selected) == 1
        return selected[0][0]

    def get_cache_loc(self,fid):
        # returns the cache location for the file
        cursor = self.db.cursor()
        cursor.execute('''SELECT cache_loc FROM cache WHERE fid = ?''',(fid,))
        selected = cursor.fetchall()
        assert len(selected) == 1
        return selected[0][0]

    def get_creation_time(self,fid):
        # returns the creation time of the cache file
        cursor = self.db.cursor()
        cursor.execute('''SELECT create_time FROM cache WHERE fid = ?''',(fid,))
        selected = cursor.fetchall()
        assert len(selected) == 1
        return selected[0][0]

    def get_access_time(self,fid):
        # returns the last accessed time of the cache file
        cursor = self.db.cursor()
        cursor.execute('''SELECT access_time FROM cache WHERE fid = ?''',(fid,))
        selected = cursor.fetchall()
        assert len(selected) == 1
        return selected[0][0]

    def update_access_time(self,fid):
        # updates the last cache accessed time
        cursor = self.db.cursor()
        cursor.execute('''UPDATE cache SET access_time = ? WHERE fid = ?''',(time.time(),fid))

    def remove_entry(self,fid):
        # removes an entry in the db defined by the file id key
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM cache WHERE fid = ?''',(fid,))
        self.db.commit()

    def add_entry(self,fid,size=0):
        # adds an entry to the db defined by the file id
        # also adds the required fields
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO cache(fid, create_time, access_time, cache_loc, file_size)
                          VALUES(?,?,?,?,?)''', (fid,time.time(),time.time(),self.cache_loc+'/'+fid.split('/')[-1],size))
        self.db.commit()

    def remove_db(self):
        # removes the db from disk permanently
        cursor = self.db.cursor()
        cursor.execute('''DROP TABLE cache''')
        self.db.commit()

    def check_db_empty(self):
        # checks that the database is empty
        cursor = self.db.cursor()
        cursor.execute('''SELECT fid FROM cache''')
        entry =  cursor.fetchone()
        if entry == None:
            return True
        else:
            return False

    def get_entry(self,fid):
        # returns the whole entry given by the file id
        cursor = self.db.cursor()
        cursor.execute('''SELECT fid, create_time, access_time, cache_loc, file_size 
                          FROM cache WHERE fid = ?''',(fid,))
        return cursor.fetchone()

    def get_total_cache_size(self):
        # returns the total size of the files in cache
        cursor = self.db.cursor()
        cursor.execute('''SELECT file_size from cache''')
        selected = cursor.fetchall()
        total_size = np.sum(selected)
        return total_size

    def get_least_recent(self):
        # returns the least recently accessed files, returns fid
        cursor = self.db.cursor()
        cursor.execute('''SELECT fid FROM cache WHERE access_time = (SELECT min(access_time) FROM cache)''')
        selected = cursor.fetchall()
        assert len(selected) == 1
        return selected[0][0]

    def get_all_fids(self):
        # returns a list off all the file ids in the database
        cursor = self.db.cursor()
        cursor.execute('''SELECT fid FROM cache''')
        all_fids = [str(x[0]) for x in cursor.fetchall()]

        return all_fids

    def close_db(self):
        self.db.close()
        