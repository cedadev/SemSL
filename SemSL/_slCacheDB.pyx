import lmdb
import sqlite3
import os
import time
import sys
import numpy as np
import pickle
import collections
import datetime
from SemSL._slConfigManager import slConfig
import SemSL._slUtils as slU


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
        if not cacheloc[-1] == '/':
            self._cacheloc = '{}/{}'.format(cacheloc,fid.split('/')[-1])
        else:
            self._cacheloc = '{}{}'.format(cacheloc,fid.split('/')[-1])

    def setAccessTime(self):
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        self._cachetimeaccessed = sDate
    def setCreateTime(self):
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        self._cachecreated = sDate
    def setFileSize(self,size):
        self._filesize = size

class slCacheDB_lmdb_obj(slCacheDB):
    def __init__(self):
        MAP_SIZE = 10*2**20
        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.cdb = lmdb.open('{}/semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE)
        else:
            self.cdb = lmdb.open('{}semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE)


    def add_entry(self,fid,size=0):

        with self.cdb.begin(write=True) as txn:
           datum = __slDBObject(fid,self.cache_loc)
           datum.setCreateTime()
           datum.setAccessTime()
           datum.setFileSize(size)
           txn.put(fid.encode(),pickle.dumps(datum))

    def remove_entry(self,fid):
        with self.cdb.begin(write=True) as txn:
            txn.delete(fid.encode())

    def update_access_time(self,fid):
        with self.cdb.begin(write=True) as txn:
            value = txn.get(fid.encode())
            datum = pickle.loads(value)
            datum.setAccessTime()
            txn.delete(fid.encode())
            txn.put(fid.encode('utf-8'), pickle.dumps(datum))

    def check_cache(self,fid):
        with self.cdb.begin() as txn:
            value = txn.get(fid.encode())
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
            value = txn.get(fid.encode())
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
        sDate = dbobj._cachetimeaccessed
        return datetime.datetime.strptime(sDate, '%Y-%m-%d %H:%M:%S.%f')

    def get_creation_time(self,fid):
        dbobj = self.get_entry(fid)
        sDate = dbobj._cachecreated
        return datetime.datetime.strptime(sDate, '%Y-%m-%d %H:%M:%S.%f')

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
        access_time = datetime.datetime.now()
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                dbobj = pickle.loads(value)
                obj_accessed = datetime.datetime.strptime(dbobj._cachetimeaccessed, '%Y-%m-%d %H:%M:%S.%f')

                if obj_accessed < access_time:
                    least_recent = key.decode()
                    access_time = obj_accessed
        return least_recent

    def get_all_fids(self):
        env = self.cdb
        file_list = collections.deque()
        with env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                file_list.append(key.decode())
        return file_list

    def close_db(self):
        self.cdb.close()


class slCacheDB_lmdb_nest(slCacheDB):
    def __init__(self):
        MAP_SIZE = 10*2**20
        MAX_DBS = 2**20
        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.env = lmdb.open('{}/semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE,max_dbs=MAX_DBS)
        else:
            self.env = lmdb.open('{}semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE,max_dbs=MAX_DBS)


    def add_entry(self,_fid,size=0):
        # need to convert to bytes string
        fid = _fid.encode()
        # check that it doesn't already exist
        try:
            self.env.open_db(fid,write=False)
            raise ValueError('Child database already exists!')
        except TypeError:
            pass
        # add the child db path into the env db
        child_db = self.env.open_db(fid)
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor(child_db)
            cursor.put(b'fid',fid)
            cursor.put(b'create_time', sDate.encode())#change to dt
            cursor.put(b'access_time', sDate.encode())
            cursor.put(b'file_size', str(size).encode())
            cache_loc = '{}/{}'.format(self.cache_loc,fid.decode().split('/')[-1])
            cursor.put(b'cache_loc', cache_loc.encode())

    def remove_entry(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin(write=True) as txn:
            txn.drop(child_db)
            txn.delete(fid)

    def update_access_time(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin(write=True) as txn:
            now = datetime.datetime.now()
            sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
            cursor = txn.cursor(child_db)
            cursor.put(b'access_time', sDate.encode())

    def check_cache(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        with self.env.begin() as txn:
            value = txn.get(fid)
            if not value == None:
                return True
            else:
                return False

    def check_db_empty(self):
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                if key == None:
                    continue
                else:
                    return False
            return True

    def get_entry(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        try:
            child_db = self.env.open_db(fid,create=False)
            with self.env.begin() as txn:
                cursor = txn.cursor(child_db)
                return cursor
        except lmdb.NotFoundError:
            return None

    def get_fid(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin() as txn:
            cursor = txn.cursor(child_db)
            return cursor.get(b'fid').decode()

    def get_cache_loc(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin() as txn:
            cursor = txn.cursor(child_db)
            return cursor.get(b'cache_loc').decode()

    def get_access_time(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin() as txn:
            cursor = txn.cursor(child_db)
            sDate = cursor.get(b'access_time')
            oDate = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
            return oDate

    def get_creation_time(self,_fid):
        # need to convert to bytes string
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin() as txn:
            cursor = txn.cursor(child_db)
            sDate = cursor.get(b'create_time')
            oDate = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
            return oDate

    def get_file_size(self,_fid):
        fid = _fid.encode()
        child_db = self.env.open_db(fid)
        with self.env.begin() as txn:
            cursor = txn.cursor(child_db)
            return float(cursor.get(b'file_size').decode())

    def get_total_cache_size(self):
        size_tot = 0
        fids = self.get_all_fids()
        with self.env.begin() as txn:
            for fid in fids:
                size_tot += self.get_file_size(fid)
        return size_tot

    def get_least_recent(self):
        least_recent = None
        access_time = datetime.datetime.now()
        fids = self.get_all_fids()
        for fid in fids:
            child_db = self.env.open_db(fid.encode())
            with self.env.begin() as txn:
                cursor = txn.cursor(child_db)
                sDate = cursor.get(b'access_time')
                obj_accessed = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
                if obj_accessed < access_time:
                    least_recent = fid
                    access_time = obj_accessed
        return least_recent

    def get_all_fids(self):
        file_list = collections.deque()
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                file_list.append(key.decode())
        return file_list

    def close_db(self):
        self.env.close()


class slCacheDB_lmdb(slCacheDB):
    def __init__(self):
        MAP_SIZE = 10*2**20
        MAX_DBS = 0
        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.env = lmdb.open('{}/semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE)
        else:
            self.env = lmdb.open('{}semslcachedb'.format(self.cache_loc),map_size=MAP_SIZE)


    def add_entry(self,fid,size=0):
        # need to convert to bytes string
        #print('FID IN CACHE ADD: {}'.format(fid))
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor()
            key_ct = '{}_create_time'.format(fid)
            key_at = '{}_access_time'.format(fid)
            key_fs = '{}_file_size'.format(fid)
            key_cl = '{}_cache_loc'.format(fid)
            cursor.put(key_ct.encode(), sDate.encode())
            cursor.put(key_at.encode(), sDate.encode())
            cursor.put(key_fs.encode(), str(size).encode())

            # remove alias and bucket from fid (can't assume last element in path is whole filepath)
            bucket = slU._get_bucket(fid)
            alias = slU._get_alias(fid)
            fpath =  fid.replace(bucket,'')
            fpath = fpath.replace(alias,'')
            while fpath[0] == '/':
                fpath = fpath[1:]
            if self.cache_loc[-1] != '/':
                cache_loc = '{}/{}'.format(self.cache_loc, fpath)
            else:
                cache_loc = '{}{}'.format(self.cache_loc, fpath)
            cursor.put(key_cl.encode(), cache_loc.encode())

    def rename_entry(self,oldnames,newnames):
        # get size of old entry
        #print(self.get_all_fids())
        for i in range(len(oldnames)):
            #print('new name {}'.format(newnames[i]))
            #print('old name {}'.format(oldnames[i]))
            filesize = self.get_file_size(oldnames[i])
            # add new entry

            self.add_entry(newnames[i])#,filesize)
            # delete old entry
            self.remove_entry(oldnames[i])

    def remove_entry(self,fid):
        # need to convert to bytes string
        key_ct = '{}_create_time'.format(fid)
        key_at = '{}_access_time'.format(fid)
        key_fs = '{}_file_size'.format(fid)
        key_cl = '{}_cache_loc'.format(fid)
        with self.env.begin(write=True) as txn:
            txn.delete(key_cl.encode())
            txn.delete(key_ct.encode())
            txn.delete(key_at.encode())
            txn.delete(key_fs.encode())

    def update_access_time(self,fid):
        # need to convert to bytes string
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor()
            now = datetime.datetime.now()
            sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')

            key_at = '{}_access_time'.format(fid)
            cursor.put(key_at.encode(), sDate.encode())

    def check_cache(self,fid):
        # need to convert to bytes string
        key_cl = '{}_cache_loc'.format(fid)
        with self.env.begin() as txn:
            value = txn.get(key_cl.encode())
            if not value == None:
                return True
            else:
                return False

    def check_db_empty(self):
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                if key == None:
                    continue
                else:
                    return False
            return True

    def get_entry(self,fid):
        # need to convert to bytes string
        key_ct = '{}_create_time'.format(fid)
        key_at = '{}_access_time'.format(fid)
        key_fs = '{}_file_size'.format(fid)
        key_cl = '{}_cache_loc'.format(fid)

        with self.env.begin() as txn:
            cursor = txn.cursor()
            if cursor.get(key_cl.encode()):
                return {'create_time':cursor.get(key_ct.encode()),
                        'access_time':cursor.get(key_at.encode()),
                        'file_size':cursor.get(key_fs.encode()),
                        'cache_loc':cursor.get(key_cl.encode())}
            else:
                return None

    def get_fid(self,fid):
        # need to convert to bytes string
        with self.env.begin() as txn:
            cursor = txn.cursor()
            key_cl = '{}_cache_loc'.format(fid)
            if cursor.get(key_cl.encode()):
                return fid
            else:
                return None

    def get_cache_loc(self,fid):
        # need to convert to bytes string
        with self.env.begin() as txn:
            cursor = txn.cursor()
            key_cl = '{}_cache_loc'.format(fid)
            return cursor.get(key_cl.encode()).decode()

    def get_access_time(self,fid):
        # need to convert to bytes string
        with self.env.begin() as txn:
            cursor = txn.cursor()
            key_at = '{}_access_time'.format(fid)
            sDate = cursor.get(key_at.encode())
            oDate = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
            return oDate

    def get_creation_time(self,fid):
        # need to convert to bytes string
        with self.env.begin() as txn:
            cursor = txn.cursor()
            key_ct = '{}_create_time'.format(fid)
            sDate = cursor.get(key_ct.encode())
            oDate = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
            return oDate

    def get_file_size(self,fid):
        with self.env.begin() as txn:
            cursor = txn.cursor()
            key_at = '{}_file_size'.format(fid)
            return float(cursor.get(key_at.encode()).decode())

    def get_total_cache_size(self):
        size_tot = 0
        fids = self.get_all_fids()
        with self.env.begin() as txn:
            for fid in fids:
                size_tot += self.get_file_size(fid)
        return size_tot

    def get_least_recent(self):
        least_recent = None
        access_time = datetime.datetime.now()
        fids = self.get_all_fids()
        for fid in fids:
            with self.env.begin() as txn:
                cursor = txn.cursor()
                key_ct = '{}_access_time'.format(fid)
                sDate = cursor.get(key_ct.encode())
                obj_accessed = datetime.datetime.strptime(sDate.decode(), '%Y-%m-%d %H:%M:%S.%f')
                if obj_accessed < access_time:
                    least_recent = fid
                    access_time = obj_accessed
        return least_recent

    def get_all_fids(self):
        file_list = collections.deque()
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key,value in cursor:
                removestr = key.decode().split('.nc')[-1]
                fid_key = key.decode().replace(removestr,'')
                if not fid_key in file_list:
                    file_list.append(fid_key)
        return list(set(file_list))

    def close_db(self):
        self.env.close()

# We need a database interface to not use lmdb potentially without a refactor
class slCacheDB_sql(object):

    def __init__(self):
        # initilise the 'database'
        # load if there currently exists a copy on disk
        self.sl_config = slConfig()
        self.cache_loc =  self.sl_config['cache']['location']
        if not self.cache_loc[-1] == '/':
            self.db = sqlite3.connect('{}/semslcachedb'.format(self.cache_loc))
        else:
            self.db = sqlite3.connect('{}semslcachedb'.format(self.cache_loc))
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
        return datetime.datetime.strptime(selected[0][0], '%Y-%m-%d %H:%M:%S.%f')

    def get_access_time(self,fid):
        # returns the last accessed time of the cache file
        cursor = self.db.cursor()
        cursor.execute('''SELECT access_time FROM cache WHERE fid = ?''',(fid,))
        selected = cursor.fetchall()
        assert len(selected) == 1
        return datetime.datetime.strptime(selected[0][0], '%Y-%m-%d %H:%M:%S.%f')

    def update_access_time(self,fid):
        # updates the last cache accessed time
        cursor = self.db.cursor()
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        cursor.execute('''UPDATE cache SET access_time = ? WHERE fid = ?''',(sDate,fid))

    def remove_entry(self,fid):
        # removes an entry in the db defined by the file id key
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM cache WHERE fid = ?''',(fid,))
        self.db.commit()

    def add_entry(self,fid,size=0):
        # adds an entry to the db defined by the file id
        # also adds the required fields
        cursor = self.db.cursor()
        now = datetime.datetime.now()
        sDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        cursor.execute('''INSERT INTO cache(fid, create_time, access_time, cache_loc, file_size)
                          VALUES(?,?,?,?,?)''', (fid,sDate,sDate,self.cache_loc+'/'+fid.split('/')[-1],size))
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
        