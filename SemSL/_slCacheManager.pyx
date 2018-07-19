"""
Cache/staging management for SemSL.

Requested files from backend are stored in a disk caching area defined in the user's config file. Object requests check
the disk cache before, going to the backend, and if the file exists in cache the path is provided, otherwise the request
is sent to the backend. Once the cache is full, the least recently accessed files are removed in order to make space.
"""

import lmdb
import os
import time
from SemSL._slConfigManager import slConfig

from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_nest as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_obj as slCacheDB
#from SemSL._slCacheDB import slCacheDB_sql as slCacheDB


class slCacheManager(slCacheDB):

    def __init__(self):
        # open db from cache location
        slCacheDB.__init__(self)
        self.cache_loc =  self.sl_config['cache']['location']

    def _space_in_cache(self,size):
        ''' Calculates whether there is space in the cache for the new file.
            size is the size of the file which wants to be added to cache
            returns true if space
        '''
        current_cache_used = self.get_total_cache_size()
        cache_size = self.sl_config['cache']['cache_size']
        if size+current_cache_used <= cache_size:
            return True
        else:
            return False


    def _remove_oldest(self,size): # rename
        ''' Remove the oldest files from the cache giving at least space for
            the required size.
            size is the required amount of space for the new file
        '''
        while not self._space_in_cache(size):
            rem_id = self.get_least_recent()
            self.remove_entry(rem_id)
            self._remove_file(rem_id)

    def _write_to_cache(self,fid,test=False,file_size=None):
        if not self.check_cache(fid):
            # get file from backend
            if not test:
                print 'WARNING: backend not implemented, using dummy file'

            # Need to get the size of the file
            if test:
                if file_size == None:
                    file_size = 90*10**6
            else:
                # need to caculate or query the files size
                raise NotImplementedError
            # remove oldest cached files if need be
            self._remove_oldest(file_size)
            # save to cache
            if test:
                with open(self.cache_loc+'/'+fid.split('/')[-1],'w') as f:
                    f.write('testwrite')
            else:
                # get the files from the backend into the cache area
                raise NotImplementedError
            # update cachedb
            # set access and creation time, and filesize
            self.add_entry(fid,file_size)

        else:
            return 0

    def get(self,fid,test=False,file_size=None):
        ''' Retrieve the required filepath for the file from the cache, if the file doesn't exist in cache then pull it
            into cache and update the database.

        :param fid: the id of the file
        :param data: can pass the data straight from memory?
        :param test: whether the method is being called from a test, then creates dummy file
        :return: the path to the file in cache
        '''

        if self.check_cache(fid):
            # if the file exists in cache the access time needs updating
            self.update_access_time(fid)
            return self.get_cache_loc(fid)
        else:
            if test:
                if not file_size == None:
                    self._write_to_cache(fid,test=True,file_size=file_size)
                else:
                    self._write_to_cache(fid,test=True)
            else:
                raise NotImplementedError
            return self.get_cache_loc(fid)

    def put(self,fid,data=None,test=False,file_size=None):
        ''' Uploads the file from cache, or directly to the backend, if not in cache, will save to cache

        :param fid:
        :param data:
        :param test:
        :param file_size:
        :return: 0 on success
        '''

        if not data:
            # upload to backend, bypass checking if file is in cache
            if self.check_cache(fid):
                floc = self.get_cache_loc(fid)
                # take file and stick in backend
                if test:
                    pass
                else:
                    raise NotImplementedError

            else:
                raise ValueError, 'We have nothing to put into backend?'
        elif data:
            #save data to backend
            raise NotImplementedError

        # add file to cache
        if not self.check_cache(fid):
            if test:
                if not file_size == None:
                    self._write_to_cache(fid,test=True,file_size=file_size)
                else:
                    self._write_to_cache(fid,test=True)
            else:
                raise NotImplementedError
        return 0

    def _remove_file(self,fid,silent=False):
        try:
            if self.cache_loc[-1] != '/':
                path = '{}/{}'.format(self.cache_loc, fid.split('/')[-1])
            else:
                path = '{}{}'.format(self.cache_loc, fid.split('/')[-1])
            os.remove(path.encode())
        except OSError as e:
            if not silent:
                print('OSError for file not existing? : {}'.format(fid))


    def _clear_cache(self):
        ''' Removes all data from the cache and database, then removes database

        :return: 0 on successful removal
        '''
        file_list = self.get_all_fids()
        for fid in file_list:
            self._remove_file(fid)
            self.remove_entry(fid)
        self.close_db()
        if self.cache_loc[-1] != '/':
            try:
                path = '{}/semslcachedb'.format(self.cache_loc)
                os.remove(path.encode())
            except OSError:
                path = 'rm -r {}'.format('{}/semslcachedb'.format(self.cache_loc))
                os.system(path.encode())
        else:
            try:
                path = '{}semslcachedb'.format(self.cache_loc)
                os.remove(path.encode())
            except OSError:
                path = 'rm -r {}'.format('{}semslcachedb'.format(self.cache_loc))
                os.system(path.encode())
        return 0


