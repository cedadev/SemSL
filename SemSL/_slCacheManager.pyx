"""
Cache/staging management for SemSL.

Requested files from backend are stored in a disk caching area defined in the user's config file. Object requests check
the disk cache before, going to the backend, and if the file exists in cache the path is provided, otherwise the request
is sent to the backend. Once the cache is full, the least recently accessed files are removed in order to make space.
"""

import os
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager
import SemSL._slUtils as slU

from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_nest as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_obj as slCacheDB
#from SemSL._slCacheDB import slCacheDB_sql as slCacheDB


class slCacheManager(object):

    def __init__(self):
        # open db from cache location
        self.sl_config = slConfig()
        self.DB = slCacheDB()
        self.cache_loc =  self.sl_config['cache']['location']
        self.access_type = 'r'

    def _return_client(self,fid):

        conn_man = slConnectionManager(self.sl_config)
        alias = slU._get_alias(fid)
        conn = conn_man.open(alias)
        client = conn.get()
        return client

    def _get_fname(self,fid):
        fname = fid[:]
        alias = slU._get_alias(fid)
        bucket = slU._get_bucket(fid)
        fname = fname.replace(alias,'')
        fname = fname.replace(bucket,'')
        #remove preceeding /
        while fname[0] == '/':
            fname = fname[1:]

        return fname

    def _space_in_cache(self,size):
        """ Calculates whether there is space in the cache for the new file.
            size is the size of the file which wants to be added to cache
            returns true if space
        """
        current_cache_used = self.DB.get_total_cache_size()
        cache_size = self.sl_config['cache']['cache_size']
        if size+current_cache_used <= cache_size:
            return True
        else:
            return False

    def _remove_oldest(self,size): # rename
        """ Remove the oldest files from the cache giving at least space for
            the required size.
            size is the required amount of space for the new file
        """
        while not self._space_in_cache(size):
            rem_id = self.DB.get_least_recent()
            self.DB.remove_entry(rem_id)
            self._remove_file(rem_id)

    def _write_to_cache(self,fid,test=False,file_size=None):
        if not self.DB.check_cache(fid):

            bucket = slU._get_bucket(fid)
            client = self._return_client(fid)
            alias = slU._get_alias(fid)
            fname = self._get_fname(fid)
            # get the correct backend for the file
            backend = slU._get_backend(fid)


            # Need to get the size of the file
            if test:
                if file_size is None:
                    file_size = 90*10**6
            else:
                # need to calculate or query the files size
                file_size = backend.get_object_size(client,bucket,fname)
            # remove oldest cached files if need be
            self._remove_oldest(file_size)


            if '/' in fname:
                try:
                    os.makedirs(str.join('',fname.split('/')[:-1]))
                except FileExistsError:
                    pass



            backend.download(client,bucket,fname, self.DB.cache_loc+'/'+fname)

            # update cachedb
            # set access and creation time, and filesize
            self.DB.add_entry(fid,file_size)
        else:
            return 0

    def _upload_from_cache(self,fid,test=False):
        """
        Uploads the required file from cache to the backend. Called from close().
        :param fid: the file name
        :param test: determines whether the call is from a test
        :return:
        """

        # get location of file
        #print('file for upload {}'.format(fid))
        cloc = self.DB.get_cache_loc(fid)
        client = self._return_client(fid)
        bucket = slU._get_bucket(fid)

        # get backend object
        backend = slU._get_backend(fid)

        # create list of buckets to check through
        buckets_dict = backend.list_buckets(client)
        bucket_check = False
        for i in range(len(buckets_dict)):
            if buckets_dict[i]['Name'] == bucket:
                bucket_check = True


        # create bucket if it doen't exist
        if not bucket_check:
            backend.create_bucket(client,bucket)

        fname = self._get_fname(fid)
        backend.upload(client,cloc,bucket,fname)

    def _check_whether_posix(self,fid,access_type):
        # Check whether there is an alias in the file path if not, assume it is a posix filepath and pass back the
        # filepath as the cache path, as there is no need to cp from disk to the caching area
        if slU._get_alias(fid) is None:
            # Check whether is looks like a filepath
            # use os.path.exists for a or r
            if access_type == 'r' or access_type == 'a':
                return os.path.exists(fid)

            elif access_type == 'w':
                if not '/' in fid:
                    return True
                else:
                    return os.path.exists(os.path.dirname(fid))

            return False # returns False if alias not in
        else:
            return 'Alias exists' # return if alias is in list

    def open(self,fid,access_type,diskless=False,test=False,file_size=None):
        """Retrieve the required filepath for the file from the cache, if the file doesn't exist in cache then pull it
            into cache and update the database.

        :param fid: the id of the file
        :param test: whether the method is being called from a test, then creates dummy file
        :return: the path to the file in cache, or data from backend
        """
        self.access_type = access_type
        self.fid = fid
        self.diskless=diskless

        # Check if the file path is probably just a posix path -- then just return the same path
        if self._check_whether_posix(fid,access_type) == 'Alias exists':
            pass
        elif self._check_whether_posix(fid,access_type):
            return fid
        elif not self._check_whether_posix(fid,access_type):
            raise ValueError("Invalid alias, or path doesn't exist")
        else:
            raise TypeError('File ID not valid')


        if self.diskless:
            raise NotImplementedError
            # we need to assert whether the required file can fit into memory before we try
            # this, and if it doesn't, then throw an error?

        if access_type == 'r' or access_type == 'a':
            if self.DB.check_cache(fid):
                # if the file exists in cache the access time needs updating
                self.DB.update_access_time(fid)
                return self.DB.get_cache_loc(fid)
            else:
                if test:
                    self._write_to_cache(fid,test=True,file_size=file_size)
                    self.DB.add_entry(fid)
                else:
                    # retrieve file from backend
                    if not diskless:
                        # write file to cache
                        # a 'download' avoids reading too much into memory and crashing as opposed to a 'read'
                        #file_size= GET FILESIZE
                        self._write_to_cache(fid,file_size=file_size)
                        self.DB.add_entry(fid)
                        #self._write_to_cache(fid,file_size=file_size)
                    else: # diskless
                        # 'get' file, don't write to cache
                        # if too large for memory, throw execption
                        raise NotImplementedError
                return self.DB.get_cache_loc(fid)

        elif access_type == 'w':
            # create a cache entry
            self.DB.add_entry(fid)

            return self.DB.get_cache_loc(fid)

        else:
            raise ValueError('Invalid access type')


    def close(self,fid,mode,subfiles_accessed=[],test=False):
        """ Uploads the file from cache, or directly to the backend, if not in cache, will save to cache
        :param test:
        :return: 0 on success
        """
        # update access db
        self.DB.update_access_time(fid)

        #fid = self.fid
        if mode == 'r':
            pass
        elif mode == 'a' or mode== 'w':
            #if self.diskless:
            #   raise NotImplementedError
            #    # do something
            #else:
            # do something else
            if self._check_whether_posix(fid,mode) == 'Alias exists':
                self._upload_from_cache(fid)
                # upload subfiles
                for sf in subfiles_accessed:
                    subfid = sf#slU._get_alias(fid)+'/'+slU._get_bucket(fid)+'/'+sf
                    #self.DB.add_entry(subfid) removed because not needed?? TODO affirm
                    self._upload_from_cache(subfid)


        else:
            raise ValueError('Access mode not supported')

    def bulk_download(self,file_list):
        # Download all the files in the list in one backend call

        # Get all the aliases in the file_list then do the download for each backend
        aliases = []
        for file in file_list:

            file_alias = slU._get_alias(file)
            if not file_alias in aliases:
                aliases.append(file_alias)


        # Keep track of file size total
        tot_size = 0

        for alias in aliases:
            # build the  list for the backend
            files_in_backend = []
            for file in file_list:
                if alias in file:
                    files_in_backend.append(file)

            bucket = slU._get_bucket(file)
            client = self._return_client(file)
            # get the correct backend for the file
            backend = slU._get_backend(file_list[0])

            # Do bulk download
            for file in file_list:

                fname = self._get_fname(file)
                file_size = backend.get_object_size(client,bucket,fname)
                tot_size+=file_size
                if tot_size > self.sl_config['cache']['cache_size']:
                    raise ValueError("When updating subfile metadata the subfile's total size exceeded the"
                                     "size of the cache")
            # remove oldest cached files if need be
                self._remove_oldest(file_size)

            if '/' in fname:
                try:
                    os.makedirs(str.join('',fname.split('/')[:-1]))
                except FileExistsError:
                    pass
            for file in files_in_backend:
                fname = self._get_fname(file)
                backend.download(client,bucket,fname, self.DB.cache_loc+'/'+fname)

    def remove_from_backend(self,file_list):
        # remove all these files from the backend
        aliases = []
        for file in file_list:

            file_alias = slU._get_alias(file)
            if not file_alias in aliases:
                aliases.append(file_alias)


        # Keep track of file size total
        tot_size = 0

        for alias in aliases:
            # build the  list for the backend
            if alias:
                files_in_backend = []
                for file in file_list:
                    if alias in file:
                        files_in_backend.append(file)

                bucket = slU._get_bucket(file)
                client = self._return_client(file)
                # get the correct backend for the file
                backend = slU._get_backend(file_list[0])

                # Do bulk download
                for file in file_list:
                    fname = self._get_fname(file)
                    backend.remove_obj(client,bucket,fname)


    def bulk_upload(self,file_list):
        # Download all the files in the list in one backend call

        # Get all the aliases in the file_list then do the download for each backend
        aliases = []
        for file in file_list:
            file_alias = slU._get_alias(file)
            if not file_alias in aliases:
                aliases.append(file_alias)

        # Keep track of file size total
        tot_size = 0

        for alias in aliases:
            # build the  list for the backend
            if alias != None:
                files_in_backend = []
                for file in file_list:

                    if alias in file:
                        files_in_backend.append(file)

                bucket = slU._get_bucket(file)
                client = self._return_client(file)
                # get the correct backend for the file
                backend = slU._get_backend(file_list[0])

                for file in files_in_backend:
                    fname = self._get_fname(file)
                    cachedfile = self.DB.cache_loc+'/'+fname
                    cachedfile.replace('//','/')
                    backend.upload(client,cachedfile,bucket,fname)

    def _remove_file(self,fid,silent=True):
        key = slU._get_key(fid)
        try:
            if self.DB.cache_loc[-1] != '/':
                path = '{}/{}'.format(self.DB.cache_loc, key)
            else:
                path = '{}{}'.format(self.DB.cache_loc, key)
            os.remove(path.encode())
        except OSError as e:
            if not silent:
                print('OSError for file not existing? : {}'.format(key))


    def _clear_cache(self):
        """ Removes all data from the cache and database, then removes database

        :return: 0 on successful removal
        """
        file_list = self.DB.get_all_fids()
        for fid in file_list:
            self._remove_file(fid)
            self.DB.remove_entry(fid)
        self.DB.close_db()
        if self.DB.cache_loc[-1] != '/':
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


