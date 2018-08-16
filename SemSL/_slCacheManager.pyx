"""
Cache/staging management for SemSL.

Requested files from backend are stored in a disk caching area defined in the user's config file. Object requests check
the disk cache before, going to the backend, and if the file exists in cache the path is provided, otherwise the request
is sent to the backend. Once the cache is full, the least recently accessed files are removed in order to make space.
"""

import os
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager
from SemSL import Backends

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




    def _get_alias(self,fid):

        # get all host keys
        hosts = self.sl_config['hosts']
        host_keys = hosts.keys()
        # Iterate through config to get all aliases
        aliases = []
        for host_name in iter(host_keys):
            aliases.append(self.sl_config['hosts'][host_name]['alias'])

        for alias in aliases:
            if alias in fid:
                return alias
            else: # return None if alias isn't found in list
                return None

    def _return_client(self,fid):

        conn_man = slConnectionManager(self.sl_config)
        alias = self._get_alias(fid)
        conn = conn_man.open(alias)
        client = conn.get()
        return client

    def _get_fname(self,fid):
        fname = fid[:]
        alias = self._get_alias(fid)
        bucket = self._get_bucket(fid)
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

    def _get_backend(self,fid):
        """ Get the backend object inorder to interact with the backend

        :param fid:
        :return:
        """

        alias = self._get_alias(fid)

        host_name = None
        try:
            hosts = self.sl_config["hosts"]
            for h in hosts:
                if alias in hosts[h]['alias']:
                    host_name = h
        except Exception as e:
            raise ValueError("Error in config file {} {}".format(
                                self.sl_config["filename"],
                                e))

        host_config = self.sl_config["hosts"][host_name]
        backend_name = host_config['backend']
        backend = Backends.get_backend_from_id(backend_name)

        return backend

    def _write_to_cache(self,fid,test=False,file_size=None):
        if not self.DB.check_cache(fid):
            # get file from backend
            if not test:
                print 'WARNING: backend not implemented, using dummy file'

            # Need to get the size of the file
            if test:
                if file_size is None:
                    file_size = 90*10**6
            else:
                # need to caculate or query the files size
                raise NotImplementedError
            # remove oldest cached files if need be
            self._remove_oldest(file_size)

            bucket = self._get_bucket(fid)
            client = self._return_client(fid)
            alias = self._get_alias(fid)
            fname = self._get_fname(fid)
            if '/' in fname:
                try:
                    os.makedirs(str.join('',fname.split('/')[:-1]))
                except FileExistsError:
                    pass

            # get the correct backend for the file
            backend = self._get_backend(fid)

            backend.download(self,client,bucket,fname, self.DB.cache_loc+'/'+fname)# only works with boto3 client objects

            # update cachedb
            # set access and creation time, and filesize
            self.DB.add_entry(fid,file_size)
        else:
            return 0

    def _get_bucket(self,fid):
        """
        Return the bucketname
        :param fid:
        :return:
        """
        fname = fid[:]
        alias = self._get_alias(fid)
        bucket = fname.replace(alias,'')
        assert bucket.split('/')[0] == ''
        return bucket.split('/')[1]

    def _upload_from_cache(self,fid,test=False):
        """
        Uploads the required file from cache to the backend. Called from close().
        :param fid: the file name
        :param test: determines whether the call is from a test
        :return:
        """

        # get location of file
        cloc = self.DB.get_cache_loc(fid)
        client = self._return_client(fid)
        bucket = self._get_bucket(fid)

        # create list of buckets to check through
        buckets_dict = client.list_buckets()['Buckets']
        bucket_check = False
        for i in range(len(buckets_dict)):
            if buckets_dict[i]['Name'] == bucket:
                bucket_check = True

        # get backend object
        backend = self._get_backend(fid)

        # create bucket if it doen't exist
        if not bucket_check:
            backend.create_bucket(self,client,bucket)

        fname = self._get_fname(fid)
        backend.upload(self,client,cloc,bucket,fname)

    def _check_whether_posix(self,fid,access_type):
        # Check whether there is an alias in the file path if not, assume it is a posix filepath and pass back the
        # filepath as the cache path, as there is no need to cp from disk to the caching area
        if self._get_alias(fid) is None:
            # Check whether is looks like a filepath
            # use os.path.exists for a or r
            if access_type == 'r' or access_type == 'a':
                return os.path.exists(fid)

            elif access_type == 'w':
                return os.path.exists(os.path.dirname(fid))

            return False # returns False if alias not in
        else:
            return 'Alias exists' # return None if alias is in list

    def open(self,fid,access_type='r',diskless=False,test=False,file_size=None):
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
                else:
                    # get connection
                    # call get on backend
                    if not diskless:
                        # write file to cache
                        # a 'download' avoids reading too much into memory and crashing as opposed to a 'read'
                        #file_size= GET FILESIZE
                        self._write_to_cache(fid,file_size=file_size)
                        #self._write_to_cache(fid,file_size=file_size)
                    else: # diskless
                        # 'get' file, don't write to cache
                        # if too large for memory, throw execption
                        raise NotImplementedError
                return self.DB.get_cache_loc(fid)

        elif access_type == 'w':
            # create a cache entry
            self.DB.add_entry(fid)
            # use backend to create file?
            # for dev purposes
            with open(self.DB.get_cache_loc(fid),'w') as f:
                f.write('test')
            # create file with frontend... TODO

            return self.DB.get_cache_loc(fid)

        else:
            raise ValueError('Invalid access type')


    def close(self,fid,test=False):
        """ Uploads the file from cache, or directly to the backend, if not in cache, will save to cache
        :param test:
        :return: 0 on success
        """

        #fid = self.fid
        if self.access_type == 'r':
            # update access db
            self.DB.update_access_time(fid)
        elif self.access_type == 'a' or self.access_type == 'w':
            if self.diskless:
                pass
                # do something
            else:
                # do something else
                if self.DB.check_cache(fid):
                    self._upload_from_cache(fid)
            pass
        else:
            raise ValueError('Access mode not supported')

    def _remove_file(self,fid,silent=False):
        try:
            if self.DB.cache_loc[-1] != '/':
                path = '{}/{}'.format(self.DB.cache_loc, fid.split('/')[-1])
            else:
                path = '{}{}'.format(self.DB.cache_loc, fid.split('/')[-1])
            os.remove(path.encode())
        except OSError as e:
            if not silent:
                print('OSError for file not existing? : {}'.format(fid))


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


