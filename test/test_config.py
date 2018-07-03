#!/usr/bin/env python2.7
''' Tests the user config file to check all the usr config file setup

'''

import json
import unittest
import os

class ConfigTest(unittest.TestCase):

    def setUp(self):
        # get user home directory
        user_home = os.environ["HOME"]

        # add the config file name
        s3_user_config_filename = user_home + "/" + ".s3nc4.json"

        # open the file
        try:
            fp = open(s3_user_config_filename)
            # deserialize from the JSON
            self._s3_user_config = json.load(fp)
            # add the filename to the config so we can refer to it in error messages
            self._s3_user_config["filename"] = s3_user_config_filename
            # close the config file
            fp.close()
        except IOError:
            raise s3IOException("User config file does not exist with path: " +
                                s3_user_config_filename)

    def test_req_fields_present(self):
        req_keys = ['version',
                    'hosts',
                    'cache_location',
                    'max_cache_size',
                    'max_object_size_for_memory',
                    'max_object_size',
                    'read_threads',
                    'write_threads']
        for key in req_keys:
            self.assertIn(key, self._s3_user_config.keys())

    def test_valid_version(self):
        valid_versions = ['8',]
        self.assertIn(self._s3_user_config['version'],valid_versions)

    def test_cache_writable(self):
        with open(self._s3_user_config['cache_location']+'test','w') as f:
            f.write('test text')
        os.remove(self._s3_user_config['cache_location']+'test')

    def test_args_valid(self):
        keys_for_size = ["max_cache_size",
	                     "max_object_size_for_memory",
	                     "max_object_size"]
        file_format_sizes = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        for key in keys_for_size:
            value = self._s3_user_config[key]
            if value.endswith(file_format_sizes):
                suffix = value[-2:]
                size = int(value[:-2])
            elif value[-1] == "B":
                suffix = "B"
                size = int(value[:-1])
            else:
                raise ValueError('Problem with a size value in config.')

        threads_args = ['read_threads', 'write_threads']
        for key in threads_args:
            value = int(self._s3_user_config[key])

    def test_hosts_keys_valid(self):
        host_keys = ['alias',
                     'url',
                     'accessKey',
                     'secretKey',
                     'api']
        hosts = self._s3_user_config['hosts']
        for hostkey in hosts:
            host = hosts[hostkey]
            for key in host.iterkeys():
                self.assertIn(key,host_keys)

    def test_hosts_values_valid(self):
        hosts = self._s3_user_config['hosts']
        for hostkey in hosts:
            host = hosts[hostkey]
            for value in host.itervalues():
                self.assertIs(type(value),unicode)

if __name__ == '__main__':
    unittest.main()