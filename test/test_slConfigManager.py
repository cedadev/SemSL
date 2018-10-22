__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL._slConfigManager import slConfig
import unittest
import json
import os

class TestConfigFile(unittest.TestCase):

    def setUp(self):
        # get user home directory
        user_home = os.environ["HOME"]

        # add the config file name
        s3_user_config_filename = user_home + "/" + ".sem-sl.json"

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
            raise ValueError("User config file does not exist with path: " +
                                s3_user_config_filename)

    def test_req_fields_present(self):
        main_req_keys = ['version',
                         'hosts',
                         'cache',
                         'system']
        host_keys = ['alias',
                     'backend',
                     'url',
                     'required_credentials',
                     'object_size',
                     'read_connections',
                     'write_connections',
                     'api']
        cache_keys = ['location',
                      'cache_size']

        for key in main_req_keys:
            self.assertIn(key, self._s3_user_config.keys())

        cache = self._s3_user_config['cache']
        for key in cache_keys:
            self.assertIn(key, cache)

        hosts = self._s3_user_config['hosts'].keys()
        for host in hosts:
            host = self._s3_user_config['hosts'][host]
            for key in host_keys:
                self.assertIn(key, host.keys())


    def test_valid_version(self):
        valid_versions = ['8',]
        self.assertIn(self._s3_user_config['version'],valid_versions)

    def test_cache_writable(self):
        with open(self._s3_user_config['cache']['location']+'test','w') as f:
            f.write('test text')
        os.remove(self._s3_user_config['cache']['location']+'test')

    def test_args_valid(self):
        keys_for_size = ["object_size"]
        file_format_sizes = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

        hosts = self._s3_user_config['hosts'].keys()
        for host in hosts:
            host = self._s3_user_config['hosts'][host]
            for key in keys_for_size:
                value = host[key]
                if value.endswith(file_format_sizes):
                    suffix = value[-2:]
                    size = int(value[:-2])
                elif value[-1] == "B":
                    suffix = "B"
                    size = int(value[:-1])
                else:
                    raise ValueError('Problem with {} object size value in config.'.format(host.key()))

            threads_args = ['read_connections', 'write_connections']
            for key in threads_args:
                value = int(host[key])

        sys_keys_for_size = ["object_size_for_memory"]
        for key in sys_keys_for_size:
            value = self._s3_user_config['system'][key]
            if value.endswith(file_format_sizes):
                suffix = value[-2:]
                size = int(value[:-2])
            elif value[-1] == "B":
                suffix = "B"
                size = int(value[:-1])
            else:
                raise ValueError('Problem with object size for memory value in config.')

        cache_keys_for_size = ["cache_size"]
        for key in cache_keys_for_size:
            value = self._s3_user_config['cache'][key]
            if value.endswith(file_format_sizes):
                suffix = value[-2:]
                size = int(value[:-2])
            elif value[-1] == "B":
                suffix = "B"
                size = int(value[:-1])
            else:
                raise ValueError('Problem with cache size value in config.')

class testslConfig(unittest.TestCase):

    def setUp(self):
        self.sl_config = slConfig()

        #get user home directory
        user_home = os.environ["HOME"]

        # add the config file name
        s3_user_config_filename = user_home + "/" + ".sem-sl.json"
        fp = open(s3_user_config_filename)
        # deserialize from the JSON
        self.json_config = json.load(fp)

    def test_config(self):
        self.assertEqual(self.sl_config['cache']['location'],self.json_config['cache']['location'])
        self.assertEqual(self.sl_config['hosts'][list(self.sl_config['hosts'].keys())[0]]['alias'],
                         self.json_config['hosts'][list(self.json_config['hosts'].keys())[0]]['alias'])




if __name__ == "__main__":
    unittest.main()
