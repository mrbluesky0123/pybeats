import json
import unittest
from pybeats_core import *

config = json.loads('{"dest_host": "127.0.0.1", "dest_port": 5400, "sincedb_path": "sincedb.db", "path": "/Users/mrbluesky/code/pybeats/*.py", "log_level": "INFO", "prefix": "[fep-alp][daemon-name]", "expiration_term": 24, "batch_size": 100}')
global_config = PyBeatsConfig(config)


class MyTestCase(unittest.TestCase):

    def test_getting_config(self):
        self.assertEqual(24, global_config.expiration_term_hour, '#1 Test getting config.expiration_term')
        self.assertEqual('127.0.0.1', global_config.dest_host, '#2 Test getting config.expiration_term')
        self.assertEqual('sincedb.db', global_config.sincedb_path, '#3 Test getting config.expiration_term')
        self.assertEqual('INFO', global_config.log_level, '#4 Test getting config.expiration_term')

    def test_getting_since_db_data(self):
        file_work = PyBeatsFileWork(global_config)
        sincedb_row_dict = file_work.get_since_db_data()

        self.assertEqual(1234352, sincedb_row_dict[1234]['last_read_byte_offset'])
        self.assertEqual(3456346, sincedb_row_dict[457322]['last_read_byte_offset'])
        # self.assertEqual('/root', sincedb_row_dict_list[0]['last_path'])
        # self.assertEqual('/var', sincedb_row_dict_list[1]['last_path'])

    def test_getting_files_in_path(self):
        file_work = PyBeatsFileWork(global_config)
        file_list = file_work.get_files_in_path()
        self.assertEqual(True, '/Users/mrbluesky/code/pybeats/pybeats_main.py' in file_list)

    def test_getting_files_in_path(self):
        file_work = PyBeatsFileWork(global_config)
        file_pointer_list = file_work.open_files()
        self.assertEqual(26828014, file_pointer_list[0][1])

    # Need to fix
    def test_connecting(self):
        socket_work = PyBeatsSocketWork(global_config)
        self.assertRaises(PyBeatsSocketWorkException, socket_work.connect())

if __name__ == '__main__':
    unittest.main()
