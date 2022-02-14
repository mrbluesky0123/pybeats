# -*- coding: utf-8 -*-
import argparse
import json
import time
import signal

from pybeats_core import *
from pybeats_exceptions import PyBeatsSocketWorkException, PyBeatsFileWorkException, PyBeatsException


class PyBeatsMainWork(PyBeatsWork):

    def __init__(self, pybeats_config):
        super(PyBeatsMainWork, self).__init__(pybeats_config)
        self.pybeats_config = pybeats_config
        self.global_queues = PyBeatsSendLogQueues()
        self.pybeats_socket_work = PyBeatsSocketWork(self.pybeats_config)
        self.pybeats_file_work = PyBeatsFileWork(self.pybeats_config)
        self.pybeats_db_work = PyBeatsDBWork(self.pybeats_config)

    def do_pybeats(self):
        # IMPORTANT: Signal handling when received `kill` signal or another
        self.logger.info('Watching files...')
        while True:
            self.read_log_and_send_log()
            time.sleep(self.pybeats_config.file_mon_term)

    def handle_signal(self, signum, frame):
        self.logger.info('Signal \'%i\' received.' % signum)
        try:
            # Send all in queue
            queues = self.global_queues.all_queues.values()
            for each_queue in queues:
                self.pybeats_socket_work.send_log(each_queue)
            # Return all resources
            self.pybeats_socket_work.close()
            self.pybeats_db_work.close()
            self.pybeats_file_work.close_files()
        except Exception:
            # Ignore all exceptions
            pass
        exit(0)

    def read_log_and_send_log(self):
        since_db_data_dict = self.pybeats_db_work.get_since_db_data()
        file_infos_to_read = self.pybeats_file_work.open_files()
        for each_file in file_infos_to_read:
            inode = each_file['file_inode']
            file_path = each_file['file_path']
            file_pointer = each_file['file_pointer']

            # Get queue
            logs_to_send = self.global_queues.get_queue(file_path)
            if not logs_to_send:
                self.global_queues.make_new_queue(file_path)
                logs_to_send = self.global_queues.get_queue(file_path)

            # Create new row if inode has no file information. This means that the inode is newly added one.
            try:
                since_db_data = since_db_data_dict[inode]
            except KeyError:
                self.logger.info('No inode data in sincedb.')
                since_db_data = {'last_read_offset': 0, 'last_active_timestamp': 0.0, 'last_path': file_path}
                self.pybeats_db_work.insert_since_db_data(inode, since_db_data)

            # If the file path is different from the path in db, it needs to be updated.
            # Set `expired_yn` of old one to 'Y' and insert new one's info.
            if file_path != since_db_data['last_path']:
                self.pybeats_db_work.set_expired(inode, since_db_data['last_path'])
                since_db_data = {'last_read_offset': 0, 'last_active_timestamp': 0.0, 'last_path': file_path}
                self.pybeats_db_work.insert_since_db_data(inode, since_db_data)

            # First of all, detect if log is added.
            file_pointer.seek(0, 2)  # Move cursor to the end of log
            if file_pointer.tell() <= since_db_data['last_read_offset']:
                since_db_data['last_active_timestamp'] = time.time()
                self.pybeats_db_work.update_since_db_data(inode, since_db_data)
                self.pybeats_db_work.commit()
                continue  # Logs are not added on this file
            # Second, move cursor to the position to start to read.
            file_pointer.seek(since_db_data['last_read_offset'])
            while True:
                single_log = file_pointer.readline()
                if not single_log:
                    break
                # If the single log doesn't end with '\n', it is regarded as non-complete log.
                # Send list that last complete log is added. And move cursor to the end of last complete log.
                if single_log[len(single_log) - 1] != '\n':
                    last_read_offset = file_pointer.tell() - len(single_log)
                else:
                    last_read_offset = file_pointer.tell()
                    logs_to_send.append(single_log)

                if len(logs_to_send) == self.pybeats_config.batch_size:
                    self.pybeats_socket_work.send_log(logs_to_send)
                    batch_size_counter = 0

            # Update sincedb
            since_db_data['last_read_offset'] = last_read_offset
            since_db_data['last_active_timestamp'] = time.time()
            self.pybeats_db_work.update_since_db_data(inode, since_db_data)

            self.pybeats_db_work.commit()
            file_pointer.close()


def register_all_signal(signal_handler):
    for x in dir(signal):
        if not x.startswith("SIG"):
            continue
        try:
            signum = getattr(signal, x)
            signal.signal(signum, signal_handler)
        except Exception:
            continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run pyBeats with arguments.")
    parser.add_argument('-c', '--config-path', dest='config-path', help="(OPTIONAL) Config file path", required=False)
    args = parser.parse_args()

    if args.__dict__['config-path'] is None or args.__dict__['config-path'] == '':
        config_path = 'pybeats.json'
    else:
        config_path = args.__dict__['config-path']

    # Open config
    pybeats_config_raw = open(config_path).read()
    pybeats_config_dict = json.loads(pybeats_config_raw)
    pybeats_config = PyBeatsConfig(pybeats_config_dict)

    # Inject config
    pybeats = PyBeatsMainWork(pybeats_config)
    # Register signal
    register_all_signal(pybeats.handle_signal)
    pybeats.do_pybeats()
