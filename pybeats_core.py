# -*- coding: utf-8 -*-
from glob import glob
from pybeats_exceptions import PyBeatsFileWorkException, PyBeatsSocketWorkException, PyBeatsDBWorkException
import sqlite3
import os
import socket
import time
import logging
import sys
from logging.handlers import TimedRotatingFileHandler


class PyBeatsWork(object):
    def __init__(self, pybeats_config):
        self.configs = pybeats_config
        self.logger = logging.getLogger(self.__class__.__name__)
        log_format = logging.Formatter('%(asctime)s (%(levelname)s) [%(name)s] %(message)s', '%Y/%m/%d %H:%M:%S')
        # File logger
        rotation_handler = TimedRotatingFileHandler(pybeats_config.log_path + 'pybeats.log', when='d',
                                                    interval=1, backupCount=1)
        rotation_handler.setFormatter(log_format)
        # Stdout logger
        stdout_handler = logging.StreamHandler()
        stdout_handler.setFormatter(log_format)

        self.logger.addHandler(rotation_handler)
        self.logger.addHandler(stdout_handler)
        self.logger.setLevel(pybeats_config.log_level)


class PyBeatsDBWork(PyBeatsWork):
    def __init__(self, pybeats_config):
        super(PyBeatsDBWork, self).__init__(pybeats_config)
        self.configs = pybeats_config
        # Open sincedb
        self.sincedb = sqlite3.connect(self.configs.sincedb_path)
        self.logger.info('Since db opened.')

    def get_since_db_data(self):
        # Get expiration term from config
        expiration_term_hour = int(self.configs.expiration_term_hour)
        expiration_term = expiration_term_hour * 60 * 60 * 1000

        try:
            sincedb_cursor = self.sincedb.cursor()
            # Get rows whose last_active_timestamp is in expiration_term
            sql = 'SELECT inode, ' \
                  '       last_read_offset, ' \
                  '       last_active_timestamp, ' \
                  '       last_path ' \
                  '  FROM pybeats_sincedb ' \
                  ' WHERE last_active_timestamp > ?' \
                  '   AND expired_yn = \'N\''
            sincedb_cursor.execute(sql, [time.time() - expiration_term * 60 * 60])
            sincedb_list = sincedb_cursor.fetchall()
        except self.sincedb.Error as e:
            raise PyBeatsDBWorkException(e.message)

        self.logger.debug('inode | last_read_byte_offset | last_active_timestamp | last_path')
        sincedb_row_dict = {}
        for sincedb_row in sincedb_list:
            self.logger.debug('%d | %d | %f | %s', sincedb_row[0], sincedb_row[1], sincedb_row[2], sincedb_row[3])
            sincedb_row_dict[sincedb_row[0]] = {'last_read_offset': sincedb_row[1],
                                                'last_active_timestamp': sincedb_row[2],
                                                'last_path': sincedb_row[3]}
        return sincedb_row_dict

    def insert_since_db_data(self, inode, file_info):
        sincedb_cursor = self.sincedb.cursor()

        # inode | last_read_byte_offset | last_active_timestamp | last_path
        sql = 'INSERT INTO pybeats_sincedb(inode, last_read_offset, last_active_timestamp, last_path) ' \
              'VALUES(?, ?, ?, ?)'
        sincedb_cursor.execute(sql, [inode, file_info['last_read_offset'],
                                     file_info['last_active_timestamp'], file_info['last_path']])

    def update_since_db_data(self, inode, since_db_data):
        sincedb_cursor = self.sincedb.cursor()
        # inode | last_read_byte_offset | last_active_timestamp | last_path
        sql = 'UPDATE pybeats_sincedb SET last_read_offset = ?, last_active_timestamp = ? ' \
              'WHERE inode = ? AND last_path = ?'

        sincedb_cursor.execute(sql, [since_db_data['last_read_offset'], since_db_data['last_active_timestamp'],
                                     inode, since_db_data['last_path']])

    def set_expired(self, inode, last_path):
        sincedb_cursor = self.sincedb.cursor()

        # inode | last_read_byte_offset | last_active_timestamp | last_path
        sql = 'UPDATE pybeats_sincedb SET expired_yn = \'Y\' ' \
              'WHERE inode = ? AND last_path = ?'

        sincedb_cursor.execute(sql, [inode, last_path])

    def commit(self):
        self.sincedb.commit()

    def close(self):
        if self.sincedb is not None:
            self.sincedb.close()
            self.logger.info('Sincedb is closed.')


class PyBeatsFileWork(PyBeatsWork):

    def __init__(self, pybeats_config):
        super(PyBeatsFileWork, self).__init__(pybeats_config)
        self.configs = pybeats_config
        self.opened_file_list = []

    def get_files_in_path(self):
        file_path = self.configs.path
        file_list = glob(file_path)
        return file_list

    def open_files(self):
        files_to_open_list = self.get_files_in_path()
        file_info_list = []
        for file_path in files_to_open_list:
            file_pointer = open(file_path)
            self.opened_file_list.append(file_pointer)
            self.logger.debug('\'%s\' opened.', file_path)
            file_inode = os.stat(file_path).st_ino
            file_info = {'file_path': file_path, 'file_pointer': file_pointer, 'file_inode': file_inode}
            file_info_list.append(file_info)
        return file_info_list

    def close_files(self):
        for each_file in self.opened_file_list:
            each_file.close()
        self.logger.info('All files are closed.')


class PyBeatsSocketWork(PyBeatsWork):
    def __init__(self, pybeats_config):
        super(PyBeatsSocketWork, self).__init__(pybeats_config)
        self.configs = pybeats_config
        self.sock = None
        self.connect()

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.configs.dest_host, self.configs.dest_port))
            self.logger.info('%s:%d connected.', self.configs.dest_host, self.configs.dest_port)
        except socket.error as msg:
            self.logger.error('Socket exception occurred!: %s', msg[1])
            retries = 0
            while retries < self.configs.max_connection_retry:
                retries += 1
                self.logger.info('Trying to reconnect...')
                try:
                    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.sock.connect((self.configs.dest_host, self.configs.dest_port))
                    is_connected = True
                    break
                except socket.error:
                    self.logger.error('Failed to reconnect!')
                    is_connected = False
                time.sleep(30)
            if is_connected:
                self.logger.info('%s:%d reconnected.', self.configs.dest_host, self.configs.dest_port)
            else:
                self.logger.error('Socket exception occurred!: %s', msg[1])
                raise PyBeatsSocketWorkException(msg[1])

    def send_log(self, logs):
        while len(logs) > 0:
            try:
                each_log = logs.pop()
                self.sock.send(each_log)
                self.logger.debug('[%s] is sent to %s:%d.', each_log.rstrip('\n'), self.configs.dest_host, self.configs.dest_port)
            except socket.error as msg:
                self.logger.error('Socket exception occurred!: %s', msg[1])
                self.connect()
                logs.append(each_log)
                # If we failed to reconnect, exception raises in connection().
                # Resend if reconnected.
                continue

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.logger.info('Connection is closed.')


class PyBeatsConfig(PyBeatsWork):
    def __init__(self, pybeats_config):
        self.dest_host = pybeats_config['dest_host']
        self.dest_port = pybeats_config['dest_port']
        self.max_connection_retry = pybeats_config['max_connection_retry']
        self.sincedb_path = pybeats_config['sincedb_path']
        self.path = pybeats_config['path']
        self.log_level = pybeats_config['log_level']
        self.log_path = pybeats_config['log_path']
        self.prefix = pybeats_config['prefix']
        self.expiration_term_hour = pybeats_config['expiration_term']
        self.file_mon_term = pybeats_config['file_mon_term']
        self.batch_size = pybeats_config['batch_size']
        super(PyBeatsConfig, self).__init__(self)

        self.logger.info('## CONFIGS')
        self.logger.info('## dest_host: %s' % self.dest_host)
        self.logger.info('## dest_port: %d' % self.dest_port)
        self.logger.info('## max_connection_retry: %d' % self.max_connection_retry)
        self.logger.info('## sincedb_path: %s' % self.sincedb_path)
        self.logger.info('## path: %s' % self.path)
        self.logger.info('## log_level: %s' % self.log_level)
        self.logger.info('## log_path: %s' % self.log_path)
        self.logger.info('## expiration_term_hour: %dH' % self.expiration_term_hour)
        self.logger.info('## file_mon_term: %d' % self.file_mon_term)
        self.logger.info('## batch_size: %d ' % self.batch_size)


class PyBeatsSendLogQueues(PyBeatsWork):
    def __init__(self):
        self.all_queues = {}

    def make_new_queue(self, queue_name):
        self.all_queues['queue_name'] = []

    def get_queue(self, queue_name):
        try:
            return self.all_queues['queue_name']
        except KeyError:
            return None
