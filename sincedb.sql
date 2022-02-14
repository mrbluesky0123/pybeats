-- inode | last_read_byte_offset | last_active_timestamp | last_path
CREATE table pybeats_sincedb(inode INTEGER, last_read_offset INTEGER DEFAULT 0, last_active_timestamp REAL, last_path TEXT, expired_yn TEXT(1) DEFAULT 'N');

ALTER TABLE pybeats_sincedb COLUMN expired_yn TEXT(1);

DROP TABLE pybeats_sincedb;

.schema