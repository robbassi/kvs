from os import fsync, SEEK_SET, SEEK_CUR
from contextlib import contextmanager
from common import TOMBSTONE

ENCODING = 'utf-8'
BYTEORDER = 'big'
KEY_BYTES = 2
VALUE_BYTES = 2 

TOMBSTONE_SIZE = -1
TOMBSTONE_BUFF = TOMBSTONE_SIZE.to_bytes(KEY_BYTES, BYTEORDER, signed=True)

class KVReader:
    def __init__(self, path):
        self.fd = open(path, 'rb')

    def has_next(self):
        if self.fd.read(1):
            self.skip(-1)
            return True
        return False

    def seek(self, offset):
        self.fd.seek(offset, SEEK_SET)

    def skip(self, offset):
        self.fd.seek(offset, SEEK_CUR)

    def read_key_size(self):
        key_buff = self.fd.read(KEY_BYTES)
        key_size = int.from_bytes(key_buff, BYTEORDER)
        return key_size

    def read_value_size(self):
        value_buff = self.fd.read(VALUE_BYTES)
        value_size = int.from_bytes(value_buff, BYTEORDER, signed=True)
        return value_size

    def read_bytes(self, size):
        buff = self.fd.read(size)
        data = buff.decode(ENCODING)
        return data

    def skip_key(self):
        key_size = self.read_key_size()
        self.skip(key_size)

    def skip_value(self):
        value_size = self.read_value_size()
        if value_size > 0:
            self.skip(value_size)

    def read_key(self):
        key_size = self.read_key_size()
        key_data = self.read_bytes(key_size)
        return key_data

    def read_value(self):
        value_size = self.read_value_size()
        if value_size == TOMBSTONE_SIZE:
            return TOMBSTONE
        value_data = self.read_bytes(value_size)
        return value_data

    def read_entry(self):
        return self.read_key(), self.read_value()

    def close(self):
        self.fd.close()

class KVWriter:
    def __init__(self, path, append=False):
        self.fd = open(path, 'ab' if append else 'wb')

    def write_key(self, key):
        key_buff = bytes(key, ENCODING)
        key_size_buff = int.to_bytes(len(key_buff), KEY_BYTES, BYTEORDER)
        self.fd.write(key_size_buff)
        self.fd.write(key_buff)

    def write_value(self, value):
        if value == TOMBSTONE:
            self.fd.write(TOMBSTONE_BUFF)
            return
        value_buff = bytes(value, ENCODING)
        value_size_buff = int.to_bytes(len(value_buff), VALUE_BYTES, BYTEORDER, signed=True)
        self.fd.write(value_size_buff)
        self.fd.write(value_buff)

    def write_entry(self, key, value):
        self.write_key(key)
        self.write_value(value)

    def sync(self):
        self.fd.flush()
        fsync(self.fd.fileno())

    def truncate(self):
        self.fd.seek(0)
        self.fd.truncate()
        
    def close(self):
        self.fd.close()

@contextmanager
def kv_reader(path):
    reader = KVReader(path)
    try:
        yield reader
    finally:
        reader.close()

@contextmanager
def kv_writer(path, append=False):
    writer = KVWriter(path, append)
    try:
        yield writer
    finally:
        writer.sync()
        writer.close()

def kv_iter(path):
    with kv_reader(path) as r:
        while r.has_next():
            yield r.read_entry()
