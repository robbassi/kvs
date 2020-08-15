from os import SEEK_SET, SEEK_CUR
from contextlib import contextmanager
from common import TOMBSTONE

KV_ENCODING = 'utf-8'
KV_BYTEORDER = 'big'
KV_KEY_BYTES = 2
KV_VALUE_BYTES = 2 

TOMBSTONE_SIZE = -1
TOMBSTONE_BUFF = TOMBSTONE_SIZE.to_bytes(KV_KEY_BYTES, KV_BYTEORDER, signed=True)

class KVReader:
    def __init__(self, path):
        self.fd = open(path, 'rb')

    def seek(self, offset):
        self.fd.seek(offset, SEEK_SET)

    def skip(self, offset):
        self.fd.seek(offset, SEEK_CUR)

    def has_next(self):
        return self.fd.peek(1)

    def read_key_size(self):
        key_buff = self.fd.read(KV_KEY_BYTES)
        key_size = int.from_bytes(key_buff, KV_BYTEORDER)
        return key_size

    def read_value_size(self):
        value_buff = self.fd.read(KV_VALUE_BYTES)
        value_size = int.from_bytes(value_buff, KV_BYTEORDER, signed=True)
        return value_size

    def read_key_bytes(self, key_size):
        key_buff = self.fd.read(key_size)
        key_data = key_buff.decode(KV_ENCODING)
        return key_data

    def read_value_bytes(self, value_size):
        value_buff = self.fd.read(value_size)
        value_data = value_buff.decode(KV_ENCODING)
        return value_data

    def read_key(self):
        key_size = self.read_key_size()
        key_data = self.read_key_bytes(key_size)
        return key_data

    def read_value(self):
        value_size = self.read_value_size()
        if value_size == TOMBSTONE_SIZE:
            return TOMBSTONE
        value_data = self.read_value_bytes(value_size)
        return value_data

    def read_entry(self):
        return self.read_key(), self.read_value()

    def close(self):
        self.fd.close()

class KVWriter:
    def __init__(self, path, append=False):
        self.fd = open(path, 'ab' if append else 'wb')

    def write_key(self, key):
        key_buff = bytes(key, KV_ENCODING)
        key_size_buff = int.to_bytes(len(key_buff), KV_KEY_BYTES, KV_BYTEORDER)
        self.fd.write(key_size_buff)
        self.fd.write(key_buff)

    def write_value(self, value):
        if value == TOMBSTONE:
            self.fd.write(TOMBSTONE_BUFF)
            return
        value_buff = bytes(value, KV_ENCODING)
        value_size_buff = int.to_bytes(len(value_buff), KV_VALUE_BYTES, KV_BYTEORDER, signed=True)
        self.fd.write(value_size_buff)
        self.fd.write(value_buff)

    def write_entry(self, key, value):
        self.write_key(key)
        self.write_value(value)

    def flush(self):
        self.fd.flush()

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
        writer.flush()
        writer.close()

def kv_iter(path):
    with kv_reader(path) as r:
        while r.has_next():
            yield r.read_entry()
