from os import SEEK_CUR
from contextlib import contextmanager
from common import TOMBSTONE

KV_ENCODING = 'utf-8'
KV_BYTEORDER = 'big'

TOMBSTONE_SIZE = -1
TOMBSTONE_BUFF = int.to_bytes(TOMBSTONE_SIZE, 2, KV_BYTEORDER, signed=True)

# primitives

def read_int16(fd):
    buff = fd.read(2)
    value = int.from_bytes(buff, KV_BYTEORDER, signed=True)
    return value

def read_uint16(fd):
    buff = fd.read(2)
    value = int.from_bytes(buff, KV_BYTEORDER)
    return value

def write_int16(fd, value):
    buff = int.to_bytes(value, 2, KV_BYTEORDER, signed=True)
    fd.write(buff)

def write_uint16(fd, value):
    buff = int.to_bytes(value, 2, KV_BYTEORDER)
    fd.write(buff)

def read_key(fd):
    key_size = read_uint16(fd)
    buff = fd.read(key_size)
    key = buff.decode(KV_ENCODING)
    return key

def read_value(fd):
    value_size = read_int16(fd)
    if value_size == TOMBSTONE_SIZE:
        return TOMBSTONE
    buff = fd.read(value_size)
    value = buff.decode(KV_ENCODING)
    return value

def write_key(fd, key):
    key_bytes = bytes(key, KV_ENCODING)
    write_uint16(fd, len(key_bytes))
    fd.write(key_bytes)

def write_value(fd, value):
    if value == TOMBSTONE:
        fd.write(TOMBSTONE_BUFF)
    else:
        value_bytes = bytes(value, KV_ENCODING)
        write_int16(fd, len(value_bytes))
        fd.write(value_bytes)

# high-level interface

class KVWriter:
    def __init__(self, path, append=False):
        self.fd = open(path, 'ab' if append else 'wb')

    def write(self, key, value):
        write_key(self.fd, key)
        write_value(self.fd, value)

    def flush(self):
        self.fd.flush()

    def truncate(self):
        self.fd.seek(0)
        self.fd.truncate()
        
    def close(self):
        self.fd.close()

@contextmanager
def kv_writer(path, append=False):
    writer = KVWriter(path, append)
    try:
        yield writer
    finally:
        writer.flush()
        writer.close()

class KVReader:
    def __init__(self, path):
        self.path = path

    def entries(self):
        with open(self.path, 'rb') as fd:
            while fd.peek(1):
                key = read_key(fd)
                value = read_value(fd)
                yield key, value

    def search(self, search_key):
        with open(self.path, 'rb') as fd:
            key = read_key(fd)
            while key:
                if key == search_key:
                    return read_value(fd)
                # quit early if the key is too big
                if key > search_key:
                    return None
                # jump to the next key
                value_size = read_int16(fd)
                if value_size > 0:
                    fd.seek(value_size, SEEK_CUR)
                key = read_key(fd)
        return None
