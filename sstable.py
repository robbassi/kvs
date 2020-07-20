from contextlib import contextmanager
from os import path

from memtable import TOMBSTONE


class SSTable:

    def __init__(self, sstable_path):
        self.path = sstable_path
        self.fd = None

    def write(self, memtable):
        with self.open('w'):
            for (key, value) in memtable.entries():
                if value == TOMBSTONE:
                    entry = f"{key},\n"
                else:
                    entry = f"{key},{value}\n"
                self.fd.write(entry)

    def search(self, search_key):
        if path.exists(self.path):
            with self.open('r') as sstable:
                for line in sstable.fd:
                    (key, value) = line.rstrip().split(',')
                    if key == search_key:
                        if value:
                            return value

    @contextmanager
    def open(self, mode):
        try:
            self.fd = open(self.path, mode)
            yield self
        finally:
            self.fd.close()
