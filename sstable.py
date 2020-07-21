from os import path

from memtable import TOMBSTONE


class SSTable:
    """Represents a Sorted-String-Table (SSTable) on disk"""

    def __init__(self, sstable_path):
        self.path = sstable_path
        self.fd = None

    def write(self, memtable):
        self.fd = open(self.path, 'w')
        with open(self.path, 'w'):
            for (key, value) in memtable.entries():
                if value == TOMBSTONE:
                    entry = f"{key},\n"
                else:
                    entry = f"{key},{value}\n"
                self.fd.write(entry)

    def search(self, search_key):
        if path.exists(self.path):
            self.fd = open(self.path, 'r')
            with open(self.path, 'r') as sstable:
                for line in sstable:
                    (key, value) = line.rstrip().split(',')
                    if key == search_key:
                        if value:
                            return value
