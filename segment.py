from contextlib import contextmanager
from os import path

from memtable import TOMBSTONE


class Segment:

    def __init__(self, segment_path):
        self.path = segment_path
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
            with self.open('r') as segment:
                for line in segment.fd:
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
