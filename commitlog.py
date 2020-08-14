from os import path
from common import TOMBSTONE
from memtable import Memtable
from binio import KVReader, KVWriter

class CommitLog:
    def __init__(self, writer):
        self.writer = writer

    @classmethod 
    def resume(cls, log_path):
        memtable = Memtable()
        writer = None
        # recover the memtable if necessary
        if path.exists(log_path):
            log = KVReader(log_path)
            for key, value in log.entries():
                if value == TOMBSTONE:
                    memtable.unset(key)
                else:
                    memtable.set(key, value)
            writer = KVWriter(log_path, append=True)
        else:
            writer = KVWriter(log_path)
        return cls(writer), memtable

    def record_set(self, k, v):
        self.writer.write(k, v)
        self.writer.flush()

    def record_unset(self, k):
        self.writer.write(k, TOMBSTONE)
        self.writer.flush()

    def purge(self):
        self.writer.truncate()
