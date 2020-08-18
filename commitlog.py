from os import path
from common import TOMBSTONE
from memtable import Memtable
from binio import kv_iter, KVWriter

class CommitLog:
    def __init__(self, writer):
        self.writer = writer

    @classmethod 
    def resume(cls, log_path):
        memtable = Memtable()
        writer = None
        # recover the memtable if necessary
        if path.exists(log_path):
            for key, value in kv_iter(log_path):
                if value == TOMBSTONE:
                    memtable.unset(key)
                else:
                    memtable.set(key, value)
            writer = KVWriter(log_path, append=True)
        else:
            writer = KVWriter(log_path)
        return cls(writer), memtable

    def record_set(self, k, v):
        self.writer.write_entry(k, v)
        self.writer.sync()

    def record_unset(self, k):
        self.writer.write_entry(k, TOMBSTONE)
        self.writer.sync()

    def purge(self):
        self.writer.truncate()
