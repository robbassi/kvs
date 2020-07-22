from os import path
from memtable import Memtable

class CommitLog:
    def __init__(self, fd):
        self.fd = fd

    @classmethod 
    def resume(cls, log_path):
        memtable = Memtable()
        fd = None
        # recover the memtable if necessary
        if path.exists(log_path):
            fd = open(log_path, 'r+')
            record = fd.readline()
            while record:
                key, value = record.rstrip().split(',')
                if value:
                    memtable.set(key, value)
                else:
                    memtable.unset(key)
                record = fd.readline()
        else:
            fd = open(log_path, 'w')
        return cls(fd), memtable

    def record_set(self, k, v):
        self.fd.write(f"{k},{v}\n")
        self.fd.flush()

    def record_unset(self, k):
        self.fd.write(f"{k},\n")
        self.fd.flush()

    def purge(self):
        self.fd.seek(0)
        self.fd.truncate()
