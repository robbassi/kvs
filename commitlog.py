from os import path
from memtable import Memtable

class CommitLog:
    def __init__(self, log_path):
        self.fd = None
        self.log_path = log_path
        self.recovered_memtable = None
        self.resume()
     
    def resume(self):
        # recover the memtable if necessary
        if path.exists(self.log_path):
            memtable = Memtable()
            self.fd = open(self.log_path, 'r+')
            record = self.fd.readline()
            while record:
                key, value = record.rstrip().split(',')
                memtable.set(key, value)
                record = self.fd.readline()
            # check if it has any data
            if memtable.approximate_bytes() > 0:
                self.recovered_memtable = memtable
        else:
            self.fd = open(self.log_path, 'w')

    def record(self, k, v):
        self.fd.write(f"{k},{v}\n")
        self.fd.flush()

    def purge(self):
        self.fd = open(self.log_path, 'w')

