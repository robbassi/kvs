from memtable import Memtable
from bloomfilter import BloomFilter
from readerwriterlock import rwlock


class KVS:
    def __init__(self):
        self.bloomfilter = BloomFilter(size=10, hash_count=3)
        self.memtable = Memtable()
        self.rwl = rwlock.RWLockFairD()

    def set(self, k, v):
        with self.rwl.gen_wlock():
            self.bloomfilter.add(v)
            self.memtable.set(k, v)

    def get(self, k):
        with self.rwl.gen_rlock():
            if self.bloomfilter.exists(k):
                self.memtable.get(k)

    def unset(self, k):
        with self.rwl.gen_wlock():
            if self.bloomfilter.exists(k):
                self.memtable.unset(k)
