from memtable import Memtable
from bloomfilter import BloomFilter
from readerwriterlock import rwlock


class KVS:
    def __init__(self):
        # come up with size and hash_count with bloom filter
        self.bloomfilter = BloomFilter(10, 5)
        self.memtable = Memtable()
        self.rwl = rwlock.RWLockFairD()

    def set(self, k, v):
        with self.rwl.gen_wlock():
            self.memtable.set(k, v)

    def get(self, k):
        with self.rwl.gen_rlock():
            self.memtable.get(k)

    def unset(self, k):
        with self.rwl.gen_wlock():
            self.memtable.unset(k)
