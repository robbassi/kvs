from memtable import Memtable
from bloomfilter import BloomFilter
from readerwriterlock import rwlock
from segments import Segments

BF_SIZE = 10000
BF_HASH_COUNT = 5
MT_MAX_SIZE = 1000


class KVS:
    def __init__(self, segments_path):
        self.__segments_path = segments_path
        self.__rwl = rwlock.RWLockFairD()
        self.__bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        self.__mt = Memtable()
        self.__segments = Segments(self.__segments_path)

    def set(self, k, v):
        with self.__rwl.gen_wlock():
            if self.__mt.approximate_bytes() <= MT_MAX_SIZE:
                self.__bf.add(k)
                self.__mt.set(k, v)
            else:
                self.__segments.flush(self.__mt)
                self.__mt = Memtable()
                self.__mt.set(k, v)

    def get(self, k):
        with self.__rwl.gen_rlock():
            if self.__bf.exists(k):
                r = self.__mt.get(k)
                if r is None:
                    return self.__segments.search(k)
                else:
                    return r

    def unset(self, k):
        with self.__rwl.gen_wlock():
            if self.__bf.exists(k):
                self.__mt.unset(k)
