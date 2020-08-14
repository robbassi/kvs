from bloomfilter import BloomFilter
from binio import KVReader, kv_writer

BF_SIZE = 10000
BF_HASH_COUNT = 5


class SSTable:
    """Represents a Sorted-String-Table (SSTable) on disk"""

    def __init__(self, path, bf=None):
        self.reader = KVReader(path)
        self.bf = bf
        if not self.bf:
            self._sync()

    def _sync(self):
        self.bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        for key, value in self.reader.entries():
            self.bf.add(key)

    @classmethod
    def create(cls, path, memtable):
        bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        with kv_writer(path) as writer:
            for key, value in memtable.entries():
                writer.write(key, value)
                bf.add(key)
        return cls(path, bf)

    def entries(self):
        yield from self.reader.entries()

    def search(self, key):
        if self.bf.exists(key):
            return self.reader.search(key)
        return None
