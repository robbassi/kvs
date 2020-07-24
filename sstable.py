from bloomfilter import BloomFilter
from memtable import TOMBSTONE

BF_SIZE = 10000
BF_HASH_COUNT = 5


class SSTable:
    """Represents a Sorted-String-Table (SSTable) on disk"""

    def __init__(self, fd, bf=None):
        self.fd = fd
        self.bf = bf
        if not self.bf:
            self.__sync()

    def __sync(self):
        self.bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        for (key, value) in self.entries():
            self.bf.add(key)

    @classmethod
    def create(cls, path, memtable):
        bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        fd = open(path, 'w')
        with fd:
            for (key, value) in memtable.entries():
                if value == TOMBSTONE:
                    fd.write(f"{key},\n")
                else:
                    fd.write(f"{key},{value}\n")
                bf.add(key)
        return cls(fd, bf)

    def entries(self):
        yield from (line.rstrip().split(',') for line in open(self.fd.name, 'r'))

    def search(self, search_key):
        if not self.bf.exists(search_key):
            return None
        for (key, value) in self.entries():
            if key == search_key:
                if value:
                    return value
        return None
