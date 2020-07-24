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
        with open(path, 'w') as fd:
            for (key, value) in memtable.entries():
                if value == TOMBSTONE:
                    fd.write(f"{key},\n")
                else:
                    fd.write(f"{key},{value}\n")
                bf.add(key)
        # pass readonly copy to constructor
        fd = open(path, 'r')
        return cls(fd, bf)

    def entries(self):
        self.fd.seek(0)
        yield from (line.rstrip().split(',') for line in self.fd)

    def search(self, search_key):
        if not self.bf.exists(search_key):
            return None
        for (key, value) in self.entries():
            if key == search_key:
                if value:
                    return value
                else:
                    return TOMBSTONE
        return None
