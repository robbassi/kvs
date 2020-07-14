# Heavily inspired by https://www.kdnuggets.com/2016/08/gentle-introduction-bloom-filter.html

from bitarray import bitarray
import mmh3


class BloomFilter:
    def __init__(self, size, hash_count):
        super(BloomFilter, self).__init__()
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)
        self.hash_count = hash_count
        self.size = size

    def __add__(self, item):
        for i in range(self.hash_count):
            hashed_index = mmh3.hash(item, i) % self.size
            self.bit_array[hashed_index] = 1

    def __contains__(self, item):
        out = True
        for i in range(self.hash_count):
            hashed_index = mmh3.hash(item, i) % self.size
            if self.bit_array[hashed_index] == 0:
                out = False
        return out
