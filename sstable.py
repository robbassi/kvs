from __future__ import annotations
from typing import List
from os import stat
from common import TOMBSTONE
from bloomfilter import BloomFilter
from binio import kv_iter, kv_reader, kv_writer

BF_SIZE = 10000
BF_HASH_COUNT = 5


class SSTable:
    """Represents a Sorted-String-Table (SSTable) on disk"""

    class Entries:
        def __init__(self, sstable: SSTable) -> None:
            self.sstable = sstable
            self._pairs = kv_iter(sstable.path)
            try:
                self.current_pair: Tuple[str, Value] = next(self._pairs)
            except StopIteration:
                self.has_next = False
            else:
                self.has_next = True

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(file={self.file}"

        def advance(self) -> None:
            if self.has_next:
                try:
                    self.current_pair = next(self._pairs)
                except StopIteration:
                    self.has_next = False
            else:
                raise RuntimeError("Cannot advance to the next entry. No entries left.")

    def __init__(self, path, index, bf=None):
        self.path = path
        self.index = index
        self.size = stat(path).st_size
        self.bf = bf
        if not self.bf:
            self._sync()

    def _sync(self):
        self.bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        with kv_reader(self.path) as r:
            while r.has_next():
                key = r.read_key()
                self.bf.add(key)
                r.skip_value()

    @classmethod
    def create(cls, path, index, memtable):
        bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        with kv_writer(path) as writer:
            for key, value in memtable.entries():
                writer.write_entry(key, value)
                bf.add(key)
        return cls(path, index, bf)

    @classmethod
    def merge(cls, sstables: List[SSTable]) -> SSTable:
        new_path = sstables[0].path.replace(".dat", "-compacted.dat")
        new_index = sstables[0].index
        new_bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
        readers = [cls.Entries(sstable) for sstable in sstables
                    if sstable.size > 0]
        with kv_writer(new_path) as writer:
            while readers:
                min_reader = min(
                    readers,
                    key=lambda r: (r.current_pair[0], r.sstable.index * -1),
                )
                for reader in readers:
                    if reader is min_reader:
                        continue
                    if reader.current_pair[0] == min_reader.current_pair[0]:
                        reader.advance()
                if min_reader.current_pair[1] is not TOMBSTONE:
                    writer.write_entry(*min_reader.current_pair)
                    new_bf.add(min_reader.current_pair[0])
                min_reader.advance()
                readers = [reader for reader in readers if reader.has_next]
        return cls(new_path, new_index, new_bf)

    def search(self, search_key):
        if not self.bf.exists(search_key):
            return None
        with kv_reader(self.path) as r:
            while r.has_next():
                key = r.read_key()
                # stop if the key is too big
                if key > search_key:
                    return None
                if key == search_key:
                    return r.read_value()
                r.skip_value()
        return None


