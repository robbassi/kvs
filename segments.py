from sstable import SSTable
from os import scandir, mkdir, path, remove
from re import compile as compile_re
from bloomfilter import BloomFilter
from binio import kv_iter

SEGMENT_TEMPLATE = 'segment-%d.dat'
SEGMENT_PATTERN = compile_re("segment-(?P<index>\d+)\.dat")

BF_SIZE = 10000
BF_HASH_COUNT = 5

from compaction import compaction_pass, compute_buckets

class Segments:
    def __init__(self, segment_dir):
        self.segment_dir = segment_dir
        self.segments = []
        if path.isdir(self.segment_dir):
            self._load_segments()
        else:
            mkdir(self.segment_dir)

    def _load_segments(self):
        segment_files = []
        with scandir(self.segment_dir) as files:
            for f in files:
                match = SEGMENT_PATTERN.search(f.name)
                if match:
                    index = int(match.group('index'))
                    segment = SSTable(f.path)
                    segment_files.append((index, segment))
        if segment_files:
            segment_files.sort(key=lambda t: t[0], reverse=True)
            self.segments = [t[1] for t in segment_files]

    def flush(self, memtable):
        index = len(self.segments) + 1
        path = f"{self.segment_dir}/{SEGMENT_TEMPLATE % index}"
        sstable = SSTable.create(path, memtable)
        self.segments.insert(0, sstable)

    def search(self, k):
        for segment in self.segments:
            value = segment.search(k)
            if value is not None:
                return value
        return None

    def compaction_pass(self):
        buckets = compute_buckets(self.segment_dir)
        files = compaction_pass(buckets)
        if files is not None:
            old_files, new_file = files
            for old_file in old_files:
                remove(old_file.path)
                # TODO: remove deleted segments from in memory segments as they are being deleted on disk
                for k, _ in kv_iter(new_file.path):
                    new_bf = BloomFilter(BF_SIZE, BF_HASH_COUNT)
                    new_bf.add(k)
            self.segments += [SSTable(new_file.path, new_bf)]
