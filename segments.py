from sstable import SSTable
from os import scandir, mkdir, path, remove, rename
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
                    segment = SSTable(f.path, index)
                    segment_files.append(segment)
        if segment_files:
            self.segments = sorted(
                segment_files,
                key=lambda segment: segment.index,
                reverse=True,
            )
   
    def flush(self, memtable):
        index = 0
        if len(self.segments):
            index = self.segments[0].index + 1
        path = f"{self.segment_dir}/{SEGMENT_TEMPLATE % index}"
        sstable = SSTable.create(path, index, memtable)
        self.segments.insert(0, sstable)

    def search(self, k):
        for segment in self.segments:
            value = segment.search(k)
            if value is not None:
                return value
        return None

    def compact(self):
        buckets = compute_buckets(self.segments)
        old_files, new_file = compaction_pass(buckets)
        if new_file:
            new_segments = []
            old_indexes = set(f.index for f in old_files)
            updated = False
            for segment in self.segments:
                if not updated and segment.index <= new_file.index:
                    new_segment = SSTable(new_file.path, new_file.index)
                    new_segments.append(new_segment)
                    updated = True
                if segment.index not in old_indexes:
                    new_segments.append(segment)
            self.segments = new_segments
            # delete files
            for f in old_files:
                remove(f.path)
            # rename new_file
            new_file_path = new_file.path.replace("-compacted", "")
            rename(new_file.path, new_file_path)

