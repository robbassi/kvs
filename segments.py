from sstable import SSTable
from os import scandir, mkdir, path
from re import compile as compile_re

SEGMENT_TEMPLATE = 'segment-%d.dat'
SEGMENT_PATTERN = compile_re("segment-(?P<index>\d+)\.dat")

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
            if value:
                return value
        return None

    def compact(self):
        pass

