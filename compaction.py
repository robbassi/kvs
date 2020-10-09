import sys
from common import TOMBSTONE, Value
from binio import kv_iter, kv_writer
from typing import List, Optional, Tuple
from os import scandir, stat
from sstable import SSTable

MIN_THRESHOLD = 4
MIN_SIZE = 50000000


class Bucket:
    def __init__(self):
        self.min = 0
        self.max = 0
        self.avg = 0
        self.segments = []

    def compute_bounds(self):
        bucket_size = 0
        for segment in self.segments:
            bucket_size += segment.size
        self.avg = round(bucket_size / len(self.segments))
        self.min = self.avg - round(self.avg / 2)
        self.max = self.avg + round(self.avg / 2)

    def add(self, segment: SSTable):
        self.segments.append(segment)
        self.compute_bounds()

    def fits(self, segment: SSTable) -> bool:
        return segment.size >= self.min and segment.size <= self.max

    def size(self) -> int:
        return len(self.segments)

    def oldest(self, n: int) -> List[SSTable]:
        segments = sorted(self.segments, key=lambda segment: segment.index)
        return segments[:n]

def compute_buckets(segments: List[SSTable]) -> List[Bucket]:
    small_bucket = Bucket()
    buckets = [small_bucket]
    for segment in segments:
        if segment.size <= MIN_SIZE:
            small_bucket.add(segment)
        else:
            segment_bucket = None
            for bucket in buckets:
                if bucket.fits(segment):
                    segment_bucket = bucket
                    break
            if segment_bucket is None:
                new_bucket = Bucket()
                new_bucket.add(segment)
                buckets.append(new_bucket)
            else:
                segment_bucket.add(segment)
            buckets.sort(key=lambda b: b.avg)
    return buckets

def compaction_pass(buckets: List[Bucket]) -> Tuple[List[SSTable], Optional[SSTable]]:
    for bucket in buckets:
        if bucket.size() >= MIN_THRESHOLD:
            old_files = bucket.oldest(MIN_THRESHOLD)
            new_file = SSTable.merge(old_files)
            return (old_files, new_file)
    return ([], None)

def describe_buckets(buckets):
    for i, b in enumerate(buckets):
        compaction = "(needs compaction)" if b.size() >= MIN_THRESHOLD else ""
        print(
            f"Tier {i + 1} | min {b.min} avg {b.avg} max {b.max} {compaction}"
        )
        for f in b.segments:
            print(f" {f.path} {f.size} bytes")
