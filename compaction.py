from typing import List, Optional, Tuple
from os import scandir
from segments import SEGMENT_PATTERN

MIN_THRESHOLD = 4
MIN_SIZE = 50

class File:
    def __init__(self, path, size, index):
        self.path = path
        self.size = size
        self.index = index

class Bucket:
    def __init__(self):
        self.min = 0
        self.max = 0
        self.avg = 0
        self.files = []

    def compute_bounds(self):
        bucket_size = 0
        for file in self.files:
            bucket_size += file.size
        self.avg = round(bucket_size / len(self.files))
        self.min = self.avg - round(self.avg / 2)
        self.max = self.avg + round(self.avg / 2)
    
    def add(self, file : File):
        self.files.append(file)
        self.compute_bounds()

    def fits(self, file : File) -> bool:
        return file.size >= self.min and file.size <= self.max

    def size(self) -> int:
        return len(self.files)

    def last(self, n : int) -> List[File]:
        files = sorted(self.files, key=lambda file: file.index)
        return files[:n]

def compute_buckets(path : str) -> List[Bucket]:
    # collect the files
    all_files = []
    with scandir(path) as files:
        for file in files:
            match = SEGMENT_PATTERN.search(file.name)
            if match:
                index = int(match.group('index'))
                size = file.stat().st_size
                all_files.append(File(file.path, size, index))
    # bucket the files
    small_bucket = Bucket()
    buckets = [small_bucket]
    for file in all_files:
        if file.size <= MIN_SIZE:
            small_bucket.add(file)
        else:
            file_bucket = None
            for bucket in buckets:
                if bucket.fits(file):
                    file_bucket = bucket
                    break
            if file_bucket is None:
                new_bucket = Bucket()
                new_bucket.add(file)
                buckets.append(new_bucket)
            else:
                file_bucket.add(file)
            buckets.sort(key=lambda b: b.avg)
    return buckets

# stub
def merge(in_files : List[File]) -> File:
    oldest = in_files[0].index
    for file in in_files:
        if oldest > file.index:
            oldest = file.index 
    return File(f"segment-{oldest}", 1000, oldest)

def compaction_pass(buckets) -> Optional[Tuple[List[File], File]]:
    for bucket in buckets:
        if bucket.size() >= MIN_THRESHOLD:
            old_files = bucket.last(MIN_THRESHOLD)
            new_file = merge(old_files)
            return (old_files, new_file)

#########

def describe_buckets(buckets):
    n = 1
    for b in buckets:
        compaction = "yes" if b.size() >= MIN_THRESHOLD else "no"
        print(f"Tier {n} | min {b.min} avg {b.avg} max {b.max} [needs compact {compaction}]") 
        for f in b.files:
            print(f" {f.path} {f.size} bytes")
        n += 1
