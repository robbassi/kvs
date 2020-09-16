import sys
from common import TOMBSTONE, Value
from binio import kv_iter, kv_writer
from dataclasses import dataclass
from typing import List, Optional, Tuple
from os import scandir
from segments import SEGMENT_PATTERN

MIN_THRESHOLD = 4
MIN_SIZE = 50


@dataclass(frozen=True)
class File:
    path: str
    size: int
    index: int


class Entries:
    def __init__(self, file: File) -> None:
        self.file = file
        self._pairs = kv_iter(file.path)
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


class Bucket:
    def __init__(self):
        self.min = 0
        self.max = 0
        self.avg = 0
        self.files = []

    def compute_bounds(self):
        bucket_size = 0
        for file_ in self.files:
            bucket_size += file_.size
        self.avg = round(bucket_size / len(self.files))
        self.min = self.avg - round(self.avg / 2)
        self.max = self.avg + round(self.avg / 2)

    def add(self, file: File):
        self.files.append(file)
        self.compute_bounds()

    def fits(self, file_: File) -> bool:
        return file_.size >= self.min and file_.size <= self.max

    def size(self) -> int:
        return len(self.files)

    def oldest(self, n: int) -> List[File]:
        files = sorted(self.files, key=lambda file_: file_.index)
        return files[:n]


def compute_buckets(path: str) -> List[Bucket]:
    # collect the files
    all_files: List[File] = []
    with scandir(path) as files:
        for file in files:
            match = SEGMENT_PATTERN.search(file.name)
            if match:
                index = int(match.group("index"))
                size = file.stat().st_size
                all_files.append(File(file.path, size, index))
    # bucket the files
    small_bucket = Bucket()
    buckets = [small_bucket]
    for file_ in all_files:
        if file_.size <= MIN_SIZE:
            small_bucket.add(file_)
        else:
            file_bucket = None
            for bucket in buckets:
                if bucket.fits(file_):
                    file_bucket = bucket
                    break
            if file_bucket is None:
                new_bucket = Bucket()
                new_bucket.add(file_)
                buckets.append(new_bucket)
            else:
                file_bucket.add(file_)
            buckets.sort(key=lambda b: b.avg)
    return buckets


def merge(in_files: List[File]) -> File:
    oldest = in_files[0].path.replace(".dat", "")
    readers = list(filter(lambda r: r.has_next, (Entries(file) for file in in_files)))
    with kv_writer(f"{oldest}-compacted.dat") as writer:
        while readers:
            min_reader = min(
                readers,
                key=lambda r: (r.current_pair[0], r.file.index * -1),
            )
            for reader in readers:
                if reader is min_reader:
                    continue
                if reader.current_pair[0] == min_reader.current_pair[0]:
                    reader.advance()
            if min_reader.current_pair[1] is not TOMBSTONE:
                writer.write_entry(*min_reader.current_pair)
            min_reader.advance()
            readers = [reader for reader in readers if reader.has_next]

    return File(f"{oldest}-compacted.dat", -1, 0)


def compaction_pass(buckets: List[Bucket]) -> Optional[Tuple[List[File], File]]:
    for bucket in buckets:
        if bucket.size() >= MIN_THRESHOLD:
            old_files = bucket.oldest(MIN_THRESHOLD)
            new_file = merge(old_files)
            return (old_files, new_file)
    return None


#########


def describe_buckets(buckets):
    n = 1
    for b in buckets:
        compaction = "yes" if b.size() >= MIN_THRESHOLD else "no"
        print(
            f"Tier {n} | min {b.min} avg {b.avg} max {b.max} [needs compact {compaction}]"
        )
        for f in b.files:
            print(f" {f.path} {f.size} bytes")
        n += 1
