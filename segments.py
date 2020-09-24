import logging
from os import scandir, mkdir, path, remove, rename
from re import compile as compile_re
from threading import Lock, Thread
from queue import Queue
from readerwriterlock import rwlock
from binio import kv_iter
from bloomfilter import BloomFilter
from sstable import SSTable

SEGMENT_TEMPLATE = 'segment-%d.dat'
SEGMENT_PATTERN = compile_re("segment-(?P<index>\d+)\.dat")

COMPACT_EXIT = 0
COMPACT_REQUIRED = 1

from compaction import compaction_pass, compute_buckets

class Segments:
    def __init__(self, segment_dir):
        self.compaction_lock = Lock()
        self.compaction_thread = None
        self.compaction_queue = None
        self.segment_lock = rwlock.RWLockFairD()
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
        with self.segment_lock.gen_wlock():
            index = 0
            if len(self.segments):
                index = self.segments[0].index + 1
            path = f"{self.segment_dir}/{SEGMENT_TEMPLATE % index}"
            sstable = SSTable.create(path, index, memtable)
            self.segments.insert(0, sstable)
            self.compaction_queue.put(COMPACT_REQUIRED)

    def search(self, k):
        with self.segment_lock.gen_rlock():
            for segment in self.segments:
                value = segment.search(k)
                if value is not None:
                    return value
        return None

    def compact(self):
        with self.compaction_lock:
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
                with self.segment_lock.gen_wlock():
                    self.segments = new_segments
                # delete files
                for f in old_files:
                    remove(f.path)
                # rename new_file
                new_file_path = new_file.path.replace("-compacted", "")
                rename(new_file.path, new_file_path)

    def _compaction_loop(self):
        while self.compaction_queue.get():
            logging.info("running compaction from compaction loop")
            self.compact()
            self.compaction_queue.task_done()
        self.compaction_queue.task_done()

    def start_compaction_daemon(self):
        with self.compaction_lock:
            if not self.compaction_queue:
                self.compaction_queue = Queue()
                self.compaction_thread = Thread(target=self._compaction_loop, daemon=True)
                self.compaction_thread.start()

    def stop_compaction_daemon(self, graceful=True):
        with self.compaction_lock:
            if self.compaction_queue:
                if graceful:
                    self.compaction_queue.put(COMPACT_EXIT)
                    self.compaction_thread.join()
                    self.compaction_queue.join()
                self.compaction_thread = None
                self.compaction_queue = None

