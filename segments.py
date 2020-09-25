from dataclasses import dataclass
from os import scandir, mkdir, path, remove, rename
from re import compile as compile_re
from threading import RLock, Thread
from queue import Queue
from readerwriterlock import rwlock
from binio import kv_iter
from bloomfilter import BloomFilter
from sstable import SSTable
from compaction import compaction_pass, compute_buckets

SEGMENT_TEMPLATE = 'segment-%d.dat'
SEGMENT_PATTERN = compile_re("segment-(?P<index>\d+)\.dat")

COMPACT_EXIT = 0
COMPACT_REQUIRED = 1


class Segments:

    @dataclass
    class CompactionThread:
        queue: Queue
        thread: Thread
        
        def notify(self):
            self.queue.put(COMPACT_REQUIRED)

        def get_task(self):
            return self.queue.get()

        def task_done(self):
            self.queue.task_done()

        def start(self):
            if not self.thread.is_alive():
                self.thread.start()

        def stop(self):
            self.queue.put(COMPACT_EXIT)
            self.thread.join()
            self.queue.join()

    def __init__(self, segment_dir):
        self.compaction_lock = RLock()
        self.compaction_thread = None
        self.segment_lock = rwlock.RWLockWrite()
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
            self._notify_compaction_thread()

    def search(self, k):
        with self.segment_lock.gen_rlock():
            for segment in self.segments:
                value = segment.search(k)
                if value is not None:
                    return value
        return None

    def compact(self):
        with self.compaction_lock:
            with self.segment_lock.gen_rlock():
                buckets = compute_buckets(self.segments)
            old_segments, new_segment = compaction_pass(buckets)
            if new_segment is None:
                return
            # update the in-memory segments list
            new_segments = []
            updated = False
            old_indexes = set(f.index for f in old_segments)
            with self.segment_lock.gen_wlock():
                for segment in self.segments:
                    if not updated and segment.index <= new_segment.index:
                        new_segments.append(new_segment)
                        updated = True
                    if segment.index not in old_indexes:
                        new_segments.append(segment)
                # delete the old segments from disk
                for old_segment in old_segments:
                    remove(old_segment.path)
                # fix the new segment's path
                updated_path = new_segment.path.replace("-compacted", "")
                rename(new_segment.path, updated_path)
                new_segment.path = updated_path
                # update the segments
                self.segments = new_segments

    def _compaction_loop(self):
        while self.compaction_thread.get_task():
            with self.compaction_lock:
                self.compact()
                self.compaction_thread.task_done()
        self.compaction_thread.task_done()

    def _notify_compaction_thread(self):
        if self.compaction_thread:
            self.compaction_thread.notify()

    def start_compaction_thread(self):
        if not self.compaction_thread:
            worker = Thread(target=self._compaction_loop, daemon=True)
            self.compaction_thread = Segments.CompactionThread(Queue(), worker)
            self.compaction_thread.start()

    def stop_compaction_thread(self, graceful=True):
        if self.compaction_thread:
            if graceful:
                self.compaction_thread.stop()
            self.compaction_thread = None

