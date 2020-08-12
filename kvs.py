from memtable import Memtable
from readerwriterlock import rwlock
from segments import Segments
from commitlog import CommitLog

BF_HASH_COUNT = 5
MT_MAX_SIZE = 5000000


class KVS:
    def __init__(self, segments_path, log_path):
        self.rwlock = rwlock.RWLockFairD()
        self.commitlog, self.memtable = CommitLog(log_path).resume(log_path)
        self.segments = Segments(segments_path)

    def get(self, k):
        with self.rwlock.gen_rlock():
            r = self.memtable.get(k)
            if r is None:
                return self.segments.search(k)
            else:
                return r

    def set(self, k, v):
        with self.rwlock.gen_wlock():
            if self.memtable.approximate_bytes() <= MT_MAX_SIZE:
                self.commitlog.record_set(k, v)
                self.memtable.set(k, v)
            else:
                self.commitlog.record_set(k, v)
                self.memtable.set(k, v)
                self.segments.flush(self.memtable)
                self.memtable = Memtable()

    def unset(self, k):
        with self.rwlock.gen_wlock():
            self.commitlog.record_unset(k)
            self.memtable.unset(k)
