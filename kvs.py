from memtable import Memtable
from readerwriterlock import rwlock
from segments import Segments
from commitlog import CommitLog

BF_HASH_COUNT = 5
MT_MAX_SIZE = 5000000


class KVS:
    def __init__(self, segments_path, log_path):
        self.rwlock = rwlock.RWLockFairD()
        self.commitlog, self.memtable = CommitLog.resume(log_path)
        self.segments = Segments(segments_path)

    def get(self, k):
        with self.rwlock.gen_rlock():
            return v if (v := self.memtable.get(k)) else self.segments.search(k)

    def set(self, k, v):
        with self.rwlock.gen_wlock():
            self.commitlog.record_set(k, v)
            self.memtable.set(k, v)
            if self.memtable.approximate_bytes() >= MT_MAX_SIZE:
                self.renew_memtable()

    def unset(self, k):
        with self.rwlock.gen_wlock():
            self.commitlog.record_unset(k)
            self.memtable.unset(k)
            if self.memtable.approximate_bytes() >= MT_MAX_SIZE:
                self.renew_memtable()

    def renew_memtable(self):
        self.segments.flush(self.memtable)
        self.memtable = Memtable()
        self.commitlog.purge()
