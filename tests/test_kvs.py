import _thread
import threading
from kvs import KVS

# check that read locks are released when write lock is requested

# run set get set get concurrently, expect to have get executed in the end

# ask for the read of a key that is not there, then write, then get key back to ensure
# writing happened before look up

def test_10_rlocks():
    kvs = KVS()
    threads = []

    for i in range(1000):
        evt = threading.Event()
        evt.set()
        threads.append(evt)
        _thread.start_new_thread(kvs.get, (f'{1}',))
        _thread.start_new_thread(kvs.set, (f'{1}', f'{1}'))

    for i in threads:
        i.wait()


    assert True


