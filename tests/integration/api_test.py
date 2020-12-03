import concurrent.futures as ThreadPoolExecutor
from requests_futures.sessions import FuturesSession
import random

base_url = "http://127.0.0.1:8000"
timeout=20000
max_workers=10
session = FuturesSession(max_workers=max_workers)
futures = []
existing_keys=set()

def get():
    url = base_url + f"/kvs/{random.choice(tuple(existing_keys))}"
    return session.get(url)

def put(key, value):
    existing_keys.add(key)
    url = base_url + "/kvs"
    params = {
        f'key':f'{key}',
        f'value':f'{value}'
    }
    return session.put(url, params=params)

def run_requests_async(count):
    for i in range(count):
        _get=lambda: get()
        _put=lambda: put(i, i*2)

        if(len(existing_keys) == 0):
            func=_put
        else:
            [func]=random.choices(
                population=[_get, _put],
                weights=[0.8, 0.2],
                k=1
            )

        futures.append(func())

    for future in ThreadPoolExecutor.as_completed(futures, timeout=timeout):
        print(future.result())

if __name__ == "__main__":
    count=10000
    run_requests_async(count)