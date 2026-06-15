import urllib.request, threading, time, sys

IP = '101.46.65.2'
TOTAL = 10000
CONCURRENT = 200

done = [0]
lock = threading.Lock()

def worker():
    while True:
        try:
            with urllib.request.urlopen(f'http://{IP}/api/data', timeout=5) as r:
                with lock:
                    done[0] += 1
                    if done[0] >= TOTAL:
                        return
        except:
            pass

start = time.time()
threads = [threading.Thread(target=worker, daemon=True) for _ in range(CONCURRENT)]
for t in threads:
    t.start()

while done[0] < TOTAL:
    time.sleep(1)
    with lock:
        print(f'\r已完成: {done[0]}/{TOTAL}', end='')
        sys.stdout.flush()

elapsed = time.time() - start
print(f'\n完成 {TOTAL} 请求, 耗时 {elapsed:.1f}s, QPS: {TOTAL/elapsed:.0f}')
