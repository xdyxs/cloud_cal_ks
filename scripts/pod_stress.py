import urllib.request, threading, time

host = 'http://backend-svc/api/data'
done = [0]
lock = threading.Lock()

def work():
    while done[0] < 10000:
        try:
            urllib.request.urlopen(urllib.request.Request(host), timeout=3)
            with lock:
                done[0] += 1
        except:
            pass

for _ in range(100):
    threading.Thread(target=work, daemon=True).start()

while done[0] < 10000:
    time.sleep(1)

print(f'完成 10000 请求')
