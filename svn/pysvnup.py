import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

default_interval = 60 * 2
max_workers = 20


class Monitor:

    def __init__(self, interval, cb):
        super().__init__()
        self.interval = interval
        self.cb = cb
        self.t = None
        self.running = True

    def poll(self):
        while self.running:
            t = time.time()
            if self.t is None:
                self.t = t
            else:
                if t - self.t > self.interval:
                    self.t = t
                    self.cb()

    def done(self):
        print("done")
        self.running = False


class SvnTask:
    def __init__(self, f):
        super().__init__()
        self.f = f
        self._valid = os.path.exists(f)
        self._done = False
        self._m = None

    def valid(self):
        return os.path.exists(self.f)

    def svnup(self):
        if self.valid():
            os.system('svn up {}'.format(self.f))
        else:
            self.done()

    def run(self):
        self._m = Monitor(default_interval, self.svnup)
        self._m.poll()

    def done(self):
        print('done task ', self.f)
        self._done = True
        self._m.done()

    def __str__(self) -> str:
        return 'task: ' + self.f


def parse_config():
    txt = open('./config.txt')
    tasks = []
    with open('./config.txt', 'r') as f:
        for line in txt.readlines():
            if os.path.exists(line.strip()):
                tasks.append(SvnTask(line.strip()))
    return tasks


def loop():
    last = None
    pool = None
    tasks = []
    while True:
        if os.path.exists('./config.txt'):
            ctime = os.stat('./config.txt').st_mtime
            if last != ctime:
                last = ctime
                for task in tasks:
                    task.done()
                if pool is not None:
                    pool.shutdown()
                pool = ThreadPoolExecutor(max_workers)
                tasks = parse_config()
                print("submit new tasks")
                for task in tasks:
                    print(task)
                    pool.submit(task.run)


print("Start Run Svn Monitor")

t = threading.Thread(target=loop, name='loop')
t.start()
t.join()
