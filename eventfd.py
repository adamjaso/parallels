import time, os, sys, random
done = bytes([1, 0, 0, 0, 0, 0, 0, 0])
worker_eventfd = int(os.getenv('WORKER_EVENTFD'))
worker_index = os.getenv('WORKER_INDEX')
for line in sys.stdin:
    t = 1 + random.randint(0, 5)
    print('working {}s {}...'.format(t, worker_index))
    time.sleep(t)
    print('work ... {}'.format(line))
    time.sleep(1)
    os.write(worker_eventfd, done)
