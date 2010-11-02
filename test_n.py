from queues import queues
import time
queue_name = 'test_queues_%.f' % time.time()
q = queues.Queue(queue_name)

n = 10000

for i in xrange(0, n):
    assert q.write(i)

assert len(q) == n

for i in xrange(0, n):
    assert q.read() == n-i-1

assert len(q) == 0
