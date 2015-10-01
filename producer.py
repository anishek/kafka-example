from kafka import SimpleProducer, KafkaClient
import time
import json
import threading


class Produce:
    """
    ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic first --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.2 --config retention.ms=5
    """

    def __init__(self, threads=1):
        self.threads = threads

    def run(self):
        active_threads = []
        for x in range(0, self.threads):
            current = threading.Thread(name=x, target=worker, args=(1000,))
            active_threads.append(current)
            current.start()
        for th in active_threads:
            th.join()


def worker(ids=1000):
    n = threading.currentThread().getName()
    print 'started: ', n
    client = KafkaClient("localhost:9092")
    producer = SimpleProducer(client)

    for x in range(0, ids):
        msg = dict()
        msg[n + x] = int(time.time() * 1000)
        producer.send_messages('first', json.dumps(msg))
    print 'finished: ', n


Produce(4).run()
