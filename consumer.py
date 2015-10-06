from kafka import KafkaConsumer
from kafka.common import ConsumerTimeout


class Consumer(object):
    def __init__(self, topic_name='first'):
        self.topic_name = topic_name

    def run(self):
        cons = KafkaConsumer(bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)
        cons.set_topic_partitions(('first', 0, 0), ('first', 1, 0))
        count = 0
        try:
            line = cons.next()
            res = dict({0: 0, 1: 0})
            while line:
                res[line.partition] += 1
                line = cons.next()
        except ConsumerTimeout:
            print 'done fetching'
        for k in res.keys():
            print 'messages:', k, ' : ', res[k]


Consumer().run()
