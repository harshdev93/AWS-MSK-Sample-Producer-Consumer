from confluent_kafka import Producer
from faker import Faker
import json

p = Producer({'bootstrap.servers': 'b-2.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092,b-1.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092,b-3.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092'})

def delivery_report(err, msg):

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


fake = Faker('en_US')

def gen_ran_data(i):
    data = {}
    data["ID"] = i
    data["name"] = fake.name()
    data["address"] = fake.address()
    data["Email-ID"] = fake.safe_email()
    return data

for i in range(0, 1000):
    x = json.dumps(gen_ran_data(i))
    print(x)
    p.poll(0)
    p.produce('sampleTopic', x.encode('utf-8'), callback=delivery_report)

p.flush()
