from confluent_kafka import Consumer, KafkaError
  

c = Consumer({
    'bootstrap.servers': 'b-2.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092,b-1.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092,b-3.xxxx.xxxx.xxxx.kafka.us-east-1.amazonaws.com:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})

c.subscribe(['sampleTopic'])

while True:
    msg = c.poll(0.1)

    if msg is None:
        print("No Data")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
