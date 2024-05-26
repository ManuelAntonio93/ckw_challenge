from confluent_kafka import Consumer, KafkaException
import json

c = Consumer({
    'bootstrap.servers': '172.24.0.3:9093',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['mytopic'])

while True:
    try:
        msg = c.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            data = json.loads(msg.value())
            dummy = 1
    except KeyboardInterrupt:
        break

c.close()