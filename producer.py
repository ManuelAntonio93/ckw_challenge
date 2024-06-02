from confluent_kafka import Producer
import time
import json
import random


class MessageProducer:
    def __init__(self, p, device_id, signal_name, value_min, value_max):
        self.p = p
        self.device_id = device_id
        self.signal_name = signal_name
        self.value_min = value_min
        self.value_max = value_max

    def delivery_report(self, err, msg):
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {}".format(msg.topic()))

    def send_message(self):
        data = {
            "t_measurement": time.time(),
            "t_report": time.time() - 10,
            "device_id": self.device_id,
            "signal_name": self.signal_name,
            "value": random.randint(self.value_min, self.value_max),
        }
        self.p.produce("mytopic", json.dumps(data), callback=self.delivery_report)
        self.p.flush()


p = Producer({"bootstrap.servers": "192.168.1.72:9093"})

device1_exp = MessageProducer(p, "device1", "energy export", 50, 70)
device1_imp = MessageProducer(p, "device1", "energy import", 10, 20)
device2_exp = MessageProducer(p, "device2", "energy export", 80, 90)
device2_imp = MessageProducer(p, "device2", "energy import", 0, 10)
devices = [device1_exp, device1_imp, device2_exp, device2_imp]

while True:
    for device in devices:
        if random.randint(0,1) == 1:
            device.send_message()
    time.sleep(10)
