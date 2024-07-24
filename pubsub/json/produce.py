from confluent_kafka import Producer
from datetime import datetime
import random
import json
import uuid
from config import TOPIC

def produce():
    # Configure the Producer
    p = Producer({
        'bootstrap.servers': 'localhost:19092',  # Assuming you're running this on the same machine as the compose
        'client.id': 'python-producer'
    })

    # Produce a message
    try:
        while True:
            stock = {
                "event_time": "2023-11-30T14:43:29.700245",
                "ticker": "AMZN",
                "price": 74.95                
            }
           
            p.produce(TOPIC, key=str(uuid.uuid4), value=json.dumps(stock), callback=delivery_report)
    except Exception as e:
        print(str(e))

    # Wait for any outstanding messages to be delivered
    p.flush()

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    produce()

if __name__ == "__main__":
    main()
    
