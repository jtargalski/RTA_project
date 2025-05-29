from kafka import KafkaProducer
import json
import time
import random
import logging

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

post_ids = [f'post_{i}' for i in range(1, 11)]
user_ids = [f'user_{i}' for i in range(1, 101)]

logging.basicConfig(level=logging.INFO)

def produce_event(topic, event):
    logging.info(f"Producing to {topic}: {event}")
    producer.send(topic, event)
    producer.flush()

try:
    while True:
        # Impression event
        impression_event = {
            'post_id': random.choice(post_ids),
            'user_id': random.choice(user_ids),
            'timestamp': time.time()
        }
        produce_event('post_impressions', impression_event)

        # Random engagement event
        if random.random() < 0.3:
            engagement_event = {
                'post_id': impression_event['post_id'],
                'user_id': impression_event['user_id'],
                'engagement_type': random.choice(['like', 'comment', 'share']),
                'timestamp': time.time()
            }
            produce_event('post_engagements', engagement_event)

        # Random conversion event with variable probability between 5% and 20%
        conversion_probability = random.uniform(0.05, 0.20)
        if random.random() < conversion_probability:
            conversion_event = {
                'post_id': impression_event['post_id'],
                'user_id': impression_event['user_id'],
                'conversion_type': 'click',
                'timestamp': time.time()
            }
            produce_event('post_conversions', conversion_event)

        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping producer...")

producer.close()
