from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] kafka-consumer: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("kafka-consumer")

# Connect to Kafka broker
consumer = KafkaConsumer(
    'post_impressions',
    'post_engagements',
    'post_conversions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='analytics_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Setup MongoDB connection
mongo_client = MongoClient('mongodb://root:admin@mongo:27017/')
db = mongo_client['social_media_analytics']
stats_collection = db['post_stats']
history_collection = db['conversion_rate_history']

# In-memory stats dictionary
stats = {}

def update_stats(post_id, event_type):
    if post_id not in stats:
        stats[post_id] = {'impressions': 0, 'engagements': 0, 'conversions': 0}

    if event_type == 'impression':
        stats[post_id]['impressions'] += 1
    elif event_type == 'engagement':
        stats[post_id]['engagements'] += 1
    elif event_type == 'conversion':
        stats[post_id]['conversions'] += 1

try:
    for message in consumer:
        event = message.value
        topic = message.topic

        logger.info(f"Consumed from {topic}: {event}")

        post_id = event.get('post_id')
        if not post_id:
            continue

        if topic == 'post_impressions':
            update_stats(post_id, 'impression')
        elif topic == 'post_engagements':
            update_stats(post_id, 'engagement')
        elif topic == 'post_conversions':
            update_stats(post_id, 'conversion')

        # Update stats and calculate conversion rate
        s = stats[post_id]
        conversion_rate = s['conversions'] / s['impressions'] if s['impressions'] > 0 else 0

        # Save updated post stats
        stats_collection.update_one(
            {'post_id': post_id},
            {'$set': {
                'impressions': s['impressions'],
                'engagements': s['engagements'],
                'conversions': s['conversions'],
                'conversion_rate': conversion_rate,
                'last_updated': time.time()
            }},
            upsert=True
        )

        # Calculate and log average conversion rate across all posts
        valid_posts = [v for v in stats.values() if v['impressions'] > 0]
        if valid_posts:
            avg_conversion_rate = sum(p['conversions'] / p['impressions'] for p in valid_posts) / len(valid_posts)
            logger.info(f"Average Conversion Rate: {avg_conversion_rate:.4f}")

            # Store average conversion rate over time
            history_collection.insert_one({
                'timestamp': time.time(),
                'average_conversion_rate': avg_conversion_rate
            })

except KeyboardInterrupt:
    logger.info("Stopping consumer...")

finally:
    consumer.close()
