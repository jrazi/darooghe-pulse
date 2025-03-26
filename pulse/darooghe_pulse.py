import os
import time
import random
import uuid
import json
import datetime
import logging
from datetime import timedelta
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Constants for event generation.
MERCHANT_CATEGORIES = [
    "retail",
    "food_service",
    "entertainment",
    "transportation",
    "government",
]
PAYMENT_METHODS = ["online", "pos", "mobile", "nfc"]
COMMISSION_TYPES = ["flat", "progressive", "tiered"]
CUSTOMER_TYPES = ["individual", "CIP", "business"]
FAILURE_REASONS = ["cancelled", "insufficient_funds", "system_error", "fraud_prevented"]
DEVICE_INFO_LIBRARY = [
    {"os": "Android", "app_version": "2.4.1", "device_model": "Samsung Galaxy S25"},
    {"os": "iOS", "app_version": "3.1.0", "device_model": "iPhone 15"},
    {"os": "Android", "app_version": "1.9.5", "device_model": "Google Pixel 6"},
]


def generate_random_datetime(start, end):
    delta = end - start
    random_seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=random_seconds)


def generate_transaction_event(is_historical=False, timestamp_override=None):
    event_time = (
        timestamp_override if timestamp_override else datetime.datetime.utcnow()
    )

    transaction_id = str(uuid.uuid4())
    customer_id = f"cust_{random.randint(1, customer_count)}"
    merchant_id = f"merch_{random.randint(1, merchant_count)}"
    merchant_category = random.choice(MERCHANT_CATEGORIES)
    payment_method = random.choice(PAYMENT_METHODS)
    amount = random.randint(50000, 2000000)

    # Get a location within a distance from Tehran. !TODO : Naive approach, can be improved.
    base_lat = 35.7219
    base_lng = 51.3347
    location = {
        "lat": base_lat + random.uniform(-0.05, 0.05),
        "lng": base_lng + random.uniform(-0.05, 0.05),
    }
    device_info = (
        random.choice(DEVICE_INFO_LIBRARY)
        if payment_method in ["online", "mobile"]
        else {}
    )

    if random.random() < declined_rate:
        status = "declined"
        failure_reason = random.choice(FAILURE_REASONS)
    else:
        status = "approved"
        failure_reason = None

    risk_level = 5 if random.random() < fraud_rate else random.randint(1, 3)

    commission_type = random.choice(COMMISSION_TYPES)
    commission_amount = int(amount * 0.02)
    vat_amount = int(amount * 0.09)  # assuming 9% VAT (--\_(ツ)_/--)
    total_amount = amount + vat_amount

    event = {
        "transaction_id": transaction_id,
        "timestamp": event_time.isoformat() + "Z",
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "payment_method": payment_method,
        "amount": amount,
        "location": location,
        "device_info": device_info,
        "status": status,
        "commission_type": commission_type,
        "commission_amount": commission_amount,
        "vat_amount": vat_amount,
        "total_amount": total_amount,
        "customer_type": random.choice(CUSTOMER_TYPES),
        "risk_level": risk_level,
        "failure_reason": failure_reason,
    }
    return event


def delivery_report(err, msg):
    """Callback for delivery report. Useful for debugging."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_historical_events(producer, topic, count=20000):
    """Produce a batch of historical events spanning the last 7 days."""
    logging.info(f"Producing {count} historical events...")
    now = datetime.datetime.utcnow()
    start_time = now - timedelta(days=7)
    for _ in range(count):
        # Randomize event timestamp between (now - 7 days) and now.
        event_time = generate_random_datetime(start_time, now)
        event = generate_transaction_event(timestamp_override=event_time)
        producer.produce(topic, json.dumps(event), callback=delivery_report)
    producer.flush()
    logging.info("Historical events production completed.")


def continuous_event_production(producer, topic, base_rate):
    """
    Continuously produce events following a non-homogeneous Poisson process
    with a base rate (in events per minute) and adjusted during peak hours.
    """
    while True:
        # Determine if we're in peak hours (e.g., 9:00 to 18:00 UTC).
        current_hour = datetime.datetime.utcnow().hour
        multiplier = peak_factor if 9 <= current_hour < 18 else 1.0
        effective_rate = base_rate * multiplier
        # Lambda (per second) for the exponential distribution.
        lambda_per_sec = effective_rate / 60.0
        wait_time = random.expovariate(lambda_per_sec)
        time.sleep(wait_time)
        event = generate_transaction_event()
        producer.produce(topic, json.dumps(event), callback=delivery_report)
        producer.poll(0)


if __name__ == "__main__":
    EVENT_RATE = float(os.getenv("EVENT_RATE", 100))  # Per minute
    peak_factor = float(os.getenv("PEAK_FACTOR", 2.5))
    fraud_rate = float(os.getenv("FRAUD_RATE", 0.02))
    declined_rate = float(os.getenv("DECLINED_RATE", 0.05))
    merchant_count = int(os.getenv("MERCHANT_COUNT", 50))
    customer_count = int(os.getenv("CUSTOMER_COUNT", 1000))
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    topic = "darooghe.transactions"

    conf = {"bootstrap.servers": kafka_broker}
    producer = Producer(conf)

    # Step 1: Produce historical events.
    produce_historical_events(producer, topic, count=20000)

    # Step 2: Start continuous production.
    logging.info("Starting continuous event production...")
    continuous_event_production(producer, topic, base_rate=EVENT_RATE)
