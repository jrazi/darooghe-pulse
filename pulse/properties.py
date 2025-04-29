
import os
import random

MERCHANT_CATEGORIES = [
    "retail",
    "food_service",
    "entertainment",
    "transportation",
    "government",
]

MERCHANT_POPULATION_PER_CATEGORY = {
    "retail": {
        "weight": 2.0
    },
    "food_service": {
        "weight": 6.0
    },
    "entertainment": {
        "weight": 1.0
    },
    "transportation": {
        "weight": 1.0
    },
    "government": {
        "weight": 0.5
    }
}

PAYMENT_METHODS = ["online", "pos", "mobile", "nfc"]
COMMISSION_TYPES = ["flat", "progressive", "tiered"]
CUSTOMER_TYPES = ["individual", "CIP", "business"]
FAILURE_REASONS = ["cancelled", "insufficient_funds", "system_error", "fraud_prevented"]
DEVICE_INFO_LIBRARY = [
    {"os": "Android", "app_version": "2.4.1", "device_model": "Samsung Galaxy S25"},
    {"os": "iOS", "app_version": "3.1.0", "device_model": "iPhone 15"},
    {"os": "Android", "app_version": "1.9.5", "device_model": "Google Pixel 6"},
]

EVENT_RATE = float(os.getenv("EVENT_RATE", 100))
peak_factor = float(os.getenv("PEAK_FACTOR", 2.5))
fraud_rate = float(os.getenv("FRAUD_RATE", 0.02))
declined_rate = float(os.getenv("DECLINED_RATE", 0.05))
merchant_count = int(os.getenv("MERCHANT_COUNT", 50))
merchant_bases = {
    f"merch_{i}": {
        "lat": 35.7219 + random.uniform(-0.1, 0.1),
        "lng": 51.3347 + random.uniform(-0.1, 0.1),
    }
    for i in range(1, merchant_count + 1)
}
customer_count = int(os.getenv("CUSTOMER_COUNT", 1000))
kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = "darooghe.transactions"
event_init_mode = os.getenv("EVENT_INIT_MODE", "flush").lower()
skip_initial = False
