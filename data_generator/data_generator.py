import os
import time
import uuid
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd



# LOGGING CONFIGURATION 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "..", "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Log file path - creates a new file each day
LOG_FILE = os.path.join(LOG_DIR, f"data_generator_{datetime.now().strftime('%Y%m%d')}.log")

# Configure logging to BOTH file AND console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE),      # Write to file
        logging.StreamHandler()              # Also print to console
    ]
)
logger = logging.getLogger("DataGenerator")

# CONFIGURATION


OUTPUT_DIR = os.path.join(BASE_DIR, "..", "data", "input")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EVENTS_PER_FILE = 100
SLEEP_SECONDS = 5

# Extended event types for richer analytics
EVENT_TYPES = ["view", "purchase", "add_to_cart", "remove_from_cart", "wishlist", "search"]
EVENT_WEIGHTS = [0.50, 0.10, 0.15, 0.05, 0.10, 0.10]  # Realistic distribution

# Product catalog with categories for enrichment
PRODUCT_CATALOG = {
    "electronics": list(range(1, 101)),
    "clothing": list(range(101, 201)),
    "home_garden": list(range(201, 301)),
    "sports": list(range(301, 401)),
    "books": list(range(401, 501)),
}

# Price ranges by category
PRICE_RANGES = {
    "electronics": (50, 2000),
    "clothing": (10, 300),
    "home_garden": (15, 500),
    "sports": (20, 800),
    "books": (5, 100),
}

# User segments for enrichment
USER_SEGMENTS = ["new", "returning", "premium", "inactive"]
USER_SEGMENT_WEIGHTS = [0.20, 0.50, 0.15, 0.15]

# Anomaly injection rates for testing
ANOMALY_RATE = 0.02  # 2% of events will have anomalies


def get_product_category(product_id: int) -> str:
    #Map product ID to category.
    for category, products in PRODUCT_CATALOG.items():
        if product_id in products:
            return category
    return "unknown"


def generate_session_id(user_id: Optional[int]) -> str:
    #Generate session ID based on user and time window.
    # Sessions are ~30 min windows
    time_bucket = int(datetime.now(timezone.utc).timestamp() // 1800)
    if user_id is None:
        # Anonymous session - use random guest ID
        return f"guest-{time_bucket}-{random.randint(10000, 99999)}"
    return f"{user_id}-{time_bucket}"


def generate_event(
    inject_anomalies: bool = True,
    force_event_type: Optional[str] = None
) -> dict:
    
    # Select event type (weighted random or forced)
    if force_event_type:
        event_type = force_event_type
    else:
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Business logic: Actions that require login
    ACTIONS_REQUIRING_LOGIN = ["purchase", "add_to_cart", "wishlist", "remove_from_cart"]
    
    # Generate user_id based on event type (realistic behavior)
    if event_type in ACTIONS_REQUIRING_LOGIN:
        # These actions ALWAYS have a user_id (user must be logged in)
        user_id = random.randint(1, 1000)
    else:
        # View and search can be anonymous (10% chance of no user_id = guest browsing)
        if random.random() < 0.1:
            user_id = None  # Anonymous/guest user
        else:
            user_id = random.randint(1, 1000)
    
    # Select category first, then product (for category-aware pricing)
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    product_id = random.choice(PRODUCT_CATALOG[category])
    
    # Generate user segment (only for logged-in users)
    if user_id is not None:
        user_segment = random.choices(USER_SEGMENTS, weights=USER_SEGMENT_WEIGHTS, k=1)[0]
    else:
        user_segment = "anonymous"
    
    # Generate price based on event type and category
    if event_type == "purchase":
        price_range = PRICE_RANGES.get(category, (5, 500))
        price = round(random.uniform(*price_range), 2)
        quantity = random.randint(1, 5)
    elif event_type == "add_to_cart":
        price_range = PRICE_RANGES.get(category, (5, 500))
        price = round(random.uniform(*price_range), 2)
        quantity = random.randint(1, 3)
    else:
        price = 0.0
        quantity = 0
    
    # Generate timestamp (mostly now, sometimes late for watermark testing)
    if random.random() < 0.05:  # 5% late events for watermark testing
        late_minutes = random.randint(1, 10)
        event_time = datetime.now(timezone.utc) - timedelta(minutes=late_minutes)
    else:
        event_time = datetime.now(timezone.utc)
    
    # Generate search query for search events
    search_query = None
    if event_type == "search":
        sample_queries = ["laptop", "shoes", "book", "garden tools", "headphones", "t-shirt"]
        search_query = random.choice(sample_queries)
    
    # Build base event
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": generate_session_id(user_id),
        "event_type": event_type,
        "product_id": product_id,
        "category": category,
        "price": price,
        "quantity": quantity,
        "user_segment": user_segment,
        "search_query": search_query if search_query else "",
        "event_time": event_time.isoformat(),
        "source_system": "web",  # For data lineage
    }
    
    # Inject anomalies for testing data quality logic
    if inject_anomalies and random.random() < ANOMALY_RATE:
        anomaly_type = random.choice([
            "null_user", "negative_price", "future_timestamp", 
            "invalid_event_type", "extreme_price"
        ])
        
        if anomaly_type == "null_user":
            event["user_id"] = None
        elif anomaly_type == "negative_price":
            event["price"] = -abs(event["price"]) if event["price"] else -10.0
        elif anomaly_type == "future_timestamp":
            event["event_time"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif anomaly_type == "invalid_event_type":
            event["event_type"] = "INVALID_TYPE"
        elif anomaly_type == "extreme_price":
            event["price"] = 99999.99
        
        event["_anomaly_type"] = anomaly_type  # For test validation
    else:
        event["_anomaly_type"] = ""
    
    return event


# Main Loop

def generate_batch(num_events: int = EVENTS_PER_FILE, inject_anomalies: bool = True) -> pd.DataFrame:
    #Generate a batch of events as a DataFrame.
    events = [generate_event(inject_anomalies=inject_anomalies) for _ in range(num_events)]
    return pd.DataFrame(events)


def write_batch_to_file(df: pd.DataFrame, output_dir: str = OUTPUT_DIR) -> str:
    #Write a DataFrame to a CSV file atomically. Returns the file path.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"events_{timestamp}_{uuid.uuid4().hex[:6]}.csv"
    
    temp_path = os.path.join(output_dir, f".{file_name}")
    final_path = os.path.join(output_dir, file_name)
    
    # Remove internal anomaly tracking column before saving
    df_to_save = df.drop(columns=["_anomaly_type"], errors="ignore")
    
    try:
        # Atomic write (safe for Spark + Windows)
        df_to_save.to_csv(temp_path, index=False)
        os.replace(temp_path, final_path)
        return final_path
    except Exception as e:
        logger.error(f" Failed to write file: {str(e)}")
        raise


def main():
    logger.info("=" * 70)
    logger.info(" E-COMMERCE EVENT GENERATOR STARTED")
    logger.info("=" * 70)
    logger.info(f" Output directory: {OUTPUT_DIR}")
    logger.info(f" Log file: {LOG_FILE}")
    logger.info(f" Events per file: {EVENTS_PER_FILE}")
    logger.info(f" Interval: {SLEEP_SECONDS} seconds")
    logger.info(f" Event types: {EVENT_TYPES}")
    logger.info(f" Anomaly rate: {ANOMALY_RATE * 100}%")
    logger.info("=" * 70)
    
    # Verify output directory is writable
    try:
        test_file = os.path.join(OUTPUT_DIR, ".test_write")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info(" Output directory: WRITABLE")
    except Exception as e:
        logger.error(f" Output directory: NOT WRITABLE - {str(e)}")
        return
    
    logger.info("=" * 70)
    print("\nPress Ctrl+C to stop\n")

    batch_count = 0
    total_events = 0
    failed_batches = 0
    
    try:
        while True:
            batch_count += 1
            
            try:
                df = generate_batch(EVENTS_PER_FILE)
                file_path = write_batch_to_file(df)
                total_events += len(df)
                
                # Log statistics
                event_type_counts = df["event_type"].value_counts().to_dict()
                anomaly_count = (df["_anomaly_type"] != "").sum()
                
                logger.info("-" * 50)
                logger.info(f" [Batch {batch_count}] SUCCESS")
                logger.info(f"   File: {os.path.basename(file_path)}")
                logger.info(f"   Events: {len(df)}")
                logger.info(f"   Distribution: {event_type_counts}")
                if anomaly_count > 0:
                    logger.info(f"   Anomalies: {anomaly_count}")
                logger.info(f"   Running total: {total_events} events")
                
            except Exception as e:
                failed_batches += 1
                logger.error("-" * 50)
                logger.error(f" [Batch {batch_count}] FAILED")
                logger.error(f"   Error: {str(e)}")
                logger.error(f"   Failed batches so far: {failed_batches}")

            time.sleep(SLEEP_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("=" * 70)
        logger.info(" GENERATOR STOPPED BY USER")
        logger.info("=" * 70)
        logger.info(" FINAL STATISTICS:")
        logger.info(f"   Successful batches: {batch_count - failed_batches}")
        logger.info(f"   Failed batches: {failed_batches}")
        logger.info(f"   Total events generated: {total_events}")
        logger.info("=" * 70)


if __name__ == "__main__":
    main()
