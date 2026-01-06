import os
import time
import uuid
import random
from datetime import datetime

import pandas as pd


# Configuration

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "data", "input")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EVENTS_PER_FILE = 100
SLEEP_SECONDS = 5

EVENT_TYPES = ["view", "purchase"]


# Event Generator

def generate_event():
    event_type = random.choice(EVENT_TYPES)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "event_type": event_type,
        "product_id": random.randint(1, 500),
        "price": round(random.uniform(5, 500), 2) if event_type == "purchase" else 0.0,
        "event_time": datetime.utcnow().isoformat()
    }


# Main Loop

def main():
    print("Starting e-commerce event generator...")

    while True:
        events = [generate_event() for _ in range(EVENTS_PER_FILE)]
        df = pd.DataFrame(events)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"events_{timestamp}_{uuid.uuid4().hex[:6]}.csv"

        temp_path = os.path.join(OUTPUT_DIR, f".{file_name}")
        final_path = os.path.join(OUTPUT_DIR, file_name)

        # Atomic write (safe for Spark + Windows)
        df.to_csv(temp_path, index=False)
        os.replace(temp_path, final_path)

        print(f"Generated {EVENTS_PER_FILE} events → {file_name}")

        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
