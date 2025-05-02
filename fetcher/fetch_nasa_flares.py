import os
import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# --- Configuration ---
NASA_API_KEY = os.environ.get('NASA_API_KEY', 'DEMO_KEY') # Get API key from environment variable
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092') # Get Kafka broker address
KAFKA_TOPIC = 'nasa_flares' # Kafka topic to publish data to
FETCH_INTERVAL_SECONDS = int(os.environ.get('FETCH_INTERVAL_SECONDS', 3600)) # Check every hour by default
API_ENDPOINT = 'https://api.nasa.gov/DONKI/FLR' # NASA DONKI Solar Flare API endpoint

# --- Kafka Producer Setup ---
# Retry connection logic for robustness
producer = None
retry_delay = 5
max_retries = 10
retries = 0
while producer is None and retries < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(','), # Handle potential list of brokers
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize messages to JSON bytes
            retries=5, # Retry sending messages if it fails
            linger_ms=100 # Batch messages slightly for efficiency
        )
        print(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
    except Exception as e:
        retries += 1
        print(f"Error connecting to Kafka (attempt {retries}/{max_retries}): {e}. Retrying in {retry_delay}s...")
        time.sleep(retry_delay)

if producer is None:
    print(f"Failed to connect to Kafka after {max_retries} attempts. Exiting.")
    exit(1)


# --- Helper Function ---
def get_iso_date(days_ago=1):
    """Returns the date 'days_ago' in YYYY-MM-DD format."""
    date = datetime.utcnow() - timedelta(days=days_ago)
    return date.strftime('%Y-%m-%d')

# --- Main Fetching Loop ---
def fetch_and_send_data():
    """Fetches solar flare data from NASA API and sends it to Kafka."""
    start_date = get_iso_date(1) # Fetch data for the last day
    end_date = get_iso_date(0)   # Up to today

    params = {
        'startDate': start_date,
        'endDate': end_date,
        'api_key': NASA_API_KEY
    }

    print(f"Fetching solar flare data from {start_date} to {end_date}...")

    try:
        response = requests.get(API_ENDPOINT, params=params, timeout=30) # Added timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        flares = response.json()

        if not flares:
            print("No new flare data found in the specified period.")
            return

        print(f"Fetched {len(flares)} flare events.")

        # Send each flare event as a separate message to Kafka
        for flare in flares:
            try:
                # Use flrID as the key for potential partitioning/compaction later
                key = flare.get('flrID', f'flare_{time.time()}').encode('utf-8')
                producer.send(KAFKA_TOPIC, key=key, value=flare)
                # print(f"Sent flare {flare.get('flrID', 'N/A')} to Kafka topic '{KAFKA_TOPIC}'") # Verbose logging
            except Exception as e:
                print(f"Error sending flare {flare.get('flrID', 'N/A')} to Kafka: {e}")

        # Ensure all messages are sent before the next fetch cycle
        producer.flush()
        print(f"Successfully sent {len(flares)} flare(s) to Kafka.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from NASA API: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response from NASA API: {e}")
        print(f"Response text: {response.text[:500]}...") # Log part of the response text
    except Exception as e:
        print(f"An unexpected error occurred during fetch/send: {e}")


if __name__ == "__main__":
    if NASA_API_KEY == 'DEMO_KEY' or not NASA_API_KEY:
        print("Warning: Using DEMO_KEY or no API key provided. Rate limits are strict.")
    if not KAFKA_BROKER:
        print("Error: KAFKA_BROKER environment variable not set.")
        exit(1)

    print(f"Starting NASA Flare Fetcher. Checking every {FETCH_INTERVAL_SECONDS} seconds.")
    while True:
        fetch_and_send_data()
        print(f"Sleeping for {FETCH_INTERVAL_SECONDS} seconds...")
        time.sleep(FETCH_INTERVAL_SECONDS)
