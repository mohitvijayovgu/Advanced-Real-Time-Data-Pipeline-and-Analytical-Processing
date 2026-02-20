import pandas as pd
import os
import time
import random
from datetime import datetime
import sys
import yaml

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.config_loader import load_config

# Load config
config = load_config()

# Path configurations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_FILE = os.path.join(BASE_DIR, "iot_telemetry_data.csv")
DATA_DIR = os.path.join(BASE_DIR, config['paths']['data_dir'])

# Deterministic location mapping for the 3 known IoT devices
DEVICE_LOCATION_MAP = {
    'b8:27:eb:bf:9d:51': 'Lab-A',
    '00:0f:00:70:91:0a': 'Lab-B',
    '1c:bf:ce:15:ec:4d': 'Rooftop',
}

LOCATIONS = ["Lab-A", "Lab-B", "Rooftop"]

def load_source_data():
    """Load and normalise iot_telemetry_data.csv.

    Transformations:
      - ts (unix epoch float) -> timestamp (readable datetime string)
      - device (MAC address)  -> sensor_id
      - All numeric columns are already clean floats — no conversion needed.
    """
    df = pd.read_csv(SOURCE_FILE)

    # Convert unix epoch seconds to human-readable timestamp
    df['timestamp'] = pd.to_datetime(df['ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.drop(columns=['ts'])

    # Rename device -> sensor_id so the pipeline required-fields check passes
    df = df.rename(columns={'device': 'sensor_id'})

    df = df.reset_index(drop=True)
    return df

def add_location(df):
    """Attach a physical location label to each row using the device mapping."""
    df['location'] = df['sensor_id'].map(DEVICE_LOCATION_MAP).fillna(
        pd.Series([random.choice(LOCATIONS) for _ in range(len(df))], index=df.index)
    )
    return df

def introduce_corruption(df):
    """Inject a small fraction of realistic data quality issues."""
    df = df.copy()

    # ~0.1% of rows: null out sensor_id (simulates missing device label)
    corrupt_ids = random.sample(range(len(df)), max(1, int(len(df) * 0.001)))
    df.loc[corrupt_ids, 'sensor_id'] = None

    # ~0.1% of rows: inject physically impossible temperature (simulates sensor fault)
    corrupt_temp = random.sample(range(len(df)), max(1, int(len(df) * 0.001)))
    df.loc[corrupt_temp, 'temp'] = 999.0

    return df

def split_and_drop_files(df):
    chunk_size = config['pipeline']['chunk_size']
    poll_interval = config['pipeline']['poll_interval']

    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    print(f"Total chunks to drop: {len(chunks)}")

    os.makedirs(DATA_DIR, exist_ok=True)

    for i, chunk in enumerate(chunks):
        filename = f"iot_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.csv"
        filepath = os.path.join(DATA_DIR, filename)
        chunk.to_csv(filepath, index=False)
        print(f"Dropped file: {filename}")
        time.sleep(poll_interval)

def main():
    print("Loading source data (iot_telemetry_data.csv)...")
    df = load_source_data()

    print("Adding location labels...")
    df = add_location(df)

    print("Introducing corruption...")
    df = introduce_corruption(df)

    print(f"Dataset ready: {df.shape[0]} rows, {df.shape[1]} columns")
    print("Starting to drop files into data/ folder...")
    split_and_drop_files(df)

    print("All files dropped!")

if __name__ == "__main__":
    main()