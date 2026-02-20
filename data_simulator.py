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
SOURCE_FILE = os.path.join(BASE_DIR, "AirQualityUCI.csv")
DATA_DIR = os.path.join(BASE_DIR, config['paths']['data_dir'])

SENSOR_IDS = [
    "SENSOR_001", "SENSOR_002", "SENSOR_003",
    "SENSOR_004", "SENSOR_005"
]

LOCATIONS = ["Lab-A", "Lab-B", "Rooftop", "Basement", "Outdoor"]

def load_source_data():
    df = pd.read_csv(SOURCE_FILE, sep=';')
    df = df.drop(columns=['Unnamed: 15', 'Unnamed: 16'])
    df = df.dropna(subset=['Date', 'Time'])
    df['timestamp'] = df['Date'] + ' ' + df['Time']
    df = df.drop(columns=['Date', 'Time'])
    
    # Fix comma decimals AND replace -200 in one pass
    for col in ['CO(GT)', 'C6H6(GT)', 'T', 'RH', 'AH', 'NOx(GT)', 'NO2(GT)', 'NMHC(GT)']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.', regex=False), errors='coerce')
        df[col] = df[col].replace(-200, float('nan'))
    
    df = df.dropna(subset=['T'])
    df = df.reset_index(drop=True)
    return df

def simulate_sensors(df):
    df['sensor_id'] = [random.choice(SENSOR_IDS) for _ in range(len(df))]
    df['location'] = [random.choice(LOCATIONS) for _ in range(len(df))]
    return df

def introduce_corruption(df):
    df = df.copy()
    
    corrupt_indices = random.sample(range(len(df)), int(len(df) * 0.001))
    df.loc[corrupt_indices, 'sensor_id'] = None
    
    corrupt_indices2 = random.sample(range(len(df)), int(len(df) * 0.001))
    df.loc[corrupt_indices2, 'T'] = 999.0
    
    return df

def split_and_drop_files(df):
    chunk_size = config['pipeline']['chunk_size']
    poll_interval = config['pipeline']['poll_interval']
    
    chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
    print(f"Total chunks to drop: {len(chunks)}")
    
    for i, chunk in enumerate(chunks):
        filename = f"sensor_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.csv"
        filepath = os.path.join(DATA_DIR, filename)
        chunk.to_csv(filepath, index=False)
        print(f"Dropped file: {filename}")
        time.sleep(poll_interval)

def main():
    print("Loading source data...")
    df = load_source_data()
    
    print("Simulating sensors...")
    df = simulate_sensors(df)
    
    print("Introducing corruption...")
    df = introduce_corruption(df)
    
    print("Starting to drop files into data/ folder...")
    split_and_drop_files(df)
    
    print("All files dropped!")

if __name__ == "__main__":
    main()