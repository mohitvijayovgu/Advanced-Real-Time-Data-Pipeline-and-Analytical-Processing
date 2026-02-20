import pandas as pd
import logging
from src.config_loader import load_config

config = load_config()
logger = logging.getLogger(__name__)

def parse_timestamp(df):
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H.%M.%S')
        logger.info('Timestamp Parsed Succesfully')
    except Exception as e:
        logger.error("Failed to parse timestamp: %s", str(e))
    return df

def convert_numeric_columns(df):
    numeric_columns = config['validation']['string_to_numeric']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("Numeric Conversion Completed")
    return df

def rename_columns(df):
    column_mapping = {
        'CO(GT)': 'co_gt',
        'PT08.S1(CO)': 'pt08_s1_co',
        'NMHC(GT)': 'nmhc_gt',
        'C6H6(GT)': 'c6h6_gt',
        'PT08.S2(NMHC)': 'pt08_s2_nmhc',
        'NOx(GT)': 'nox_gt',
        'PT08.S3(NOx)': 'pt08_s3_nox',
        'NO2(GT)': 'no2_gt',
        'PT08.S4(NO2)': 'pt08_s4_no2',
        'PT08.S5(O3)': 'pt08_s5_o3',
        'T': 'temperature',
        'RH': 'humidity',
        'AH': 'abs_humidity'
    }
    df = df.rename(columns=column_mapping)
    logger.info("Columns renamed successfully")
    return df

def process(df):
    logger.info("Starting data processing")
    
    df = parse_timestamp(df)
    df = convert_numeric_columns(df)
    df = rename_columns(df)
    
    logger.info("Data processing completed. Shape: %s", str(df.shape))
    return df

