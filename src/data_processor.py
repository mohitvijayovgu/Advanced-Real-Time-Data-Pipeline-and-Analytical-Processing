import pandas as pd
import logging
from src.config_loader import load_config

config = load_config()
logger = logging.getLogger(__name__)

def parse_timestamp(df):
    """Parse ISO-format timestamp strings produced by data_simulator.py."""
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        logger.info('Timestamp parsed successfully')
    except Exception as e:
        logger.warning("Primary timestamp parse failed, trying flexible parse: %s", str(e))
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)
            logger.info('Timestamp parsed with flexible format')
        except Exception as e2:
            logger.error("Failed to parse timestamp: %s", str(e2))
    return df

def ensure_numeric_columns(df):
    """Coerce IoT numeric sensor columns to float (they should already be clean)."""
    numeric_columns = config['validation']['numeric_fields']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("Numeric column check completed")
    return df

def ensure_boolean_columns(df):
    """Ensure boolean columns are stored as Python booleans."""
    bool_columns = config['validation'].get('boolean_fields', [])
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    logger.info("Boolean column check completed")
    return df

def rename_columns(df):
    """Standardise column names for downstream consumers."""
    column_mapping = {
        'temp':      'temperature',
        'co':        'co_ppm',
        'lpg':       'lpg_ppm',
        'smoke':     'smoke_ppm',
        'light':     'light_detected',
        'motion':    'motion_detected',
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    logger.info("Columns renamed successfully")
    return df

def process(df):
    logger.info("Starting data processing")

    df = parse_timestamp(df)
    df = ensure_numeric_columns(df)
    df = ensure_boolean_columns(df)
    df = rename_columns(df)

    logger.info("Data processing completed. Shape: %s", str(df.shape))
    return df
