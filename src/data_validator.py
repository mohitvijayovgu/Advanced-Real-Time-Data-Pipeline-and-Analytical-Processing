import pandas as pd
import logging
import os
from src.config_loader import load_config

config = load_config()
logger = logging.getLogger(__name__)

def validate_required_fields(df, filename):
    required_fields = config['validation']['required_fields']
    errors = []

    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            errors.append(f"Column '{field}' has {null_count} null values")

    return errors

def validate_numeric_fields(df, filename):
    """Coerce numeric columns and flag rows that could not be parsed."""
    numeric_fields = config['validation']['numeric_fields']
    errors = []

    for field in numeric_fields:
        if field in df.columns:
            before_nulls = df[field].isnull().sum()
            df[field] = pd.to_numeric(df[field], errors='coerce')
            after_nulls = df[field].isnull().sum()
            new_non_numeric = after_nulls - before_nulls
            if new_non_numeric > 0:
                errors.append(f"Column '{field}' has {new_non_numeric} non-numeric values")

    return df, errors

def validate_boolean_fields(df, filename):
    """Ensure boolean columns contain only True/False values."""
    bool_fields = config['validation'].get('boolean_fields', [])
    errors = []

    for field in bool_fields:
        if field in df.columns:
            invalid = df[~df[field].isin([True, False, 0, 1])].shape[0]
            if invalid > 0:
                errors.append(f"Column '{field}' has {invalid} non-boolean values")

    return errors

def validate_ranges(df, filename):
    errors = []

    # Temperature range check
    temp_field = config['validation']['temperature_field']
    temp_min = config['validation']['temperature_min']
    temp_max = config['validation']['temperature_max']

    if temp_field in df.columns:
        out_of_range = df[
            (df[temp_field] < temp_min) |
            (df[temp_field] > temp_max)
        ].shape[0]
        if out_of_range > 0:
            errors.append(f"Column '{temp_field}' has {out_of_range} values outside range [{temp_min}, {temp_max}]")

    # Humidity range check
    hum_field = config['validation']['humidity_field']
    hum_min = config['validation']['humidity_min']
    hum_max = config['validation']['humidity_max']

    if hum_field in df.columns:
        out_of_range_hum = df[
            (df[hum_field] < hum_min) |
            (df[hum_field] > hum_max)
        ].shape[0]
        if out_of_range_hum > 0:
            errors.append(f"Column '{hum_field}' has {out_of_range_hum} values outside range [{hum_min}, {hum_max}]")

    return errors

def quarantine_file(filepath, errors):
    filename = os.path.basename(filepath)
    quarantine_dir = config['paths']['quarantine_dir']

    os.makedirs(quarantine_dir, exist_ok=True)
    quarantine_path = os.path.join(quarantine_dir, filename)
    os.rename(filepath, quarantine_path)

    log_path = os.path.join(config['paths']['logs_dir'], 'quarantine.log')
    os.makedirs(config['paths']['logs_dir'], exist_ok=True)
    with open(log_path, 'a') as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"File: {filename}\n")
        f.write(f"Reasons:\n")
        for error in errors:
            f.write(f"  - {error}\n")

    logger.warning("File quarantined: %s", filename)

def validate(filepath):
    filename = os.path.basename(filepath)
    logger.info("Validating file: %s", filename)

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        logger.error("Could not read file %s: %s", filename, str(e))
        return None, [f"Could not read file: {str(e)}"]

    all_errors = []

    errors1 = validate_required_fields(df, filename)
    all_errors.extend(errors1)

    df, errors2 = validate_numeric_fields(df, filename)
    all_errors.extend(errors2)

    errors3 = validate_boolean_fields(df, filename)
    all_errors.extend(errors3)

    errors4 = validate_ranges(df, filename)
    all_errors.extend(errors4)

    if all_errors:
        logger.warning("File %s failed validation: %s", filename, all_errors)
        quarantine_file(filepath, all_errors)
        return None, all_errors

    logger.info("File %s passed validation", filename)
    return df, []
