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
    numeric_fields = config['validation']['string_to_numeric']
    errors = []
    
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
            null_count = df[field].isnull().sum()
            if null_count > 0:
                errors.append(f"Column '{field}' has {null_count} non-numeric values")
    
    return df, errors

def validate_ranges(df, filename):
    errors = []
    
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
    
    return errors

def quarantine_file(filepath, errors):
    filename = os.path.basename(filepath)
    quarantine_dir = config['paths']['quarantine_dir']
    
    # Move file to quarantine folder
    quarantine_path = os.path.join(quarantine_dir, filename)
    os.rename(filepath, quarantine_path)
    
    # Write reason to quarantine log
    log_path = os.path.join(config['paths']['logs_dir'], 'quarantine.log')
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
    
    # Run all validations
    errors1 = validate_required_fields(df, filename)
    all_errors.extend(errors1)
    
    df, errors2 = validate_numeric_fields(df, filename)
    all_errors.extend(errors2)
    
    errors3 = validate_ranges(df, filename)
    all_errors.extend(errors3)
    
    if all_errors:
        logger.warning("File %s failed validation: %s", filename, all_errors)
        quarantine_file(filepath, all_errors)
        return None, all_errors
    
    logger.info("File %s passed validation", filename)
    return df, []