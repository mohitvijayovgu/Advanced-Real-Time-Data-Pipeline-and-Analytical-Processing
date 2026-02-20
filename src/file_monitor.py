import os
import time
import logging
from src.config_loader import load_config

config = load_config()

# Ensure required runtime directories exist BEFORE configuring FileHandler.
# Without this, FileHandler raises FileNotFoundError at import time if the
# logs/ (or quarantine/) folder has never been created.
for _dir in (config['paths']['logs_dir'],
             config['paths']['data_dir'],
             config['paths']['quarantine_dir']):
    os.makedirs(_dir, exist_ok=True)

log_path = os.path.join(config['paths']['logs_dir'], 'pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_new_files(data_dir, processed_files):
    all_files = set(f for f in os.listdir(data_dir) if f.endswith('.csv'))
    new_files = all_files - processed_files
    return sorted(new_files)

def monitor(callback):
    data_dir = config['paths']['data_dir']
    poll_interval = config['pipeline']['poll_interval']
    processed_files = set()

    logger.info("Pipeline started. Monitoring folder: %s", data_dir)

    while True:
        new_files = get_new_files(data_dir, processed_files)

        for filename in new_files:
            filepath = os.path.join(data_dir, filename)
            logger.info("New file detected: %s", filename)
            try:
                callback(filepath)
                processed_files.add(filename)
            except Exception as e:
                logger.error("Failed to process %s: %s", filename, str(e))

        time.sleep(poll_interval)