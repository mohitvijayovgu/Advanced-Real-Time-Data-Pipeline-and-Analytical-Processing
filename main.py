"""
Pipeline entry point.

Run with:
    python main.py

The file_monitor watches the data/ folder. For each new CSV chunk dropped
by data_simulator.py it runs: validate -> process -> aggregate.
"""

import sys
import os

# Make sure imports resolve whether run from repo root or elsewhere
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# file_monitor MUST be imported first — it creates the required directories
# (logs/, data/, quarantine/) and configures logging via basicConfig before
# any other module tries to open a FileHandler or move files.
from src.file_monitor import monitor
from src.data_validator import validate
from src.data_processor import process
from src.data_aggregator import aggregate

import logging
logger = logging.getLogger(__name__)


def handle_file(filepath):
    """Full processing pipeline for a single incoming chunk."""

    # 1. Validate — quarantines the file and returns (None, errors) if invalid
    df, errors = validate(filepath)
    if df is None:
        logger.warning("Skipping invalid file: %s | Errors: %s", filepath, errors)
        return

    # 2. Process — parse timestamps, coerce types, rename columns
    df = process(df)

    # 3. Aggregate — per-device stats + time-window resampling
    result = aggregate(df)

    by_device = result.get('by_device')
    by_time   = result.get('by_time_window')

    if by_device is not None and not by_device.empty:
        logger.info("Per-device aggregation:\n%s", by_device.to_string(index=False))

    if by_time is not None and not by_time.empty:
        logger.info("Time-window aggregation (first 3 rows):\n%s",
                    by_time.head(3).to_string(index=False))

    logger.info("File fully processed: %s", os.path.basename(filepath))


if __name__ == "__main__":
    monitor(handle_file)
