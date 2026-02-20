import pandas as pd
import logging

logger = logging.getLogger(__name__)


def aggregate_by_device(df):
    """Compute per-device statistics for numeric and boolean sensor columns."""
    group_col = 'sensor_id' if 'sensor_id' in df.columns else 'device_id'

    numeric_cols = [c for c in ['co_ppm', 'co', 'humidity', 'lpg_ppm', 'lpg',
                                 'smoke_ppm', 'smoke', 'temperature', 'temp']
                    if c in df.columns]

    bool_cols = [c for c in ['light_detected', 'light', 'motion_detected', 'motion']
                 if c in df.columns]

    agg_dict = {}
    for col in numeric_cols:
        agg_dict[col] = ['mean', 'min', 'max', 'std']
    for col in bool_cols:
        agg_dict[col] = ['sum', 'mean']   # sum = event count, mean = event rate

    if not agg_dict:
        logger.warning("No aggregatable columns found in chunk")
        return pd.DataFrame()

    agg_df = df.groupby(group_col).agg(agg_dict)
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns]
    agg_df = agg_df.reset_index()

    logger.info("Aggregation completed: %d device(s), %d metric(s)",
                len(agg_df), len(agg_df.columns) - 1)
    return agg_df


def aggregate_by_time_window(df, freq='1min'):
    """Resample sensor readings into fixed time windows (mean per window)."""
    if 'timestamp' not in df.columns:
        logger.warning("No 'timestamp' column — skipping time-window aggregation")
        return pd.DataFrame()

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df = df.set_index('timestamp')

    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if not numeric_cols:
        logger.warning("No numeric columns found for time-window aggregation")
        return pd.DataFrame()

    resampled = df[numeric_cols].resample(freq).mean().reset_index()
    logger.info("Time-window aggregation (%s): %d windows", freq, len(resampled))
    return resampled


def aggregate(df):
    """Main entry point called by the pipeline for each validated chunk."""
    logger.info("Starting aggregation on chunk with shape: %s", str(df.shape))
    return {
        'by_device':      aggregate_by_device(df),
        'by_time_window': aggregate_by_time_window(df, freq='1min'),
    }

