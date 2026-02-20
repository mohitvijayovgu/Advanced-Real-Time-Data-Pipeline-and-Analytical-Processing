# IoT Telemetry Data — Exploratory Analysis

## Dataset Overview

| Property | Value |
|---|---|
| **File** | `iot_telemetry_data.csv` |
| **Rows** | 405,184 |
| **Columns** | 9 |
| **Missing Values** | None |
| **Date Range** | 2020-07-12 to 2020-07-20 (8 days) |
| **Source** | Kaggle — [IoT Telemetry Data by Gary Stafford](https://www.kaggle.com/datasets/garystafford/iot-telemetry-data) |

---

## Column Descriptions

| Column | Type | Description |
|---|---|---|
| `ts` | float64 | Unix epoch timestamp (seconds) |
| `device` | object | IoT device identifier (MAC address) |
| `co` | float64 | Carbon monoxide sensor reading (ppm) |
| `humidity` | float64 | Relative humidity (%) |
| `light` | bool | Light detected (True/False) |
| `lpg` | float64 | LPG (Liquefied Petroleum Gas) concentration (ppm) |
| `motion` | bool | Motion detected (True/False) |
| `smoke` | float64 | Smoke concentration (ppm) |
| `temp` | float64 | Temperature (°C) |

---

## Device Distribution

| Device (MAC Address) | Record Count | Share |
|---|---|---|
| `b8:27:eb:bf:9d:51` | 187,451 | 46.3% |
| `00:0f:00:70:91:0a` | 111,815 | 27.6% |
| `1c:bf:ce:15:ec:4d` | 105,918 | 26.1% |

3 unique IoT devices collected data across the 8-day window.

---

## Numeric Feature Statistics

| Column | Min | Max | Mean | Std Dev |
|---|---|---|---|---|
| `co` | 0.0012 | 0.0144 | 0.0046 | 0.0013 |
| `humidity` | 1.10 | 99.90 | 60.51 | 11.37 |
| `lpg` | 0.0027 | 0.0166 | 0.0072 | 0.0014 |
| `smoke` | 0.0067 | 0.0466 | 0.0193 | 0.0041 |
| `temp` | 0.00 | 30.60 | 22.45 | 2.70 |

---

## Boolean Feature Distribution

### `light` (Light Detected)
| Value | Count | Percentage |
|---|---|---|
| False (dark) | 292,657 | 72.2% |
| True (light) | 112,527 | 27.8% |

### `motion` (Motion Detected)
| Value | Count | Percentage |
|---|---|---|
| False (no motion) | 404,702 | 99.9% |
| True (motion) | 482 | 0.1% |

Motion is a rare event — only 0.1% of readings detect motion.

---

## Outlier Analysis (> 3 standard deviations)

| Column | Outlier Count | Percentage |
|---|---|---|
| `co` | 2,149 | 0.53% |
| `lpg` | 1,933 | 0.48% |
| `smoke` | 1,538 | 0.38% |
| `temp` | 522 | 0.13% |
| `humidity` | 125 | 0.03% |

Outlier rates are low (< 1%) across all numeric columns. The pipeline quarantines chunks where temperature is outside the valid physical range [−10, 50°C].

---

## Correlation Matrix

| | co | humidity | lpg | smoke | temp |
|---|---|---|---|---|---|
| **co** | 1.000 | −0.657 | 0.997 | 0.998 | 0.111 |
| **humidity** | −0.657 | 1.000 | −0.672 | −0.670 | −0.410 |
| **lpg** | 0.997 | −0.672 | 1.000 | 1.000 | 0.136 |
| **smoke** | 0.998 | −0.670 | 1.000 | 1.000 | 0.132 |
| **temp** | 0.111 | −0.410 | 0.136 | 0.132 | 1.000 |

**Key observations:**
- `co`, `lpg`, and `smoke` are almost perfectly correlated (r ≈ 0.997–0.998) — they likely all come from the same combustion source.
- `humidity` is moderately negatively correlated with gas readings (−0.657 to −0.672) — higher humidity tends to coincide with lower gas concentrations.
- `temp` has a weak positive correlation with gas sensors and a moderate negative correlation with humidity (−0.410), consistent with physics (warmer air holds more moisture but records lower relative humidity).

---

## Pipeline Adaptation Notes

| Original (AirQualityUCI) | Replacement (IoT Telemetry) |
|---|---|
| `Date` + `Time` → `timestamp` | `ts` (unix float) → `timestamp` |
| `sensor_id` (synthesised) | `device` (MAC address) → `sensor_id` |
| `T` (temperature) | `temp` |
| `RH` (humidity) | `humidity` |
| `CO(GT)`, `C6H6(GT)` etc. | `co`, `lpg`, `smoke` |
| Comma-decimal conversion needed | All values are clean floats |
| −200 sentinel values present | No sentinel values — data is clean |

---

## Data Quality Summary

- **Zero missing values** across all 405,184 rows and 9 columns.
- **No sentinel/error values** (unlike AirQualityUCI which used −200 for missing).
- **Timestamps** are Unix epoch floats — converted to `datetime` during processing.
- **Boolean fields** (`light`, `motion`) are already parsed correctly by pandas.
- **Outlier rate** < 1% — the quarantine step will handle extreme temperature/humidity readings.
