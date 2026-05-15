# Dataset Understanding

## Dataset

Condition Monitoring of Hydraulic Systems

The dataset contains raw industrial sensor matrices from a hydraulic test rig.
Each sensor file contains one row per 60-second load cycle.
Each column inside a sensor file represents a measurement point within that cycle.

## Raw Sensor Structure

| File | Sensor | Quantity | Unit | Sampling Rate | Expected Columns | Actual Columns | Rows/Cycles | Status |
|---|---|---|---|---:|---:|---:|---:|---|
| PS1.txt | PS1 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| PS2.txt | PS2 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| PS3.txt | PS3 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| PS4.txt | PS4 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| PS5.txt | PS5 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| PS6.txt | PS6 | Pressure | bar | 100 Hz | 6000 | 6000 | 2205 | OK |
| EPS1.txt | EPS1 | Motor power | W | 100 Hz | 6000 | 6000 | 2205 | OK |
| FS1.txt | FS1 | Volume flow | l/min | 10 Hz | 600 | 600 | 2205 | OK |
| FS2.txt | FS2 | Volume flow | l/min | 10 Hz | 600 | 600 | 2205 | OK |
| TS1.txt | TS1 | Temperature | °C | 1 Hz | 60 | 60 | 2205 | OK |
| TS2.txt | TS2 | Temperature | °C | 1 Hz | 60 | 60 | 2205 | OK |
| TS3.txt | TS3 | Temperature | °C | 1 Hz | 60 | 60 | 2205 | OK |
| TS4.txt | TS4 | Temperature | °C | 1 Hz | 60 | 60 | 2205 | OK |
| VS1.txt | VS1 | Vibration | mm/s | 1 Hz | 60 | 60 | 2205 | OK |
| CE.txt | CE | Cooling efficiency | % | 1 Hz | 60 | 60 | 2205 | OK |
| CP.txt | CP | Cooling power | kW | 1 Hz | 60 | 60 | 2205 | OK |
| SE.txt | SE | Efficiency factor | % | 1 Hz | 60 | 60 | 2205 | OK |

## Profile Target Columns

| Column | Meaning |
|---|---|
| cooler_condition_pct | Cooler condition percentage |
| valve_condition_pct | Valve condition percentage |
| internal_pump_leakage | Internal pump leakage severity |
| hydraulic_accumulator_bar | Hydraulic accumulator pressure condition |
| stable_flag | Whether stable conditions were reached |

## Profile Target Distributions

### cooler_condition_pct

| Value | Cycle Count | Meaning |
|---:|---:|---|
| 3 | 732 | close_to_total_failure |
| 20 | 732 | reduced_efficiency |
| 100 | 741 | full_efficiency |

### valve_condition_pct

| Value | Cycle Count | Meaning |
|---:|---:|---|
| 73 | 360 | close_to_total_failure |
| 80 | 360 | severe_lag |
| 90 | 360 | small_lag |
| 100 | 1125 | optimal_switching_behavior |

### internal_pump_leakage

| Value | Cycle Count | Meaning |
|---:|---:|---|
| 0 | 1221 | no_leakage |
| 1 | 492 | weak_leakage |
| 2 | 492 | severe_leakage |

### hydraulic_accumulator_bar

| Value | Cycle Count | Meaning |
|---:|---:|---|
| 90 | 808 | close_to_total_failure |
| 100 | 399 | severely_reduced_pressure |
| 115 | 399 | slightly_reduced_pressure |
| 130 | 599 | optimal_pressure |

### stable_flag

| Value | Cycle Count | Meaning |
|---:|---:|---|
| 0 | 1449 | conditions_were_stable |
| 1 | 756 | static_conditions_might_not_have_been_reached |

## Initial Lakehouse Design Decision

For Bronze, we will preserve raw readings as arrays at the grain of one row per cycle per sensor.
This keeps the raw sensor sequence without exploding the dataset too early.

For Silver, we will create cleaned cycle-level and sensor-level feature tables.
This is where we calculate statistics such as mean, min, max, standard deviation, and anomaly flags.

For Gold, we will create business-ready machine health, component condition, and maintenance-risk tables.
