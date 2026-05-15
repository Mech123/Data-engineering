SENSOR_METADATA = [
    {
        "file_name": "PS1.txt",
        "sensor_id": "PS1",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "PS2.txt",
        "sensor_id": "PS2",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "PS3.txt",
        "sensor_id": "PS3",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "PS4.txt",
        "sensor_id": "PS4",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "PS5.txt",
        "sensor_id": "PS5",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "PS6.txt",
        "sensor_id": "PS6",
        "physical_quantity": "Pressure",
        "unit": "bar",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "EPS1.txt",
        "sensor_id": "EPS1",
        "physical_quantity": "Motor power",
        "unit": "W",
        "sampling_rate_hz": 100,
    },
    {
        "file_name": "FS1.txt",
        "sensor_id": "FS1",
        "physical_quantity": "Volume flow",
        "unit": "l/min",
        "sampling_rate_hz": 10,
    },
    {
        "file_name": "FS2.txt",
        "sensor_id": "FS2",
        "physical_quantity": "Volume flow",
        "unit": "l/min",
        "sampling_rate_hz": 10,
    },
    {
        "file_name": "TS1.txt",
        "sensor_id": "TS1",
        "physical_quantity": "Temperature",
        "unit": "°C",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "TS2.txt",
        "sensor_id": "TS2",
        "physical_quantity": "Temperature",
        "unit": "°C",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "TS3.txt",
        "sensor_id": "TS3",
        "physical_quantity": "Temperature",
        "unit": "°C",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "TS4.txt",
        "sensor_id": "TS4",
        "physical_quantity": "Temperature",
        "unit": "°C",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "VS1.txt",
        "sensor_id": "VS1",
        "physical_quantity": "Vibration",
        "unit": "mm/s",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "CE.txt",
        "sensor_id": "CE",
        "physical_quantity": "Cooling efficiency",
        "unit": "%",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "CP.txt",
        "sensor_id": "CP",
        "physical_quantity": "Cooling power",
        "unit": "kW",
        "sampling_rate_hz": 1,
    },
    {
        "file_name": "SE.txt",
        "sensor_id": "SE",
        "physical_quantity": "Efficiency factor",
        "unit": "%",
        "sampling_rate_hz": 1,
    },
]


PROFILE_COLUMNS = [
    "cooler_condition_pct",
    "valve_condition_pct",
    "internal_pump_leakage",
    "hydraulic_accumulator_bar",
    "stable_flag",
]


PROFILE_VALUE_MEANINGS = {
    "cooler_condition_pct": {
        3: "close_to_total_failure",
        20: "reduced_efficiency",
        100: "full_efficiency",
    },
    "valve_condition_pct": {
        100: "optimal_switching_behavior",
        90: "small_lag",
        80: "severe_lag",
        73: "close_to_total_failure",
    },
    "internal_pump_leakage": {
        0: "no_leakage",
        1: "weak_leakage",
        2: "severe_leakage",
    },
    "hydraulic_accumulator_bar": {
        130: "optimal_pressure",
        115: "slightly_reduced_pressure",
        100: "severely_reduced_pressure",
        90: "close_to_total_failure",
    },
    "stable_flag": {
        0: "conditions_were_stable",
        1: "static_conditions_might_not_have_been_reached",
    },
}
