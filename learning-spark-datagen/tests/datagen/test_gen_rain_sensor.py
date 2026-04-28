"""Tests for GenRainSensor: determinism, validation, timeseries, and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenRainSensor


# ── determinism ───────────────────────────────────────────────────


def test_generate_one_deterministic():
    gen = GenRainSensor(seed=42)
    r1 = gen.generate_one(index=0)
    r2 = gen.generate_one(index=0)
    protovalidate.validate(r1)
    protovalidate.validate(r2)
    assert r1.reading_id == r2.reading_id
    assert r1.sensor_id == r2.sensor_id
    assert r1.timestamp_ms == r2.timestamp_ms
    assert r1.precipitation_mm == r2.precipitation_mm
    assert r1.temperature_celsius == r2.temperature_celsius


def test_different_indices_yield_different_readings():
    gen = GenRainSensor(seed=42)
    r0 = gen.generate_one(index=0)
    r1 = gen.generate_one(index=1)
    protovalidate.validate(r0)
    protovalidate.validate(r1)
    assert r0.reading_id != r1.reading_id
    assert r0.sensor_id != r1.sensor_id  # different sensors (index 0 vs 1)


# ── batch generation ──────────────────────────────────────────────


def test_generate_count():
    gen = GenRainSensor(seed=42)
    readings = gen.generate(40)
    assert len(readings) == 40
    for r in readings:
        protovalidate.validate(r)
    reading_ids = {r.reading_id for r in readings}
    assert len(reading_ids) == 40


def test_generate_1000():
    gen = GenRainSensor(seed=42)
    readings = gen.generate(1000)
    assert len(readings) == 1000
    for r in readings:
        protovalidate.validate(r)
    assert len({r.reading_id for r in readings}) == 1000


def test_generate_range():
    gen = GenRainSensor(seed=42)
    batch = gen.generate_range(10, 20)
    full = gen.generate(20)
    assert len(batch) == 10
    for a, b in zip(batch, full[10:20]):
        protovalidate.validate(a)
        assert a.reading_id == b.reading_id
        assert a.timestamp_ms == b.timestamp_ms


# ── timeseries properties ────────────────────────────────────────


def test_sensor_cycling():
    """Indices cycle through sensors: index 0 → sensor 0, index 1 → sensor 1, etc."""
    gen = GenRainSensor(seed=42, num_sensors=5)
    readings = gen.generate(15)
    sensor_ids = [r.sensor_id for r in readings]
    # First 5 readings should be 5 different sensors
    assert len(set(sensor_ids[:5])) == 5
    # Then the pattern repeats
    assert sensor_ids[0] == sensor_ids[5] == sensor_ids[10]
    assert sensor_ids[1] == sensor_ids[6] == sensor_ids[11]


def test_timestamps_increase_for_same_sensor():
    """For a given sensor, successive readings have increasing timestamps."""
    gen = GenRainSensor(seed=42, num_sensors=4, interval_minutes=5)
    readings = gen.generate(20)
    sensor_timeseries: dict[str, list[int]] = {}
    for r in readings:
        sensor_timeseries.setdefault(r.sensor_id, []).append(r.timestamp_ms)
    for sid, ts_list in sensor_timeseries.items():
        for i in range(1, len(ts_list)):
            assert ts_list[i] > ts_list[i - 1], f"Sensor {sid}: timestamps not increasing"
            assert ts_list[i] - ts_list[i - 1] == 5 * 60 * 1000


def test_interval_minutes_respected():
    gen = GenRainSensor(seed=42, num_sensors=2, interval_minutes=15)
    readings = gen.generate(6)
    # sensor 0: indices 0, 2, 4 → time steps 0, 1, 2
    s0 = [r for r in readings if r.sensor_id == readings[0].sensor_id]
    assert len(s0) == 3
    assert s0[1].timestamp_ms - s0[0].timestamp_ms == 15 * 60 * 1000
    assert s0[2].timestamp_ms - s0[1].timestamp_ms == 15 * 60 * 1000


# ── geolocation ───────────────────────────────────────────────────


def test_location_populated():
    gen = GenRainSensor(seed=42)
    r = gen.generate_one(index=0)
    protovalidate.validate(r)
    loc = r.location
    assert -90 <= loc.latitude <= 90
    assert -180 <= loc.longitude <= 180
    assert loc.place_name


def test_same_sensor_same_location():
    """All readings from the same sensor have the same deployment location."""
    gen = GenRainSensor(seed=42, num_sensors=3)
    readings = gen.generate(9)
    for i in (0, 1, 2):
        assert readings[i].location.latitude == readings[i + 3].location.latitude
        assert readings[i].location.longitude == readings[i + 3].location.longitude
        assert readings[i].location.place_name == readings[i + 3].location.place_name


# ── weather consistency ───────────────────────────────────────────


def test_rain_fields_consistent():
    """When it's raining, precipitation > 0 and intensity != NONE; vice versa."""
    gen = GenRainSensor(seed=42)
    readings = gen.generate(200)
    for r in readings:
        protovalidate.validate(r)
        if r.is_raining:
            assert r.precipitation_mm > 0
            assert r.rain_intensity != 1  # RAIN_INTENSITY_NONE
        else:
            assert r.precipitation_mm == 0.0
            assert r.rain_intensity == 1  # RAIN_INTENSITY_NONE


def test_atmospheric_ranges():
    gen = GenRainSensor(seed=42)
    readings = gen.generate(200)
    for r in readings:
        protovalidate.validate(r)
        assert 0.0 <= r.humidity_percent <= 100.0
        assert 600.0 <= r.pressure_hpa <= 1100.0
        assert r.wind_speed_kmh >= 0.0
        assert 0.0 <= r.wind_direction_degrees < 360.0
        assert 5 <= r.battery_level_percent <= 100
        assert r.signal_strength_dbm < 0


# ── device health ─────────────────────────────────────────────────


def test_battery_drains_over_time():
    """Battery should generally trend downward over many time steps."""
    gen = GenRainSensor(seed=42, num_sensors=1)
    readings = gen.generate(200)
    early_avg = sum(r.battery_level_percent for r in readings[:20]) / 20
    late_avg = sum(r.battery_level_percent for r in readings[-20:]) / 20
    assert late_avg < early_avg


# ── NDJSON round-trip ─────────────────────────────────────────────


def test_ndjson_round_trip():
    gen = GenRainSensor(seed=42)
    readings = gen.generate(10)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenRainSensor.write_ndjson(path, readings)
        read_back = GenRainSensor.read_ndjson(path)
        assert len(read_back) == 10
        for a, b in zip(readings, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.reading_id == b.reading_id
            assert a.sensor_id == b.sensor_id
            assert a.timestamp_ms == b.timestamp_ms
            assert a.location.place_name == b.location.place_name
            assert a.precipitation_mm == b.precipitation_mm
            assert a.temperature_celsius == b.temperature_celsius
    finally:
        path.unlink(missing_ok=True)
