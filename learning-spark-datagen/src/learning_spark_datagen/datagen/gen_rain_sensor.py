"""Generate deterministic fake IoT rain-sensor readings with geolocation and weather simulation."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from random import Random
from uuid import UUID

_here = Path(__file__).resolve().parent
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format  # noqa: E402

from rain_sensor.v1 import rain_sensor_pb2  # noqa: E402

# 20 real-world sensor deployment sites spanning extreme-rain to desert climates.
# Each profile drives the weather simulation so the generated data is plausible
# for that locale — great for geo-spatial aggregation in Spark / Delta.
_SENSOR_PROFILES: list[dict] = [
    # ── Extreme rain ──────────────────────────────────────────────
    {
        "lat": 25.2744,
        "lon": 91.7362,
        "alt": 1484.0,
        "place": "Cherrapunji, Meghalaya, India",
        "rain_prob": 0.72,
        "precip_range": (0.2, 12.0),
        "temp_range": (11.0, 24.0),
        "humidity_range": (78.0, 99.0),
        "pressure_base": 850.0,
        "wind_range": (2.0, 35.0),
        "signal_range": (-85, -55),
    },
    {
        "lat": 25.2973,
        "lon": 91.5822,
        "alt": 1401.0,
        "place": "Mawsynram, Meghalaya, India",
        "rain_prob": 0.75,
        "precip_range": (0.3, 14.0),
        "temp_range": (10.0, 23.0),
        "humidity_range": (80.0, 99.0),
        "pressure_base": 855.0,
        "wind_range": (3.0, 40.0),
        "signal_range": (-90, -60),
    },
    # ── Tropical rainforest ───────────────────────────────────────
    {
        "lat": 5.6947,
        "lon": -76.6611,
        "alt": 43.0,
        "place": "Quibdó, Chocó, Colombia",
        "rain_prob": 0.70,
        "precip_range": (0.2, 10.0),
        "temp_range": (23.0, 33.0),
        "humidity_range": (82.0, 98.0),
        "pressure_base": 1008.0,
        "wind_range": (1.0, 20.0),
        "signal_range": (-80, -50),
    },
    {
        "lat": -3.1190,
        "lon": -60.0217,
        "alt": 92.0,
        "place": "Manaus, Amazonas, Brazil",
        "rain_prob": 0.55,
        "precip_range": (0.1, 8.0),
        "temp_range": (24.0, 35.0),
        "humidity_range": (75.0, 97.0),
        "pressure_base": 1005.0,
        "wind_range": (1.0, 18.0),
        "signal_range": (-75, -45),
    },
    # ── Tropical maritime ─────────────────────────────────────────
    {
        "lat": 1.3521,
        "lon": 103.8198,
        "alt": 15.0,
        "place": "Singapore",
        "rain_prob": 0.50,
        "precip_range": (0.1, 9.0),
        "temp_range": (24.0, 34.0),
        "humidity_range": (70.0, 96.0),
        "pressure_base": 1010.0,
        "wind_range": (2.0, 25.0),
        "signal_range": (-60, -30),
    },
    {
        "lat": 19.0760,
        "lon": 72.8777,
        "alt": 14.0,
        "place": "Mumbai, Maharashtra, India",
        "rain_prob": 0.45,
        "precip_range": (0.2, 15.0),
        "temp_range": (22.0, 36.0),
        "humidity_range": (60.0, 95.0),
        "pressure_base": 1008.0,
        "wind_range": (3.0, 45.0),
        "signal_range": (-65, -35),
    },
    # ── Temperate wet ─────────────────────────────────────────────
    {
        "lat": 47.6062,
        "lon": -122.3321,
        "alt": 56.0,
        "place": "Seattle, WA, USA",
        "rain_prob": 0.42,
        "precip_range": (0.05, 3.0),
        "temp_range": (2.0, 22.0),
        "humidity_range": (55.0, 92.0),
        "pressure_base": 1013.0,
        "wind_range": (2.0, 30.0),
        "signal_range": (-55, -25),
    },
    {
        "lat": 60.3913,
        "lon": 5.3221,
        "alt": 12.0,
        "place": "Bergen, Vestland, Norway",
        "rain_prob": 0.55,
        "precip_range": (0.1, 5.0),
        "temp_range": (-2.0, 18.0),
        "humidity_range": (65.0, 95.0),
        "pressure_base": 1010.0,
        "wind_range": (5.0, 50.0),
        "signal_range": (-70, -35),
    },
    {
        "lat": 51.5074,
        "lon": -0.1278,
        "alt": 11.0,
        "place": "London, England, UK",
        "rain_prob": 0.38,
        "precip_range": (0.05, 3.5),
        "temp_range": (2.0, 25.0),
        "humidity_range": (55.0, 90.0),
        "pressure_base": 1013.0,
        "wind_range": (3.0, 35.0),
        "signal_range": (-50, -20),
    },
    {
        "lat": 35.6762,
        "lon": 139.6503,
        "alt": 40.0,
        "place": "Tokyo, Kantō, Japan",
        "rain_prob": 0.35,
        "precip_range": (0.1, 6.0),
        "temp_range": (2.0, 34.0),
        "humidity_range": (45.0, 88.0),
        "pressure_base": 1012.0,
        "wind_range": (2.0, 30.0),
        "signal_range": (-50, -20),
    },
    # ── Cold wet ──────────────────────────────────────────────────
    {
        "lat": 58.3019,
        "lon": -134.4197,
        "alt": 5.0,
        "place": "Juneau, AK, USA",
        "rain_prob": 0.52,
        "precip_range": (0.05, 4.0),
        "temp_range": (-12.0, 18.0),
        "humidity_range": (60.0, 93.0),
        "pressure_base": 1010.0,
        "wind_range": (3.0, 45.0),
        "signal_range": (-80, -45),
    },
    {
        "lat": 64.1466,
        "lon": -21.9426,
        "alt": 0.0,
        "place": "Reykjavik, Iceland",
        "rain_prob": 0.48,
        "precip_range": (0.05, 3.0),
        "temp_range": (-5.0, 14.0),
        "humidity_range": (65.0, 92.0),
        "pressure_base": 1005.0,
        "wind_range": (8.0, 65.0),
        "signal_range": (-75, -40),
    },
    # ── Island (very wet) ─────────────────────────────────────────
    {
        "lat": 22.0796,
        "lon": -159.3711,
        "alt": 1569.0,
        "place": "Mt. Waialeale, Kauai, HI, USA",
        "rain_prob": 0.82,
        "precip_range": (0.3, 15.0),
        "temp_range": (12.0, 22.0),
        "humidity_range": (85.0, 100.0),
        "pressure_base": 840.0,
        "wind_range": (5.0, 50.0),
        "signal_range": (-95, -65),
    },
    {
        "lat": 19.7241,
        "lon": -155.0868,
        "alt": 12.0,
        "place": "Hilo, HI, USA",
        "rain_prob": 0.60,
        "precip_range": (0.1, 8.0),
        "temp_range": (18.0, 30.0),
        "humidity_range": (70.0, 96.0),
        "pressure_base": 1012.0,
        "wind_range": (3.0, 30.0),
        "signal_range": (-65, -35),
    },
    # ── Oceanic / fjord ───────────────────────────────────────────
    {
        "lat": -45.4148,
        "lon": 167.7181,
        "alt": 3.0,
        "place": "Milford Sound, Fiordland, NZ",
        "rain_prob": 0.62,
        "precip_range": (0.1, 7.0),
        "temp_range": (2.0, 18.0),
        "humidity_range": (70.0, 98.0),
        "pressure_base": 1010.0,
        "wind_range": (3.0, 40.0),
        "signal_range": (-85, -50),
    },
    {
        "lat": 4.4725,
        "lon": 101.3790,
        "alt": 1500.0,
        "place": "Cameron Highlands, Pahang, Malaysia",
        "rain_prob": 0.50,
        "precip_range": (0.1, 7.0),
        "temp_range": (15.0, 25.0),
        "humidity_range": (75.0, 98.0),
        "pressure_base": 845.0,
        "wind_range": (1.0, 20.0),
        "signal_range": (-80, -50),
    },
    # ── Desert (extremely dry) ────────────────────────────────────
    {
        "lat": 36.5323,
        "lon": -116.9325,
        "alt": -86.0,
        "place": "Death Valley, CA, USA",
        "rain_prob": 0.03,
        "precip_range": (0.01, 0.5),
        "temp_range": (8.0, 52.0),
        "humidity_range": (5.0, 25.0),
        "pressure_base": 1025.0,
        "wind_range": (2.0, 40.0),
        "signal_range": (-90, -55),
    },
    {
        "lat": -23.8634,
        "lon": -69.1328,
        "alt": 2400.0,
        "place": "San Pedro de Atacama, Chile",
        "rain_prob": 0.01,
        "precip_range": (0.01, 0.3),
        "temp_range": (-2.0, 28.0),
        "humidity_range": (3.0, 20.0),
        "pressure_base": 760.0,
        "wind_range": (3.0, 35.0),
        "signal_range": (-95, -65),
    },
    {
        "lat": 25.2048,
        "lon": 55.2708,
        "alt": 5.0,
        "place": "Dubai, UAE",
        "rain_prob": 0.05,
        "precip_range": (0.01, 1.0),
        "temp_range": (15.0, 48.0),
        "humidity_range": (15.0, 65.0),
        "pressure_base": 1012.0,
        "wind_range": (3.0, 40.0),
        "signal_range": (-50, -20),
    },
    # ── Mediterranean ─────────────────────────────────────────────
    {
        "lat": 37.7749,
        "lon": -122.4194,
        "alt": 16.0,
        "place": "San Francisco, CA, USA",
        "rain_prob": 0.22,
        "precip_range": (0.05, 2.5),
        "temp_range": (8.0, 22.0),
        "humidity_range": (50.0, 88.0),
        "pressure_base": 1015.0,
        "wind_range": (5.0, 40.0),
        "signal_range": (-45, -20),
    },
]

# 2024-01-01T00:00:00Z in milliseconds
_DEFAULT_BASE_TIME_MS = 1_704_067_200_000
_DEFAULT_INTERVAL_MS = 5 * 60 * 1000  # 5-minute sampling


def _weather_factor(sensor_idx: int, time_step: int) -> float:
    """Deterministic weather-front oscillation in [-1, 1].

    Combines slow, medium, and fast sine waves so each sensor experiences
    realistic cycles of rainy and dry spells without any mutable state.
    """
    t = time_step
    s = sensor_idx
    slow = math.sin(2 * math.pi * (t + s * 13) / 72)  # ~6-hour cycle
    medium = 0.4 * math.sin(2 * math.pi * (t + s * 7) / 24)  # ~2-hour cycle
    fast = 0.2 * math.sin(2 * math.pi * (t + s * 3) / 6)  # ~30-min cycle
    return (slow + medium + fast) / 1.6


def _classify_intensity(
    precipitation_mm: float,
    interval_minutes: int,
) -> "rain_sensor_pb2.RainIntensity.ValueType":
    """Map accumulated precipitation to the WMO intensity category."""
    rate = precipitation_mm * (60.0 / interval_minutes)
    if rate == 0:
        return rain_sensor_pb2.RAIN_INTENSITY_NONE
    if rate < 2.5:
        return rain_sensor_pb2.RAIN_INTENSITY_LIGHT
    if rate < 7.6:
        return rain_sensor_pb2.RAIN_INTENSITY_MODERATE
    if rate < 50.0:
        return rain_sensor_pb2.RAIN_INTENSITY_HEAVY
    return rain_sensor_pb2.RAIN_INTENSITY_VIOLENT


class GenRainSensor:
    """Generate deterministic IoT rain-sensor time-series readings.

    Readings cycle through *num_sensors* fixed deployment sites (default 20)
    so ``index`` maps to ``(sensor_idx, time_step)`` via divmod.  Same
    ``(seed, index)`` always yields the identical reading.
    """

    def __init__(
        self,
        seed: int = 42,
        num_sensors: int = 20,
        interval_minutes: int = 5,
    ) -> None:
        self._seed = seed
        self._num_sensors = num_sensors
        self._interval_minutes = interval_minutes
        self._interval_ms = interval_minutes * 60 * 1000
        self._base_time_ms = _DEFAULT_BASE_TIME_MS

        # Deterministic sensor UUIDs (offset avoids collision with reading RNG).
        self._sensor_ids = [
            str(UUID(int=Random(seed + 10_000 + i).getrandbits(128)))
            for i in range(num_sensors)
        ]

    # ── generation ────────────────────────────────────────────────

    def generate_one(self, index: int = 0) -> rain_sensor_pb2.RainSensorReading:
        """Generate a single reading.  Deterministic from ``(seed, index)``."""
        rng = Random(self._seed + index)

        sensor_idx = index % self._num_sensors
        time_step = index // self._num_sensors
        profile = _SENSOR_PROFILES[sensor_idx % len(_SENSOR_PROFILES)]

        sensor_id = self._sensor_ids[sensor_idx]
        reading_id = str(UUID(int=rng.getrandbits(128)))
        timestamp_ms = self._base_time_ms + time_step * self._interval_ms

        location = rain_sensor_pb2.GeoLocation(
            latitude=profile["lat"],
            longitude=profile["lon"],
            altitude_meters=profile["alt"],
            place_name=profile["place"],
        )

        # ── weather simulation ────────────────────────────────────
        weather = _weather_factor(sensor_idx, time_step)
        base_prob = profile["rain_prob"]
        adjusted_prob = max(0.0, min(1.0, base_prob * (0.6 + 0.8 * (weather + 1) / 2)))
        is_raining = rng.random() < adjusted_prob

        if is_raining:
            rain_strength = max(0.0, (weather + 1) / 2)
            lo, hi = profile["precip_range"]
            precipitation_mm = round(lo + rain_strength * (hi - lo) * rng.random(), 2)
            if precipitation_mm <= 0:
                precipitation_mm = round(rng.uniform(0.01, lo if lo > 0 else 0.01), 2)
        else:
            precipitation_mm = 0.0

        rain_intensity = _classify_intensity(precipitation_mm, self._interval_minutes)

        # ── atmospheric conditions ────────────────────────────────
        t_lo, t_hi = profile["temp_range"]
        temp_shift = -3.0 if is_raining else 0.0
        temperature_celsius = round(rng.uniform(t_lo, t_hi) + temp_shift, 1)

        h_lo, h_hi = profile["humidity_range"]
        humidity_boost = rng.uniform(5.0, 15.0) if is_raining else 0.0
        humidity_percent = round(
            min(100.0, rng.uniform(h_lo, h_hi) + humidity_boost), 1
        )

        p_shift = rng.uniform(-4.0, -1.0) if is_raining else rng.uniform(-1.0, 2.0)
        pressure_hpa = round(
            max(600.0, min(1100.0, profile["pressure_base"] + p_shift)),
            1,
        )

        w_lo, w_hi = profile["wind_range"]
        wind_boost = rng.uniform(0.0, 15.0) if precipitation_mm > 2.0 else 0.0
        wind_speed_kmh = round(max(0.0, rng.uniform(w_lo, w_hi) + wind_boost), 1)
        wind_direction_degrees = round(rng.random() * 359.9, 1)

        # ── device health ─────────────────────────────────────────
        base_battery = 100.0 - (time_step * 0.015)
        battery_level_percent = max(5, min(100, int(base_battery + rng.uniform(-2, 2))))

        sig_lo, sig_hi = profile["signal_range"]
        signal_strength_dbm = rng.randint(sig_lo, sig_hi)

        return rain_sensor_pb2.RainSensorReading(
            reading_id=reading_id,
            sensor_id=sensor_id,
            timestamp_ms=timestamp_ms,
            location=location,
            precipitation_mm=precipitation_mm,
            rain_intensity=rain_intensity,
            is_raining=is_raining,
            temperature_celsius=temperature_celsius,
            humidity_percent=humidity_percent,
            pressure_hpa=pressure_hpa,
            wind_speed_kmh=wind_speed_kmh,
            wind_direction_degrees=wind_direction_degrees,
            battery_level_percent=battery_level_percent,
            signal_strength_dbm=signal_strength_dbm,
        )

    def generate(self, count: int) -> list[rain_sensor_pb2.RainSensorReading]:
        """Generate ``count`` deterministic readings (same seed ⇒ same sequence)."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(
        self,
        start: int,
        end: int,
    ) -> list[rain_sensor_pb2.RainSensorReading]:
        """Generate readings for indices ``[start, end)``.  Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    # ── serialisation helpers ─────────────────────────────────────

    @staticmethod
    def reading_to_dict(reading: rain_sensor_pb2.RainSensorReading) -> dict:
        """Convert a RainSensorReading proto to a JSON-serialisable dict."""
        return json_format.MessageToDict(
            reading,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(
        path: str | Path,
        readings: list[rain_sensor_pb2.RainSensorReading],
    ) -> None:
        """Write readings to newline-delimited JSON (one object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for r in readings:
                f.write(json.dumps(GenRainSensor.reading_to_dict(r)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[rain_sensor_pb2.RainSensorReading]:
        """Read readings from a newline-delimited JSON file."""
        path = Path(path)
        readings: list[rain_sensor_pb2.RainSensorReading] = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                msg = rain_sensor_pb2.RainSensorReading()
                json_format.ParseDict(d, msg)
                readings.append(msg)
        return readings
