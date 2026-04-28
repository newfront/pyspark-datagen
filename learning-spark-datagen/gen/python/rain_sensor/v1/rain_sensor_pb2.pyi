from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RainIntensity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RAIN_INTENSITY_UNSPECIFIED: _ClassVar[RainIntensity]
    RAIN_INTENSITY_NONE: _ClassVar[RainIntensity]
    RAIN_INTENSITY_LIGHT: _ClassVar[RainIntensity]
    RAIN_INTENSITY_MODERATE: _ClassVar[RainIntensity]
    RAIN_INTENSITY_HEAVY: _ClassVar[RainIntensity]
    RAIN_INTENSITY_VIOLENT: _ClassVar[RainIntensity]
RAIN_INTENSITY_UNSPECIFIED: RainIntensity
RAIN_INTENSITY_NONE: RainIntensity
RAIN_INTENSITY_LIGHT: RainIntensity
RAIN_INTENSITY_MODERATE: RainIntensity
RAIN_INTENSITY_HEAVY: RainIntensity
RAIN_INTENSITY_VIOLENT: RainIntensity

class GeoLocation(_message.Message):
    __slots__ = ("latitude", "longitude", "altitude_meters", "place_name")
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    ALTITUDE_METERS_FIELD_NUMBER: _ClassVar[int]
    PLACE_NAME_FIELD_NUMBER: _ClassVar[int]
    latitude: float
    longitude: float
    altitude_meters: float
    place_name: str
    def __init__(self, latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., altitude_meters: _Optional[float] = ..., place_name: _Optional[str] = ...) -> None: ...

class RainSensorReading(_message.Message):
    __slots__ = ("reading_id", "sensor_id", "timestamp_ms", "location", "precipitation_mm", "rain_intensity", "is_raining", "temperature_celsius", "humidity_percent", "pressure_hpa", "wind_speed_kmh", "wind_direction_degrees", "battery_level_percent", "signal_strength_dbm")
    READING_ID_FIELD_NUMBER: _ClassVar[int]
    SENSOR_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    PRECIPITATION_MM_FIELD_NUMBER: _ClassVar[int]
    RAIN_INTENSITY_FIELD_NUMBER: _ClassVar[int]
    IS_RAINING_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_CELSIUS_FIELD_NUMBER: _ClassVar[int]
    HUMIDITY_PERCENT_FIELD_NUMBER: _ClassVar[int]
    PRESSURE_HPA_FIELD_NUMBER: _ClassVar[int]
    WIND_SPEED_KMH_FIELD_NUMBER: _ClassVar[int]
    WIND_DIRECTION_DEGREES_FIELD_NUMBER: _ClassVar[int]
    BATTERY_LEVEL_PERCENT_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_STRENGTH_DBM_FIELD_NUMBER: _ClassVar[int]
    reading_id: str
    sensor_id: str
    timestamp_ms: int
    location: GeoLocation
    precipitation_mm: float
    rain_intensity: RainIntensity
    is_raining: bool
    temperature_celsius: float
    humidity_percent: float
    pressure_hpa: float
    wind_speed_kmh: float
    wind_direction_degrees: float
    battery_level_percent: int
    signal_strength_dbm: int
    def __init__(self, reading_id: _Optional[str] = ..., sensor_id: _Optional[str] = ..., timestamp_ms: _Optional[int] = ..., location: _Optional[_Union[GeoLocation, _Mapping]] = ..., precipitation_mm: _Optional[float] = ..., rain_intensity: _Optional[_Union[RainIntensity, str]] = ..., is_raining: _Optional[bool] = ..., temperature_celsius: _Optional[float] = ..., humidity_percent: _Optional[float] = ..., pressure_hpa: _Optional[float] = ..., wind_speed_kmh: _Optional[float] = ..., wind_direction_degrees: _Optional[float] = ..., battery_level_percent: _Optional[int] = ..., signal_strength_dbm: _Optional[int] = ...) -> None: ...
