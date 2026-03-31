import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Position(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    POSITION_UNSPECIFIED: _ClassVar[Position]
    POSITION_CENTER: _ClassVar[Position]
    POSITION_LEFT_WING: _ClassVar[Position]
    POSITION_RIGHT_WING: _ClassVar[Position]
    POSITION_DEFENSE: _ClassVar[Position]
    POSITION_GOALIE: _ClassVar[Position]

class PlayerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAYER_STATUS_UNSPECIFIED: _ClassVar[PlayerStatus]
    PLAYER_STATUS_ACTIVE: _ClassVar[PlayerStatus]
    PLAYER_STATUS_INJURED: _ClassVar[PlayerStatus]
    PLAYER_STATUS_SUSPENDED: _ClassVar[PlayerStatus]

class Handedness(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HANDEDNESS_UNSPECIFIED: _ClassVar[Handedness]
    HANDEDNESS_LEFT: _ClassVar[Handedness]
    HANDEDNESS_RIGHT: _ClassVar[Handedness]
POSITION_UNSPECIFIED: Position
POSITION_CENTER: Position
POSITION_LEFT_WING: Position
POSITION_RIGHT_WING: Position
POSITION_DEFENSE: Position
POSITION_GOALIE: Position
PLAYER_STATUS_UNSPECIFIED: PlayerStatus
PLAYER_STATUS_ACTIVE: PlayerStatus
PLAYER_STATUS_INJURED: PlayerStatus
PLAYER_STATUS_SUSPENDED: PlayerStatus
HANDEDNESS_UNSPECIFIED: Handedness
HANDEDNESS_LEFT: Handedness
HANDEDNESS_RIGHT: Handedness

class HockeyPlayer(_message.Message):
    __slots__ = ("player_id", "first_name", "last_name", "team_name", "team_abbreviation", "position", "jersey_number", "nationality", "shoots_catches", "status", "created_at")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    FIRST_NAME_FIELD_NUMBER: _ClassVar[int]
    LAST_NAME_FIELD_NUMBER: _ClassVar[int]
    TEAM_NAME_FIELD_NUMBER: _ClassVar[int]
    TEAM_ABBREVIATION_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    JERSEY_NUMBER_FIELD_NUMBER: _ClassVar[int]
    NATIONALITY_FIELD_NUMBER: _ClassVar[int]
    SHOOTS_CATCHES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    player_id: str
    first_name: str
    last_name: str
    team_name: str
    team_abbreviation: str
    position: Position
    jersey_number: int
    nationality: str
    shoots_catches: Handedness
    status: PlayerStatus
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, player_id: _Optional[str] = ..., first_name: _Optional[str] = ..., last_name: _Optional[str] = ..., team_name: _Optional[str] = ..., team_abbreviation: _Optional[str] = ..., position: _Optional[_Union[Position, str]] = ..., jersey_number: _Optional[int] = ..., nationality: _Optional[str] = ..., shoots_catches: _Optional[_Union[Handedness, str]] = ..., status: _Optional[_Union[PlayerStatus, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
