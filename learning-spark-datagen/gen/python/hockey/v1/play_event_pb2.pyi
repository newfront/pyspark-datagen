import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EVENT_TYPE_UNSPECIFIED: _ClassVar[EventType]
    EVENT_TYPE_GOAL: _ClassVar[EventType]
    EVENT_TYPE_ASSIST: _ClassVar[EventType]
    EVENT_TYPE_SHOT_ON_GOAL: _ClassVar[EventType]
    EVENT_TYPE_SAVE: _ClassVar[EventType]
    EVENT_TYPE_HIT: _ClassVar[EventType]
    EVENT_TYPE_BLOCKED_SHOT: _ClassVar[EventType]
    EVENT_TYPE_PENALTY: _ClassVar[EventType]
    EVENT_TYPE_FACEOFF_WIN: _ClassVar[EventType]
    EVENT_TYPE_FACEOFF_LOSS: _ClassVar[EventType]
    EVENT_TYPE_TAKEAWAY: _ClassVar[EventType]
    EVENT_TYPE_GIVEAWAY: _ClassVar[EventType]

class StrengthState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STRENGTH_STATE_UNSPECIFIED: _ClassVar[StrengthState]
    STRENGTH_STATE_EVEN: _ClassVar[StrengthState]
    STRENGTH_STATE_POWER_PLAY: _ClassVar[StrengthState]
    STRENGTH_STATE_PENALTY_KILL: _ClassVar[StrengthState]
    STRENGTH_STATE_EMPTY_NET: _ClassVar[StrengthState]
EVENT_TYPE_UNSPECIFIED: EventType
EVENT_TYPE_GOAL: EventType
EVENT_TYPE_ASSIST: EventType
EVENT_TYPE_SHOT_ON_GOAL: EventType
EVENT_TYPE_SAVE: EventType
EVENT_TYPE_HIT: EventType
EVENT_TYPE_BLOCKED_SHOT: EventType
EVENT_TYPE_PENALTY: EventType
EVENT_TYPE_FACEOFF_WIN: EventType
EVENT_TYPE_FACEOFF_LOSS: EventType
EVENT_TYPE_TAKEAWAY: EventType
EVENT_TYPE_GIVEAWAY: EventType
STRENGTH_STATE_UNSPECIFIED: StrengthState
STRENGTH_STATE_EVEN: StrengthState
STRENGTH_STATE_POWER_PLAY: StrengthState
STRENGTH_STATE_PENALTY_KILL: StrengthState
STRENGTH_STATE_EMPTY_NET: StrengthState

class PlayEvent(_message.Message):
    __slots__ = ("event_id", "game_id", "player_id", "event_type", "period", "game_clock", "strength_state", "secondary_player_id", "occurred_at", "season", "home_team", "away_team", "home_score", "away_score")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    GAME_CLOCK_FIELD_NUMBER: _ClassVar[int]
    STRENGTH_STATE_FIELD_NUMBER: _ClassVar[int]
    SECONDARY_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    SEASON_FIELD_NUMBER: _ClassVar[int]
    HOME_TEAM_FIELD_NUMBER: _ClassVar[int]
    AWAY_TEAM_FIELD_NUMBER: _ClassVar[int]
    HOME_SCORE_FIELD_NUMBER: _ClassVar[int]
    AWAY_SCORE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    game_id: str
    player_id: str
    event_type: EventType
    period: int
    game_clock: str
    strength_state: StrengthState
    secondary_player_id: str
    occurred_at: _timestamp_pb2.Timestamp
    season: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    def __init__(self, event_id: _Optional[str] = ..., game_id: _Optional[str] = ..., player_id: _Optional[str] = ..., event_type: _Optional[_Union[EventType, str]] = ..., period: _Optional[int] = ..., game_clock: _Optional[str] = ..., strength_state: _Optional[_Union[StrengthState, str]] = ..., secondary_player_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., season: _Optional[str] = ..., home_team: _Optional[str] = ..., away_team: _Optional[str] = ..., home_score: _Optional[int] = ..., away_score: _Optional[int] = ...) -> None: ...
