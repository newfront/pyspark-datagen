import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SlotType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SLOT_TYPE_UNSPECIFIED: _ClassVar[SlotType]
    SLOT_TYPE_CENTER: _ClassVar[SlotType]
    SLOT_TYPE_LEFT_WING: _ClassVar[SlotType]
    SLOT_TYPE_RIGHT_WING: _ClassVar[SlotType]
    SLOT_TYPE_DEFENSE: _ClassVar[SlotType]
    SLOT_TYPE_GOALIE: _ClassVar[SlotType]
    SLOT_TYPE_UTILITY: _ClassVar[SlotType]
    SLOT_TYPE_BENCH: _ClassVar[SlotType]
SLOT_TYPE_UNSPECIFIED: SlotType
SLOT_TYPE_CENTER: SlotType
SLOT_TYPE_LEFT_WING: SlotType
SLOT_TYPE_RIGHT_WING: SlotType
SLOT_TYPE_DEFENSE: SlotType
SLOT_TYPE_GOALIE: SlotType
SLOT_TYPE_UTILITY: SlotType
SLOT_TYPE_BENCH: SlotType

class RosterEntry(_message.Message):
    __slots__ = ("player_id", "player_name", "slot_type", "fantasy_points", "goals", "assists", "shots_on_goal", "hits", "blocked_shots", "penalty_minutes", "plus_minus", "wins", "saves", "goals_against", "is_active")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NAME_FIELD_NUMBER: _ClassVar[int]
    SLOT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FANTASY_POINTS_FIELD_NUMBER: _ClassVar[int]
    GOALS_FIELD_NUMBER: _ClassVar[int]
    ASSISTS_FIELD_NUMBER: _ClassVar[int]
    SHOTS_ON_GOAL_FIELD_NUMBER: _ClassVar[int]
    HITS_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_SHOTS_FIELD_NUMBER: _ClassVar[int]
    PENALTY_MINUTES_FIELD_NUMBER: _ClassVar[int]
    PLUS_MINUS_FIELD_NUMBER: _ClassVar[int]
    WINS_FIELD_NUMBER: _ClassVar[int]
    SAVES_FIELD_NUMBER: _ClassVar[int]
    GOALS_AGAINST_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    player_id: str
    player_name: str
    slot_type: SlotType
    fantasy_points: float
    goals: int
    assists: int
    shots_on_goal: int
    hits: int
    blocked_shots: int
    penalty_minutes: int
    plus_minus: int
    wins: int
    saves: int
    goals_against: int
    is_active: bool
    def __init__(self, player_id: _Optional[str] = ..., player_name: _Optional[str] = ..., slot_type: _Optional[_Union[SlotType, str]] = ..., fantasy_points: _Optional[float] = ..., goals: _Optional[int] = ..., assists: _Optional[int] = ..., shots_on_goal: _Optional[int] = ..., hits: _Optional[int] = ..., blocked_shots: _Optional[int] = ..., penalty_minutes: _Optional[int] = ..., plus_minus: _Optional[int] = ..., wins: _Optional[int] = ..., saves: _Optional[int] = ..., goals_against: _Optional[int] = ..., is_active: _Optional[bool] = ...) -> None: ...

class FantasyRosterSnapshot(_message.Message):
    __slots__ = ("snapshot_id", "fantasy_team_id", "fantasy_team_name", "league_id", "scoring_week", "rank", "total_fantasy_points", "rank_delta", "roster", "captured_at")
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    FANTASY_TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    FANTASY_TEAM_NAME_FIELD_NUMBER: _ClassVar[int]
    LEAGUE_ID_FIELD_NUMBER: _ClassVar[int]
    SCORING_WEEK_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FANTASY_POINTS_FIELD_NUMBER: _ClassVar[int]
    RANK_DELTA_FIELD_NUMBER: _ClassVar[int]
    ROSTER_FIELD_NUMBER: _ClassVar[int]
    CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: str
    fantasy_team_id: str
    fantasy_team_name: str
    league_id: str
    scoring_week: str
    rank: int
    total_fantasy_points: float
    rank_delta: int
    roster: _containers.RepeatedCompositeFieldContainer[RosterEntry]
    captured_at: _timestamp_pb2.Timestamp
    def __init__(self, snapshot_id: _Optional[str] = ..., fantasy_team_id: _Optional[str] = ..., fantasy_team_name: _Optional[str] = ..., league_id: _Optional[str] = ..., scoring_week: _Optional[str] = ..., rank: _Optional[int] = ..., total_fantasy_points: _Optional[float] = ..., rank_delta: _Optional[int] = ..., roster: _Optional[_Iterable[_Union[RosterEntry, _Mapping]]] = ..., captured_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
