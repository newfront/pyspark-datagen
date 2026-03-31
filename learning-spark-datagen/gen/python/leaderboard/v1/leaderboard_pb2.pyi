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

class SortMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SORT_METHOD_UNSPECIFIED: _ClassVar[SortMethod]
    SORT_METHOD_DESCENDING: _ClassVar[SortMethod]
    SORT_METHOD_ASCENDING: _ClassVar[SortMethod]

class DisplayType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISPLAY_TYPE_UNSPECIFIED: _ClassVar[DisplayType]
    DISPLAY_TYPE_NUMERIC: _ClassVar[DisplayType]
    DISPLAY_TYPE_TIME_SECONDS: _ClassVar[DisplayType]
    DISPLAY_TYPE_TIME_MILLISECONDS: _ClassVar[DisplayType]
SORT_METHOD_UNSPECIFIED: SortMethod
SORT_METHOD_DESCENDING: SortMethod
SORT_METHOD_ASCENDING: SortMethod
DISPLAY_TYPE_UNSPECIFIED: DisplayType
DISPLAY_TYPE_NUMERIC: DisplayType
DISPLAY_TYPE_TIME_SECONDS: DisplayType
DISPLAY_TYPE_TIME_MILLISECONDS: DisplayType

class LeaderboardEntry(_message.Message):
    __slots__ = ("rank", "player_id", "player_name", "score", "previous_rank", "rank_delta", "score_delta", "hours_played")
    RANK_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NAME_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_RANK_FIELD_NUMBER: _ClassVar[int]
    RANK_DELTA_FIELD_NUMBER: _ClassVar[int]
    SCORE_DELTA_FIELD_NUMBER: _ClassVar[int]
    HOURS_PLAYED_FIELD_NUMBER: _ClassVar[int]
    rank: int
    player_id: str
    player_name: str
    score: int
    previous_rank: int
    rank_delta: int
    score_delta: int
    hours_played: float
    def __init__(self, rank: _Optional[int] = ..., player_id: _Optional[str] = ..., player_name: _Optional[str] = ..., score: _Optional[int] = ..., previous_rank: _Optional[int] = ..., rank_delta: _Optional[int] = ..., score_delta: _Optional[int] = ..., hours_played: _Optional[float] = ...) -> None: ...

class LeaderboardSnapshot(_message.Message):
    __slots__ = ("snapshot_id", "leaderboard_id", "leaderboard_name", "game_id", "captured_at", "total_entries", "sort_method", "display_type", "entries")
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    LEADERBOARD_ID_FIELD_NUMBER: _ClassVar[int]
    LEADERBOARD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAME_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    SORT_METHOD_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: str
    leaderboard_id: str
    leaderboard_name: str
    game_id: str
    captured_at: _timestamp_pb2.Timestamp
    total_entries: int
    sort_method: SortMethod
    display_type: DisplayType
    entries: _containers.RepeatedCompositeFieldContainer[LeaderboardEntry]
    def __init__(self, snapshot_id: _Optional[str] = ..., leaderboard_id: _Optional[str] = ..., leaderboard_name: _Optional[str] = ..., game_id: _Optional[str] = ..., captured_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., total_entries: _Optional[int] = ..., sort_method: _Optional[_Union[SortMethod, str]] = ..., display_type: _Optional[_Union[DisplayType, str]] = ..., entries: _Optional[_Iterable[_Union[LeaderboardEntry, _Mapping]]] = ...) -> None: ...
