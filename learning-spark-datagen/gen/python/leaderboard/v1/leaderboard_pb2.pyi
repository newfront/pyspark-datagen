from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class LeaderboardEntry(_message.Message):
    __slots__ = ("user_id", "game_id", "score", "rank", "played_at")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    PLAYED_AT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    game_id: str
    score: int
    rank: int
    played_at: int
    def __init__(self, user_id: _Optional[str] = ..., game_id: _Optional[str] = ..., score: _Optional[int] = ..., rank: _Optional[int] = ..., played_at: _Optional[int] = ...) -> None: ...
