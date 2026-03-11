from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Game(_message.Message):
    __slots__ = ("uuid", "name", "genre", "released_at")
    UUID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    GENRE_FIELD_NUMBER: _ClassVar[int]
    RELEASED_AT_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    name: str
    genre: str
    released_at: int
    def __init__(self, uuid: _Optional[str] = ..., name: _Optional[str] = ..., genre: _Optional[str] = ..., released_at: _Optional[int] = ...) -> None: ...
