from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LoginRequest(_message.Message):
    __slots__ = ("username", "password")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: str
    def __init__(self, username: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class LoginResponse(_message.Message):
    __slots__ = ("status", "token")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    status: str
    token: str
    def __init__(self, status: _Optional[str] = ..., token: _Optional[str] = ...) -> None: ...

class LogoutRequest(_message.Message):
    __slots__ = ("token",)
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: str
    def __init__(self, token: _Optional[str] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class PostRequest(_message.Message):
    __slots__ = ("token", "type", "data")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    token: str
    type: str
    data: str
    def __init__(self, token: _Optional[str] = ..., type: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("token", "type", "params")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    token: str
    type: str
    params: str
    def __init__(self, token: _Optional[str] = ..., type: _Optional[str] = ..., params: _Optional[str] = ...) -> None: ...

class DataItem(_message.Message):
    __slots__ = ("id", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    data: str
    def __init__(self, id: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("status", "items")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    status: str
    items: _containers.RepeatedCompositeFieldContainer[DataItem]
    def __init__(self, status: _Optional[str] = ..., items: _Optional[_Iterable[_Union[DataItem, _Mapping]]] = ...) -> None: ...

class BusinessRequest(_message.Message):
    __slots__ = ("request_id", "payload", "context")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    payload: str
    context: str
    def __init__(self, request_id: _Optional[str] = ..., payload: _Optional[str] = ..., context: _Optional[str] = ...) -> None: ...

class LLMQueryRequest(_message.Message):
    __slots__ = ("request_id", "query", "context")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    query: str
    context: str
    def __init__(self, request_id: _Optional[str] = ..., query: _Optional[str] = ..., context: _Optional[str] = ...) -> None: ...

class GetLLMAnswerResponse(_message.Message):
    __slots__ = ("request_id", "answer")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    answer: str
    def __init__(self, request_id: _Optional[str] = ..., answer: _Optional[str] = ...) -> None: ...

class LLMQueryResponse(_message.Message):
    __slots__ = ("request_id", "answer")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    answer: str
    def __init__(self, request_id: _Optional[str] = ..., answer: _Optional[str] = ...) -> None: ...
