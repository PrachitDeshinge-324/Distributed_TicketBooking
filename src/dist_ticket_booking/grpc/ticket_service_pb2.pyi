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
    __slots__ = ("status", "token", "leader_hint", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEADER_HINT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    token: str
    leader_hint: str
    message: str
    def __init__(self, status: _Optional[str] = ..., token: _Optional[str] = ..., leader_hint: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class LogoutRequest(_message.Message):
    __slots__ = ("token",)
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: str
    def __init__(self, token: _Optional[str] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("status", "message", "leader_hint")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LEADER_HINT_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    leader_hint: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ..., leader_hint: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("status", "items", "leader_hint")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    LEADER_HINT_FIELD_NUMBER: _ClassVar[int]
    status: str
    items: _containers.RepeatedCompositeFieldContainer[DataItem]
    leader_hint: str
    def __init__(self, status: _Optional[str] = ..., items: _Optional[_Iterable[_Union[DataItem, _Mapping]]] = ..., leader_hint: _Optional[str] = ...) -> None: ...

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

class RequestVoteRequest(_message.Message):
    __slots__ = ("from_node", "to_node", "term", "last_log_index", "last_log_term")
    FROM_NODE_FIELD_NUMBER: _ClassVar[int]
    TO_NODE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    from_node: str
    to_node: str
    term: int
    last_log_index: int
    last_log_term: int
    def __init__(self, from_node: _Optional[str] = ..., to_node: _Optional[str] = ..., term: _Optional[int] = ..., last_log_index: _Optional[int] = ..., last_log_term: _Optional[int] = ...) -> None: ...

class RequestVoteResponse(_message.Message):
    __slots__ = ("from_node", "to_node", "term", "vote_granted")
    FROM_NODE_FIELD_NUMBER: _ClassVar[int]
    TO_NODE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    from_node: str
    to_node: str
    term: int
    vote_granted: bool
    def __init__(self, from_node: _Optional[str] = ..., to_node: _Optional[str] = ..., term: _Optional[int] = ..., vote_granted: _Optional[bool] = ...) -> None: ...

class RaftLogEntry(_message.Message):
    __slots__ = ("term", "index", "command_type", "command_json")
    TERM_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    COMMAND_TYPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_JSON_FIELD_NUMBER: _ClassVar[int]
    term: int
    index: int
    command_type: str
    command_json: str
    def __init__(self, term: _Optional[int] = ..., index: _Optional[int] = ..., command_type: _Optional[str] = ..., command_json: _Optional[str] = ...) -> None: ...

class AppendEntriesRequest(_message.Message):
    __slots__ = ("from_node", "to_node", "term", "prev_log_index", "prev_log_term", "commit_index", "entries")
    FROM_NODE_FIELD_NUMBER: _ClassVar[int]
    TO_NODE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    from_node: str
    to_node: str
    term: int
    prev_log_index: int
    prev_log_term: int
    commit_index: int
    entries: _containers.RepeatedCompositeFieldContainer[RaftLogEntry]
    def __init__(self, from_node: _Optional[str] = ..., to_node: _Optional[str] = ..., term: _Optional[int] = ..., prev_log_index: _Optional[int] = ..., prev_log_term: _Optional[int] = ..., commit_index: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[RaftLogEntry, _Mapping]]] = ...) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ("from_node", "to_node", "term", "entry_appended", "match_index")
    FROM_NODE_FIELD_NUMBER: _ClassVar[int]
    TO_NODE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    ENTRY_APPENDED_FIELD_NUMBER: _ClassVar[int]
    MATCH_INDEX_FIELD_NUMBER: _ClassVar[int]
    from_node: str
    to_node: str
    term: int
    entry_appended: bool
    match_index: int
    def __init__(self, from_node: _Optional[str] = ..., to_node: _Optional[str] = ..., term: _Optional[int] = ..., entry_appended: _Optional[bool] = ..., match_index: _Optional[int] = ...) -> None: ...
