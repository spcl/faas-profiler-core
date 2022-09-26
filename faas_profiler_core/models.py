#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Models and Schemas:
- FunctionContext
- TracingContext
- InboundContext
- OutboundContext
- TraceRecord
"""

import marshmallow_dataclass

from functools import partial
from socket import AddressFamily
from typing import Any, Dict, List, Optional, Set, Tuple, Type
from marshmallow import EXCLUDE, ValidationError, fields
from marshmallow_dataclass import NewType
from marshmallow_enum import EnumField
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4

from .constants import (
    RecordDataType,
    InternalOperation,
    Provider,
    Runtime,
    TriggerSynchronicity,
    operation_proxy,
    service_proxy,
    TRACE_ID_HEADER,
    RECORD_ID_HEADER,
    PARENT_ID_HEADER
)

UNAVAILABLE = "unavailable"
UNIDENTIFIED = "unidentified"

KEY_VALUE_DELIMITER = "#"
IDENTIFIER_DELIMITER = 2 * KEY_VALUE_DELIMITER
FUNCTION_KEY_DELLIMITER = "::"

"""
Custom Fields and Types
"""


class ServiceProxy(fields.Field):
    """
    Proxy for service fields based on the provider
    """

    def _serialize(self, value, attr, obj, **kwargs):
        return value.value

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            provider = Provider(data.get("provider"))
            service = service_proxy(provider)

            return service(value)
        except Exception as error:
            raise ValidationError(str(error)) from error


ServiceType = marshmallow_dataclass.NewType(
    "ServiceType", str, field=ServiceProxy)


class OperationProxy(fields.Field):
    """
    Proxy for operatiom fields based on the provider
    """

    def _serialize(self, value, attr, obj, **kwargs):
        return value.value

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            provider = Provider(data.get("provider"))
            operation = operation_proxy(provider)

            return operation(value)
        except Exception as error:
            raise ValidationError(str(error)) from error


OperationType = NewType("OperationType", str, field=OperationProxy)


@dataclass
class BaseModel:
    """
    Base model with method for loading and dumping.
    """

    class Meta:
        unknown = EXCLUDE

    @classmethod
    def valid(cls, data: Any) -> bool:
        """
        Checks if data is valid.
        """
        schema = marshmallow_dataclass.class_schema(cls)()
        return schema.validate(data)

    @classmethod
    def load(cls, data: Any):
        """
        Loads the dataclass based with data
        """
        schema = marshmallow_dataclass.class_schema(cls)()
        return schema.load(data)

    def dump(self) -> dict:
        """
        Dumps the dataclass to dict.
        """
        schema = marshmallow_dataclass.class_schema(self.__class__)()
        return schema.dump(self)


"""
Function Context
"""


ProviderType = NewType(
    "ProviderType",
    Provider,
    field=partial(
        EnumField,
        Provider,
        by_value=True))

RuntimeType = NewType(
    "RuntimeType", Provider, field=partial(EnumField, Runtime, by_value=True))


@dataclass
class FunctionContext(BaseModel):
    """
    Context definition for serverless functions.
    """
    provider: ProviderType = Provider.UNIDENTIFIED
    runtime: RuntimeType = Runtime.UNIDENTIFIED
    region: str = UNIDENTIFIED

    function_name: str = UNIDENTIFIED
    handler: str = UNIDENTIFIED

    invoked_at: datetime = None
    handler_executed_at: datetime = None
    handler_finished_at: datetime = None
    finished_at: datetime = None

    max_memory: int = None
    max_execution_time: int = None

    has_error: bool = False
    error_type: str = None
    error_message: str = None
    traceback: List[str] = field(default_factory=list)
    response: Any = None
    arguments: dict = field(default_factory=dict)
    environment_variables: dict = field(default_factory=dict)

    @property
    def function_key(self):
        """
        Returns a unique key for the function.
        """
        return FUNCTION_KEY_DELLIMITER.join(
            [self.provider.value, self.function_name])

    @property
    def handler_execution_time(self):
        """
        Returns the handler execution time in ms
        """
        if not (self.handler_executed_at or self.handler_finished_at):
            return

        delta = self.handler_finished_at - self.handler_executed_at
        return delta.total_seconds() * 1000

    @property
    def total_execution_time(self):
        """
        Returns the total executuon time in ms
        """
        if not (self.invoked_at or self.finished_at):
            return

        delta = self.finished_at - self.invoked_at
        return delta.total_seconds() * 1000

    @property
    def profiler_time(self):
        """
        Returns the total time for the profiler (overhead)
        """
        if not (self.total_execution_time or self.handler_execution_time):
            return

        return self.total_execution_time - self.handler_execution_time


"""
Tracing Context
"""


@dataclass
class TracingContext(BaseModel):
    """
    Context definition for tracing
    """
    trace_id: UUID
    record_id: UUID
    parent_id: UUID = None

    def __str__(self) -> str:
        """
        Return tracing as string
        """
        return "TracingContext: TraceID={trace_id}, RecordID={record_id}, ParentID={parent_id}".format(
            trace_id=self.trace_id, record_id=self.record_id, parent_id=self.parent_id)

    def to_injectable(self) -> dict:
        """
        Returns the context as injectable context.
        """
        ctx = {}
        if self.trace_id:
            ctx[TRACE_ID_HEADER] = str(self.trace_id)
        if self.record_id:
            ctx[RECORD_ID_HEADER] = str(self.record_id)
        if self.parent_id:
            ctx[PARENT_ID_HEADER] = str(self.parent_id)

        return ctx


"""
Inbound and Outbound Context
"""


@dataclass
class RequestContext(BaseModel):
    """
    Base class for inbound and outbound context.
    """
    provider: ProviderType
    service: ServiceType
    operation: OperationType
    identifier: dict = field(default_factory=dict)
    tags: dict = field(default_factory=dict, metadata=dict(load_only=True))

    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED

    invoked_at: datetime = None

    def __str__(self) -> str:
        """
        Return request context as string
        """
        return "RequestContext: Provider={provider}, Service={service}, Operation={operation}, identifier={identifier}".format(
            provider=self.provider, service=self.service, operation=self.operation, identifier=self.identifier_string)

    def set_identifiers(self, identifiers: dict) -> None:
        """
        Merges identifier into stored identifier
        """
        _identifiers = {str(k): str(v) for k, v in identifiers.items()}

        if any(KEY_VALUE_DELIMITER in k for k in _identifiers.keys()):
            raise ValidationError(
                f"Keys of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

        if any(KEY_VALUE_DELIMITER in v for v in _identifiers.values()):
            raise ValidationError(
                f"Values of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

        self.identifier.update(_identifiers)

    def set_tags(self, tags: dict) -> None:
        """
        Merges tags into stored tags
        """
        self.tags.update(tags)

    @property
    def identifier_string(self) -> str:
        """
        Returns the identifiers as string.
        """
        _identifier = {str(k): str(v) for k, v in self.identifier.items()}
        _identifier = sorted(
            _identifier.items(),
            key=lambda x: x[0],
            reverse=False)
        _identifier = map(
            lambda id: KEY_VALUE_DELIMITER.join(id),
            _identifier)

        return IDENTIFIER_DELIMITER.join(_identifier)

    @property
    def resolvable(self) -> bool:
        """
        Returns True if request context is resolvable.
        This is, if identifier are set.
        """
        return self.identifier is not None and self.identifier != {}


@dataclass
class InboundContext(RequestContext):
    """
    Context definition for inbound requests
    """
    # After trace processing we eventually know, when the parent finished the trigger
    # execution.
    # TODO: Name this better
    trigger_finished_at: datetime = None

    @property
    def trigger_overhead_time(self) -> float:
        """
        Returns the time between parent finished SDK call and child gets executed
        """
        if self.trigger_finished_at is None or self.invoked_at is None:
            return

        delta = self.invoked_at - self.trigger_finished_at
        return delta.total_seconds() * 1000


@dataclass
class OutboundContext(RequestContext):
    """
    Context definition for outbound requests
    """
    finished_at: datetime = None

    has_error: bool = False
    error_message: str = ""

    @property
    def overhead_time(self) -> float:
        """
        Returns the overhead time (ms) for this outbound context.
        """
        if self.finished_at is None or self.invoked_at is None:
            return None

        delta = self.finished_at - self.invoked_at
        return delta.total_seconds() * 1000


"""
Record data
"""


@dataclass
class RecordData(BaseModel):
    name: str
    type: RecordDataType
    results: Any


"""
Trace Record
"""


@dataclass
class TraceRecord(BaseModel):
    function_context: FunctionContext
    tracing_context: TracingContext

    inbound_context: Optional[InboundContext]
    outbound_contexts: List[OutboundContext] = field(default_factory=list)

    data: Dict[str, RecordData] = field(default_factory=dict)

    """
    Record shortcut properties
    """

    def __str__(self) -> str:
        record_str = self.record_name

        if self.record_id:
            record_str += f" - {str(self.record_id)[:8]}"

        if self.total_execution_time:
            record_str += " - ({:.2f} ms)".format(self.total_execution_time)

        return record_str

    @property
    def function_key(self) -> str:
        """
        Returns the function key of the function context.
        """
        return self.function_context.function_key

    @property
    def trace_id(self):
        """
        Returns the trace id.
        """
        return self.tracing_context.trace_id

    @property
    def record_id(self):
        """
        Returns the record id.
        """
        return self.tracing_context.record_id

    @property
    def record_name(self):
        """
        Returns the record name, composed of provider and function name
        """
        func_ctx = self.function_context
        if not func_ctx:
            return FUNCTION_KEY_DELLIMITER.join([
                Provider.UNIDENTIFIED.value,
                UNIDENTIFIED])

        return f"{func_ctx.provider.value}::{func_ctx.function_name}"

    @property
    def total_execution_time(self):
        """
        Returns the execution time of the trace in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.total_execution_time

    @property
    def overhead_time(self):
        """
        Returns the overhead time spent by the profiler in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.profiler_time

    @property
    def handler_execution_time(self):
        """
        Returns the execution time of the handler in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.handler_execution_time


"""
Trace
"""


@dataclass
class Trace(BaseModel):
    """
    Represents a single trace for one function.
    """

    trace_id: UUID
    records: Dict[UUID, TraceRecord] = field(default_factory=dict)

    root_record_id: UUID = None
    invoked_at: datetime = datetime.max
    finished_at: datetime = datetime.min

    def __str__(self) -> str:
        """
        Returns string representation of the trace.
        """
        return f"Trace: {self.trace_id} - {len(self.records)} Records"

    @property
    def duration(self) -> float:
        """
        Returns the duration in ms of the trace.
        """
        if not self.invoked_at or not self.finished_at:
            return None

        delta = self.finished_at - self.invoked_at
        return delta.total_seconds() * 1e4

    def add_record(self, record: Type[TraceRecord]) -> None:
        """
        Adds a record to the trace.

        Updates the tracing context to set the trace ID correctly.
        """
        trace_ctx = record.tracing_context
        func_ctx = record.function_context
        if trace_ctx:
            trace_ctx.trace_id = self.trace_id
        else:
            record.tracing_context = TracingContext(
                trace_id=self.trace_id, record_id=uuid4())

        if func_ctx and func_ctx.invoked_at:
            self.invoked_at = min(self.invoked_at, func_ctx.invoked_at)

        if func_ctx and func_ctx.finished_at:
            self.finished_at = max(self.finished_at, func_ctx.finished_at)

        self.records[record.tracing_context.record_id] = record


"""
Profile
"""


@dataclass
class Profile(BaseModel):
    """
    Represents a single profile run, consisting of mutliple traces
    """
    profile_id: UUID
    function_context: FunctionContext
    trace_ids: Set[UUID] = field(default_factory=set)

    @property
    def number_of_traces(self) -> int:
        """
        Returns the number of traces.
        """
        return len(self.traces)

    @property
    def title(self) -> str:
        """
        Return the title of the profile
        """
        if not self.function_context:
            return str(self.profile_id)

        return f"{self.function_context.function_key} ({str(self.profile_id)[:8]})"


"""
Memory Measurement
"""


@dataclass
class MemoryLineUsageItem(BaseModel):
    line_number: int
    content: str
    occurrences: int
    memory_increment: float
    memory_total: float


@dataclass
class MemoryLineUsage(BaseModel):
    line_memories: List[MemoryLineUsageItem] = field(
        default_factory=list)


@dataclass
class MemoryUsage(BaseModel):
    interval: float
    measuring_points: List[Tuple[float, float]] = field(default_factory=list)


"""
CPU Measurement
"""


@dataclass
class CPUUsage(BaseModel):
    interval: float
    measuring_points: List[Tuple[float, float]] = field(default_factory=list)


"""
Network Measurement
"""


@dataclass
class NetworkConnectionItem(BaseModel):
    socket_family: AddressFamily
    remote_address: str
    number_of_connections: int


@dataclass
class NetworkConnections(BaseModel):
    connections: List[NetworkConnectionItem] = field(
        default_factory=list)


@dataclass
class NetworkIOCounters(BaseModel):
    bytes_sent: int
    bytes_received: int
    packets_sent: int
    packets_received: int
    error_in: int
    error_out: int
    drop_in: int
    drop_out: int


"""
Disk Measurement
"""


@dataclass
class DiskIOCounters(BaseModel):
    read_count: int
    write_count: int
    read_bytes: int
    write_bytes: int


"""
Informations
"""


@dataclass
class InformationEnvironment(BaseModel):
    runtime_name: str = UNAVAILABLE
    runtime_version: str = UNAVAILABLE
    runtime_implementation: str = UNAVAILABLE
    runtime_compiler: str = UNAVAILABLE
    byte_order: str = UNAVAILABLE
    platform: str = UNAVAILABLE
    interpreter_path: str = UNAVAILABLE
    packages: List[str] = field(default_factory=list)


@dataclass
class InformationOperatingSystem(BaseModel):
    boot_time: datetime
    system: str = UNAVAILABLE
    node_name: str = UNAVAILABLE
    release: str = UNAVAILABLE
    machine: str = UNAVAILABLE


@dataclass
class InformationIsWarm(BaseModel):
    is_warm: bool = False
    warm_since: datetime = UNAVAILABLE
    warm_for: int = 0


@dataclass
class InformationTimeShift(BaseModel):
    server: str = UNAVAILABLE
    offset: float = 0.0


"""
Captures
"""


@dataclass
class S3AccessItem(BaseModel):
    mode: str
    bucket_name: str
    object_key: str
    object_size: float
    execution_time: float

@dataclass
class S3Accesses(BaseModel):
    accesses: List[S3AccessItem] = field(
        default_factory=list)


@dataclass
class EFSAccessItem(BaseModel):
    mode: str
    file: str
    file_size: float
    execution_time: float


@dataclass
class EFSAccesses(BaseModel):
    mount_point: str
    accesses: List[EFSAccessItem] = field(
        default_factory=list)
