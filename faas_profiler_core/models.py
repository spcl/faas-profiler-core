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

from operator import add
import marshmallow_dataclass

from functools import partial, reduce
from socket import AddressFamily
from typing import Any, Dict, List, Optional
from marshmallow import EXCLUDE, ValidationError, fields, validate
from marshmallow_dataclass import NewType
from marshmallow_enum import EnumField
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from .constants import (
    AWSOperation,
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
    provider: ProviderType
    runtime: RuntimeType

    function_name: str = field(metadata=dict(validate=validate.Length(min=1)))
    handler: str = field(metadata=dict(validate=validate.Length(min=1)))

    invoked_at: datetime = None
    handler_executed_at: datetime = None
    handler_finished_at: datetime = None
    finished_at: datetime = None

    @property
    def function_key(self):
        """
        Returns a unique key for the function.
        """
        return self.provider.value + "::" + self.function_name

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

    invoked_at: datetime = None

    def set_identifiers(self, identifiers: dict) -> None:
        """
        Merges identifier into stored identifier
        """
        self.identifier.update(identifiers)

    def set_tags(self, tags: dict) -> None:
        """
        Merges tags into stored tags
        """
        self.tags.update(tags)

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
    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED


@dataclass
class OutboundContext(RequestContext):
    """
    Context definition for outbound requests
    """
    finished_at: datetime = None

    has_error: bool = False
    error_message: str = ""


"""
Record data
"""


@dataclass
class RecordData(BaseModel):
    name: str
    results: Any


"""
Trace Record
"""


@dataclass
class TraceRecord(BaseModel):
    function_context: FunctionContext
    tracing_context: Optional[TracingContext]
    inbound_context: Optional[InboundContext]
    outbound_contexts: List[OutboundContext] = field(default_factory=list)

    data: List[RecordData] = field(default_factory=list)

    @property
    def execution_time(self):
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
    def handler_time(self):
        """
        Returns the execution time of the handler in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.handler_execution_time


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
    measuring_points: List[float] = field(default_factory=list)

    @property
    def average(self) -> float:
        n = len(self.measuring_points)
        total = reduce(add, self.measuring_points, 0)

        return total / n


"""
CPU Measurement
"""


@dataclass
class CPUUsage(BaseModel):
    interval: float
    measuring_points: List[float] = field(default_factory=list)

    @property
    def average(self) -> float:
        n = len(self.measuring_points)
        total = reduce(add, self.measuring_points, 0)

        return total / n


"""
Network Measurement
"""


@dataclass
class NetworkConnectionItem(BaseModel):
    socket_descriptor: int
    socket_family: AddressFamily
    local_address: str
    remote_address: str


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


"""
Captures
"""


@dataclass
class S3CaptureItem(BaseModel):
    operation: AWSOperation
    parameters: dict = field(default_factory=dict)
    bucket_name: str = UNAVAILABLE
    object_key: str = UNAVAILABLE
    object_size: float = 0.0
    request_method: str = UNAVAILABLE
    request_status: str = UNAVAILABLE
    execution_time: float = 0.0
    request_url: str = UNAVAILABLE
    request_uri: str = UNAVAILABLE
