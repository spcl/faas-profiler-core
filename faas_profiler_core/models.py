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

from marshmallow import EXCLUDE, ValidationError, fields
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from .constants import (
    Provider,
    Runtime,
    TriggerSynchronicity,
    operation_proxy,
    service_proxy,
    TRACE_ID_HEADER,
    INVOCATION_ID_HEADER,
    PARENT_ID_HEADER
)

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
            provider = Provider[data.get("provider")]
            service = service_proxy(provider)
            
            return service(value)
        except Exception as error:
            raise ValidationError(str(error)) from error
       
    
ServiceType = marshmallow_dataclass.NewType("ServiceType", str, field=ServiceProxy)


class OperationProxy(fields.Field):
    """
    Proxy for operatiom fields based on the provider
    """

    def _serialize(self, value, attr, obj, **kwargs):
        return value.value

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            provider = Provider[data.get("provider")]
            operation = operation_proxy(provider)
            
            return operation(value)
        except Exception as error:
            raise ValidationError(str(error)) from error
       
    
OperationType = marshmallow_dataclass.NewType("OperationType", str, field=OperationProxy)


@dataclass
class BaseModel:
    """
    Base model with method for loading and dumping.
    """

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

    class Meta:
        unknown = EXCLUDE


"""
Function Context
"""


@dataclass
class FunctionContext(BaseModel):
    """
    Context definition for serverless functions.
    """
    provider: Provider
    runtime: Runtime

    function_name: str
    handler: str

    invoked_at: datetime = None
    handler_executed_at: datetime = None
    handler_finished_at: datetime = None
    finished_at: datetime = None


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
            ctx[INVOCATION_ID_HEADER] = str(self.record_id)
        if self.parent_id:
            ctx[PARENT_ID_HEADER] = str(self.parent_id)

        return ctx


"""
Inbound and Outbound Context
"""

@dataclass
class BoundContext(BaseModel):
    """
    Base class for inbound and outbound context.
    """
    provider: Provider
    service: ServiceType
    operation: OperationType
    identifier: dict = field(default_factory=dict)
    tags: dict = field(default_factory=dict, metadata=dict(load_only=True))

    def set_identifier(self, key: Any, value: Any) -> None:
        """
        Sets a new context identifier
        """
        self.identifier[key] = value

    def set_tags(self, tags: dict) -> None:
        """
        Merges tags into stored tags
        """
        self.tags.update(tags)

@dataclass
class InboundContext(BoundContext):
    """
    Context definition for inbound requests
    """
    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED

@dataclass
class OutboundContext(BoundContext):
    """
    Context definition for outbound requests
    """
    invoked_at: datetime = None
    finished_at: datetime = None

    has_error: bool = False
    error_message: str = ""

    @property
    def instance(self) -> Any:
        return self.tags.get("_instance")

    @property
    def function(self) -> Any:
        return self.tags.get("_function")

    @property
    def args(self) -> Any:
        return self.tags.get("_args")

    @property
    def kwargs(self) -> Any:
        return self.tags.get("_kwargs")

    @property
    def response(self) -> Any:
        return self.tags.get("_response")

"""
Trace Record
"""


@dataclass
class TraceRecord(BaseModel):
    function_context: FunctionContext
    tracing_context: Optional[TracingContext]
    inbound_context: Optional[InboundContext]
    outbound_contexts: List[OutboundContext] = field(default_factory=list)
