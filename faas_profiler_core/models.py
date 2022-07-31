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

from marshmallow import ValidationError, fields
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from .constants import (
    Provider,
    Runtime,
    TriggerSynchronicity,
    operation_proxy,
    service_proxy
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


"""
Function Context
"""


@dataclass
class FunctionContext:
    """
    Context definition for serverless functions.
    """
    provider: Provider
    runtime: Runtime

    function_name: str
    handler: str

    # invoked_at: Type[datetime]
    # handler_executed_at = Type[datetime]
    # handler_finished_at = Type[datetime]
    # finished_at = Type[datetime]


"""
Tracing Context
"""


@dataclass
class TracingContext:
    """
    Context definition for tracing
    """
    trace_id: UUID
    record_id: UUID
    parent_id: UUID = None


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
    identifier: dict

    def set_identifier(self, key: Any, value: Any) -> None:
        """
        Sets a new context identifier
        """
        self.identifier[key] = value

@dataclass
class InboundContext(BoundContext):
    """
    Context definition for inbound requests
    """
    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED
    tags: dict = field(default_factory=dict)

    def set_tags(self, tags: dict) -> None:
        """
        Merges tags into stored tags
        """
        self.tags.update(tags)

@dataclass
class OutboundContext(BoundContext):
    """
    Context definition for outbound requests
    """
    invoked_at: datetime
    finished_at: datetime

    has_error: bool = False
    error_message: str = ""


"""
Trace Record
"""


@dataclass
class TraceRecord(BaseModel):
    function_context: FunctionContext
    tracing_context: Optional[TracingContext]
    inbound_context: Optional[InboundContext]
    outbound_contexts: List[OutboundContext] = field(default_factory=list)

    recorded_at: datetime = datetime.now()
