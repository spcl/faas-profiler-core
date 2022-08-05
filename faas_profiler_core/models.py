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

from functools import partial
import marshmallow_dataclass

from typing import Any, List, Optional
from marshmallow import EXCLUDE, ValidationError, fields, validate
from marshmallow_dataclass import NewType
from marshmallow_enum import EnumField
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from .constants import (
    Provider,
    Runtime,
    TriggerSynchronicity,
    operation_proxy,
    service_proxy,
    TRACE_ID_HEADER,
    RECORD_ID_HEADER,
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
        self.identifiers.update(identifiers)

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
