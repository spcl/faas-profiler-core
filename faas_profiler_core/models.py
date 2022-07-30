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

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from .constants import (
    AWSOperation,
    AWSServices,
    Provider,
    Service,
    Operation,
    Runtime,
    TriggerSynchronicity
)


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
Inbound Context
"""


@dataclass
class InboundContext:
    """
    Context definition for inbound requests
    """
    provider: Provider
    service: AWSServices
    operation: AWSOperation
    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED

    identifier: dict = field(default_factory=dict)
    tags: dict = field(default_factory=dict)


"""
Outbound Context
"""


@dataclass
class OutboundContext:
    """
    Context definition for outbound requests
    """
    provider: Provider
    service: AWSServices
    operation: AWSOperation

    invoked_at: datetime
    finished_at: datetime

    has_error: bool = False
    error_message: str = ""

    identifier: dict = field(default_factory=dict)


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
