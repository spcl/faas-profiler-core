#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inbound and Outbound recording
"""
from __future__ import annotations

import boto3

from typing import Type
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from marshmallow import fields, ValidationError
from marshmallow_dataclass import NewType
from dataclasses import dataclass, field
from enum import Enum

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError

from .logging import Loggable
from .models import BaseModel, BoundContext, InboundContext, TracingContext, OutboundContext
from .constants import Provider

"""
Request Table Exceptions
"""


class InvalidContextException(Exception):
    pass


class RequestTableError(Exception):
    pass


"""
Request Record Model
"""


class RecordTypes(Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class IdentifierField(fields.Field):
    """
    Identifier Field.

    On serialize it creates a string out of the dict
    On deserialize it splits the string back to a dict.
    """

    KEY_VALUE_DELIMITER = "#"
    IDENTIFIER_DELIMITER = 2 * KEY_VALUE_DELIMITER

    def _serialize(self, value, attr, obj: Type[RequestRecord], **kwargs):
        """
        Transforms the identifier dict to identifier string.
        """
        if not isinstance(value, dict):
            raise ValidationError(
                f"Record identifier must be a dict, got {type(value)}")

        identifier = {str(k): str(v) for k, v in value.items()}
        self._check_for_delimiter_in_dict(identifier)
        self._check_for_record_type_in_dict(identifier)

        record_type = obj.record_type
        if record_type is None or record_type not in RecordTypes:
            raise ValidationError(
                f"Record type {record_type} is invalid, valid are {list(RecordTypes)}")

        # Sort dict based on keys descending.
        identifier = sorted(
            identifier.items(),
            key=lambda x: x[0],
            reverse=False)
        identifier = map(
            lambda id: self.KEY_VALUE_DELIMITER.join(id),
            identifier)

        return record_type.value + self.IDENTIFIER_DELIMITER + \
            self.IDENTIFIER_DELIMITER.join(identifier)

    def _deserialize(self, value, attr, data, **kwargs):
        breakpoint()

    def _check_for_delimiter_in_dict(self, identifier: dict) -> None:
        """
        Raises ValidationError if keys or values of identifier dict contains the delimiter symbol.
        """
        if any(self.KEY_VALUE_DELIMITER in k for k in identifier.keys()):
            raise ValidationError(
                f"Keys of identifier dict must not contain any '{self.KEY_VALUE_DELIMITER}'.")

        if any(self.KEY_VALUE_DELIMITER in v for v in identifier.values()):
            raise ValidationError(
                f"Values of identifier dict must not contain any '{self.KEY_VALUE_DELIMITER}'.")

    def _check_for_record_type_in_dict(self, identifier: dict) -> None:
        """
        Raises ValidationError if keys or values of identifier dict contains record type.
        """
        for record_type in RecordTypes:
            if any(record_type.value in k for k in identifier.keys()):
                raise ValidationError(
                    f"Keys of identifier dict must not contain any '{record_type}'.")

            if any(record_type.value in v for v in identifier.values()):
                raise ValidationError(
                    f"Values of identifier dict must not contain any '{record_type}'.")


IdentifierType = NewType("IdentifierType", dict, field=IdentifierField)


@dataclass
class RequestRecord(BaseModel):
    identifier: IdentifierType
    time_constraint: datetime
    trace_id: UUID
    record_id: UUID

    record_type: RecordTypes = field(metadata=dict(load_only=True))


"""
Record generation
"""


def make_record(
    record_type: RecordTypes,
    bound_context: Type[BoundContext],
    trace_context: Type[TracingContext]
) -> dict:
    """
    Create a record item
    """
    if (not trace_context or
        trace_context.trace_id is None or
            trace_context.record_id is None):
        raise InvalidContextException(
            "Cannot create record without tracing context."
            "Trace ID and Record ID are required.")

    time_constraint = None
    if (isinstance(bound_context, OutboundContext) and
            record_type == RecordTypes.OUTBOUND):
        time_constraint = bound_context.invoked_at
    elif (isinstance(bound_context, InboundContext) and
          record_type == RecordTypes.INBOUND):
        time_constraint = bound_context.triggered_at

    if time_constraint is None:
        raise InvalidContextException(
            "Cannot create record without time contraint.")

    record = RequestRecord(
        record_type=record_type,
        identifier=bound_context.identifier,
        time_constraint=time_constraint,
        trace_id=trace_context.trace_id,
        record_id=trace_context.record_id)

    try:
        return record.dump()
    except ValidationError as err:
        raise InvalidContextException(
            f"Creating outbound record failed: {err}")


"""
Requests Table Interface
"""


class RequestTable(ABC, Loggable):
    """
    Base class for a request table.
    """

    INBOUND_KEY = "INBOUND"
    OUTBOUND_KEY = "OUTBOUND"
    RESOLVED_KEY = "RESOLVED"

    @classmethod
    def factory(cls, provider: Provider):
        if provider == Provider.AWS:
            return AWSRequestTable
        else:
            return NoopRequestTable

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @abstractmethod
    def record_outbound_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        """
        Stores a outbound request
        """
        pass

    @abstractmethod
    def record_inbound_request(
        self,
        inbound_context: Type[InboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        """
        Stores a inbound request
        """
        pass

    @abstractmethod
    def find_tracing_context_by_inbound_context(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        """
        Finds tracing context by inbound context
        """
        pass

    @abstractmethod
    def find_tracing_context_by_outbound_request(
        self,
        outbound_context: Type[OutboundContext]
    ) -> Type[TracingContext]:
        """
        Finds tracing context by outbound context
        """
        pass


"""
Noop Request Table
"""


class NoopRequestTable(RequestTable):
    """
    Dummy Request Table for unresolved providers.
    """

    def store_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        self.logger.warn(
            "Skipping recording outbound request. No outbound request table defined.")

    def find_request(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        pass


"""
Requests Table for AWS
"""


class AWSRequestTable(RequestTable):
    """
    Represents a dynamoDB backed table for recording requests in AWS
    """

    def __init__(self, table_name: str, region_name: str) -> None:
        """
        Initializes a new request table for AWS based on table name and region name.
        """
        super().__init__()

        self.table_name = table_name
        self.region_name = region_name

        if self.table_name is None or self.region_name is None:
            raise ValueError(
                "Cannot initialize Outbound Request Table for AWS. Table name or region name is missing.")

        self.dynamodb = boto3.client('dynamodb', region_name=self.region_name)
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()

    def record_outbound_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> bool:
        """
        Records a new outbound request in dynamodb.
        """
        record = make_record(
            RecordTypes.OUTBOUND,
            outbound_context,
            trace_context)
        item = self._serialize(record)
        try:
            self.dynamodb.put_item(TableName=self.table_name, Item=item)
        except ClientError as err:
            msg = f"Failed to record outbound request {record} in {self.table_name}: {err}"
            self.logger.error(msg)
            raise RequestTableError(msg)

        self.logger.info(
            f"Successfully recorded outbound request {record} in {self.table_name}")

    def record_inbound_request(
        self,
        inbound_context: Type[InboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        """
        Records a new inbound request in dynamodb.
        """
        record = make_record(
            RecordTypes.INBOUND,
            inbound_context,
            trace_context)
        item = self._serialize(record)
        try:
            self.dynamodb.put_item(TableName=self.table_name, Item=item)
        except ClientError as err:
            msg = f"Failed to record inbound request {record} in {self.table_name}: {err}"
            self.logger.error(msg)
            raise RequestTableError(msg)

        self.logger.info(
            f"Successfully recorded inbound request {record} in {self.table_name}")

    def find_tracing_context_by_outbound_request(
        self,
        outbound_context: Type[OutboundContext]
    ) -> Type[TracingContext]:
        """
        Finds the tracing context by given outbound context in dynamodb.
        """

        return super().find_tracing_context_by_outbound_request(outbound_context)

    def find_tracing_context_by_inbound_context(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        """
        Finds the tracing context for given inbound context in dynamodb.

        Checks for entries in DB, where identifier key is the same
        """
        return super().find_tracing_context_by_inbound_context(inbound_context)

    def find_request(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        """
        Finds a tracing context based on the inbound context.
        """
        # if not inbound_context.identifier:
        #     raise RuntimeError(
        #         f"Cannot find inbound record without identifier")

        # identifier_key = make_identifier_key(inbound_context.identifier)
        # response = self.dynamodb.query(
        #     TableName=self.table_name,
        #     KeyConditionExpression="identifier_key = :v1 AND invoked_at <= :v2",
        #     Limit=1,
        #     ScanIndexForward=False,
        #     ExpressionAttributeValues={
        #         ":v1": {
        #             "S": identifier_key},
        #         ":v2": {
        #             "S": str(
        #                 datetime.now().isoformat())},
        #     })

        # if "Items" in response and len(response["Items"]) > 0:
        #     if len(response["Items"]) != 1:
        #         raise RuntimeError(
        # f"Could not find unique inbound request for {identifier_key}")

        #     trace_data = {
        #         k: self.deserializer.deserialize(v) for k,
        #         v in response["Items"][0].items()}

        #     return TracingContext.load({
        #         "trace_id": trace_data.get("trace_id"),
        #         "record_id": trace_data.get("record_id")
        #     })

        # return None

    #
    #   Private methods
    #

    def _serialize(self, item: dict) -> dict:
        """
        Serializes record for AWS
        """
        return {
            k: self.serializer.serialize(v) for k,
            v in item.items() if v != ""}

    def _deserialize(self, item: dict) -> dict:
        """
        Deserializes record to dict.
        """
        return {k: self.deserializer.deserialize(v) for k, v in item.items()}
