#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inbound and Outbound recording
"""
from __future__ import annotations

import boto3

from typing import Optional, Type
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from uuid import UUID

from marshmallow import fields, ValidationError
from marshmallow_dataclass import NewType
from dataclasses import dataclass, field
from enum import Enum

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError

from .logging import Loggable
from .models import BaseModel, RequestContext, InboundContext, TracingContext, OutboundContext
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

KEY_VALUE_DELIMITER = "#"
IDENTIFIER_DELIMITER = 2 * KEY_VALUE_DELIMITER


class RecordType(Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


def make_identifier_string(record_type: RecordType, identifier: dict) -> str:
    """
    Returns the identifier dict as string.
    """
    identifier = {str(k): str(v) for k, v in identifier.items()}
    identifier = sorted(
        identifier.items(),
        key=lambda x: x[0],
        reverse=False)
    identifier = map(
        lambda id: KEY_VALUE_DELIMITER.join(id),
        identifier)

    return record_type.value + IDENTIFIER_DELIMITER + \
        IDENTIFIER_DELIMITER.join(identifier)


class IdentifierField(fields.Field):
    """
    Identifier Field.

    On serialize it creates a string out of the dict
    On deserialize it splits the string back to a dict.
    """

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
        if record_type is None or record_type not in RecordType:
            raise ValidationError(
                f"Record type {record_type} is invalid, valid are {list(RecordType)}")

        return make_identifier_string(record_type, identifier)

    def _deserialize(self, value, attr, data, **kwargs):
        """
        Transforms the identifier string to identifier dict.
        """
        identifier = str(value).split(IDENTIFIER_DELIMITER)
        if len(identifier) == 0:
            return {}

        data["record_type"] = identifier[0]

        identifier = map(lambda id_str: id_str.split(
            KEY_VALUE_DELIMITER), identifier[1:])
        try:
            return {i[0]: i[1] for i in identifier}
        except IndexError:
            raise ValidationError(f"Identifier string {value} is malformed.")

    def _check_for_delimiter_in_dict(self, identifier: dict) -> None:
        """
        Raises ValidationError if keys or values of identifier dict contains the delimiter symbol.
        """
        if any(KEY_VALUE_DELIMITER in k for k in identifier.keys()):
            raise ValidationError(
                f"Keys of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

        if any(KEY_VALUE_DELIMITER in v for v in identifier.values()):
            raise ValidationError(
                f"Values of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

    def _check_for_record_type_in_dict(self, identifier: dict) -> None:
        """
        Raises ValidationError if keys or values of identifier dict contains record type.
        """
        for record_type in RecordType:
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

    record_type: Optional[RecordType] = field(metadata=dict(load_only=True))


"""
Record generation
"""


def make_record(
    record_type: RecordType,
    request_context: Type[RequestContext],
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

    time_constraint = request_context.invoked_at
    if time_constraint is None:
        raise InvalidContextException(
            "Cannot create record without time contraint.")

    record = RequestRecord(
        record_type=record_type,
        identifier=request_context.identifier,
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

    PARTITION_KEY = "identifier"
    SORT_KEY = "time_constraint"

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

    def record_inbound_request(
        self,
        inbound_context: Type[InboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        self.logger.warn(
            "Skipping recording inbound request. No request table defined.")

    def record_outbound_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        self.logger.warn(
            "Skipping recording outbound request. No request table defined.")

    def find_tracing_context_by_inbound_context(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        return super().find_tracing_context_by_inbound_context(inbound_context)

    def find_tracing_context_by_outbound_request(
        self,
        outbound_context: Type[OutboundContext]
    ) -> Type[TracingContext]:
        return super().find_tracing_context_by_outbound_request(outbound_context)


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
            RecordType.OUTBOUND,
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
            RecordType.INBOUND,
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
        identifer = make_identifier_string(
            RecordType.INBOUND, outbound_context.identifier)
        invoked_at = outbound_context.invoked_at - timedelta(microseconds=1000)
        try:
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression=":part_key = :id  AND :sort_key >= :tc",
                ExpressionAttributeNames={
                    ":part_key": self.PARTITION_KEY,
                    ":sort_key": self.SORT_KEY},
                ExpressionAttributeValues={
                    ":id": {"S": identifer},
                    ":tc": {"S": invoked_at.isoformat()}})
            try:
                item = self._deserialize(response.get("Items", [])[0])
            except IndexError:
                raise RequestTableError(
                    f"No inboud record for identifer string {identifer} found.")
            try:
                request_record = RequestRecord.load(item)
                return TracingContext(
                    trace_id=request_record.trace_id,
                    record_id=request_record.record_id)
            except ValidationError as err:
                raise RequestTableError(
                    f"Deserializing record failed: {err}")
        except ClientError as err:
            raise RequestTableError(
                f"Querying table {self.table_name} failed: {err}")

    def find_tracing_context_by_inbound_context(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        """
        Finds the tracing context for given inbound context in dynamodb.
        """
        identifer = make_identifier_string(
            RecordType.OUTBOUND, inbound_context.identifier)
        invoked_at = inbound_context.invoked_at + timedelta(microseconds=1000)
        try:
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression=":part_key = :id  AND :sort_key <= :tc",
                ExpressionAttributeNames={
                    ":part_key": self.PARTITION_KEY,
                    ":sort_key": self.SORT_KEY},
                ExpressionAttributeValues={
                    ":id": {"S": identifer},
                    ":tc": {"S": invoked_at.isoformat()}})
            try:
                item = self._deserialize(response.get("Items", [])[0])
            except IndexError:
                raise RequestTableError(
                    f"No inboud record for identifer string {identifer} found.")
            try:
                request_record = RequestRecord.load(item)
                return TracingContext(
                    trace_id=request_record.trace_id,
                    record_id=request_record.record_id)
            except ValidationError as err:
                raise RequestTableError(
                    f"Deserializing record failed: {err}")
        except ClientError as err:
            raise RequestTableError(
                f"Querying table {self.table_name} failed: {err}")

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
