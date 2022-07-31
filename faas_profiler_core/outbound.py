#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Outbound requests handleing.
"""

import boto3

from typing import Type
from abc import ABC, abstractmethod
from uuid import uuid4
from datetime import datetime

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError

from .logging import Loggable
from .models import InboundContext, TracingContext, OutboundContext
from .constants import Provider

def make_identifier_key(
    identifier: dict,
    key_value_delimiter: str = "#",
    identifier_delimiter: str = "##"
) -> str:
    """
    Makes a identifier key.
    Sorts the keys descending and joins the key values with #.
    """
    identifier = {str(k): str(v) for k,v in identifier.items()}
    identifier = sorted(identifier.items(), key=lambda x: x[1], reverse=False)
    identifier = map(lambda id: key_value_delimiter.join(id), identifier)

    return identifier_delimiter.join(identifier)

class OutboundRequestTable(ABC, Loggable):
    """
    Base class for a outbound request table.
    """

    @classmethod
    def factory(cls, provider: Provider):
        if provider == Provider.AWS:
            return AWSOutboundRequestTable
        else:
            return NoopOutboundRequestTable

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @abstractmethod
    def store_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        pass

    
    @abstractmethod
    def find_request(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        pass

class NoopOutboundRequestTable(OutboundRequestTable):
    """
    Dummy Outbound Request Table for unresolved providers.
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

class AWSOutboundRequestTable(OutboundRequestTable):
    """
    Represents a dynamoDB backed table for recording outbounding requests in AWS
    """

    def __init__(self, table_name: str, region_name: str) -> None:
        super().__init__()

        self.table_name = table_name
        self.region_name = region_name

        if self.table_name is None or self.region_name is None:
            raise RuntimeError(
                "Cannot initialize Outbound Request Table for AWS. Table name or region name is missing.")

        self.dynamodb = boto3.client('dynamodb', region_name=self.region_name)
        self.serializer = TypeSerializer()

    def store_request(
        self,
        outbound_context: Type[OutboundContext],
        trace_context: Type[TracingContext]
    ) -> None:
        """
        Stores the invocation the dynamodb table
        """
        if not outbound_context.identifier:
            raise RuntimeError(
                f"Cannot find inbound record without identifier")

        identifier_key = make_identifier_key(outbound_context.identifier)
        record = {
            "identifier_key": identifier_key,
            "invoked_at": outbound_context.invoked_at.isoformat(),
            "trace_id": str(trace_context.trace_id),
            "record_id": str(trace_context.invocation_id)
        }
        item = {
            k: self.serializer.serialize(v) for k,
            v in record.items() if v != ""}
        try:
            self.dynamodb.put_item(TableName=self.table_name, Item=item)
        except ClientError as err:
            self.logger.info(
                f"Failed to record outbound request {identifier_key} in {self.table_name}: {err}")
        else:
            self.logger.info(
                f"Successfully recorded outbound request {identifier_key} in {self.table_name}")


    def find_request(
        self,
        inbound_context: Type[InboundContext]
    ) -> Type[TracingContext]:
        """
        Finds a tracing context based on the inbound context.
        """
        if not inbound_context.identifier:
            raise RuntimeError(
                f"Cannot find inbound record without identifier")
        
        identifier_key = make_identifier_key(inbound_context.identifier)
        response = self.dynamodb.query(
            TableName=self.table_name,
            KeyConditionExpression="identifier_key = :v1 AND invoked_at <= :v2",
            ExpressionAttributeValues={
                ":v1": {"S": identifier_key},
                ":v2": {"S": str(datetime.now().isoformat())},
            })

        print(response.get("Items"))

        # breakpoint()