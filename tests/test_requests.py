#!/usr/bin/env python3
# -*- coding: utf-8 -*

from datetime import datetime
import os
from typing import Type
from uuid import UUID, uuid4
import pytest
import boto3

from moto import mock_dynamodb

from faas_profiler_core.constants import AWSOperation, AWSService, Provider
from faas_profiler_core.models import InboundContext, OutboundContext, TracingContext
from faas_profiler_core.requests import ( 
    InvalidContextException,
    RequestTable,
    NoopRequestTable,
    AWSRequestTable,
    RecordTypes,
    make_record
)

"""
Fixtures
"""

def create_outbound_context(
    invoked_at: datetime = datetime(2000, 1, 1, 12, 0, 59, 123456),
    identifier: dict = { "key_b": "value_b", "key_c": "value_c", "key_a": "value_a" }
) -> Type[OutboundContext]:
    return OutboundContext(
        provider=Provider.AWS,
        service=AWSService.SES,
        operation=AWSOperation.SES_EMAIL_RECEIVE,
        invoked_at=invoked_at,
        identifier=identifier)


def create_inbound_context(
    triggered_at: datetime = datetime(2000, 1, 1, 12, 0, 59, 123456),
    identifier: dict = { "key_b": "value_b", "key_c": "value_c", "key_a": "value_a" }
) -> Type[InboundContext]:
    return InboundContext(
        provider=Provider.AWS,
        service=AWSService.S3,
        operation=AWSOperation.S3_OBJECT_CREATE,
        triggered_at=triggered_at,
        identifier=identifier)

def create_tracing_context(
    trace_id: UUID = UUID("3a10d40a-3406-443f-8bc7-2f64ccdf7592"),
    record_id: UUID = UUID("0426ec94-c5e6-4907-bbc3-19bc0bdf4093"),
    parent_id: UUID = None
):
   return TracingContext(trace_id, record_id, parent_id)

"""
Testing Factory
"""

def test_aws_factory():
    assert RequestTable.factory(Provider.AWS) == AWSRequestTable

def test_noop_factory():
    assert RequestTable.factory(Provider.UNIDENTIFIED) == NoopRequestTable


"""
Testing record creation
"""

@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(), RecordTypes.OUTBOUND),
    (create_inbound_context(), RecordTypes.INBOUND),
])
def test_make_record_without_tracing_context(bound_context, record_type):
    tracing_context = TracingContext(None, None, None)

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)


@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(identifier={ "valid": "in#valid" }), RecordTypes.OUTBOUND),
    (create_inbound_context(identifier={ "valid": "invalid#" }), RecordTypes.INBOUND),
    (create_outbound_context(identifier={ "#invalid": "foo" }), RecordTypes.OUTBOUND),
    (create_inbound_context(identifier={ "#invalid": "foo" }), RecordTypes.INBOUND),
])
def test_make_record_with_delimiter_in_identifier(bound_context, record_type):
    tracing_context = create_tracing_context()

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)

@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(identifier={ "key": "INBOUND_foo" }), RecordTypes.OUTBOUND),
    (create_inbound_context(identifier={ "OUTBOUND_bar": "value" }), RecordTypes.INBOUND),
    (create_outbound_context(identifier={ "INBOUND": "INBOUNDOUTBOUND" }), RecordTypes.OUTBOUND),
    (create_inbound_context(identifier={ "OUTBOUND": "INBOUND" }), RecordTypes.INBOUND),
])
def test_make_record_with_record_type_in_identifier(bound_context, record_type):
    tracing_context = create_tracing_context()

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)

@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(invoked_at=None), RecordTypes.OUTBOUND),
    (create_inbound_context(triggered_at=None), RecordTypes.INBOUND),
])
def test_make_record_without_time_constraint(bound_context, record_type):
    tracing_context = create_tracing_context()

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)


def test_make_record_for_outbound_request():
    trace_id = 'd344e3fe-74f4-454a-9c69-85d7e0366f5a'
    record_id = '8bc1e409-d61f-4f3a-a0d8-ec1269e2ee40'
    tracing_context = create_tracing_context(UUID(trace_id), UUID(record_id))

    invoked_at = datetime(2000, 1, 1, 12, 0, 59, 123456)
    identifier = { "x_a": "a_value", "foo_bar": "bar_foo", "id": 1234 }
    outbound_context = create_outbound_context(invoked_at, identifier)

    record = make_record(RecordTypes.OUTBOUND, outbound_context, tracing_context)
    assert record == {
        "identifier": "OUTBOUND##foo_bar#bar_foo##id#1234##x_a#a_value",
        "record_id": record_id,
        "trace_id": trace_id,
        "time_constraint": "2000-01-01T12:00:59.123456"}

def test_make_record_for_inbound_request():
    trace_id = 'd90c4f58-c958-4a51-a6ed-c37219e0473b'
    record_id = 'a702a33b-facc-43e1-8c6e-e63a0748e7cb'
    tracing_context = create_tracing_context(UUID(trace_id), UUID(record_id))

    triggered_at = datetime(2000, 1, 1, 12, 59, 59, 123456)
    identifier = { "my_key": "my_value", 42: "x_value", "test_key": 42.42 }
    outbound_context = create_inbound_context(triggered_at, identifier)

    record = make_record(RecordTypes.INBOUND, outbound_context, tracing_context)
    assert record == {
        "identifier": "INBOUND##42#x_value##my_key#my_value##test_key#42.42",
        "record_id": record_id,
        "trace_id": trace_id,
        "time_constraint": "2000-01-01T12:59:59.123456"}

# """
# Testing AWS Request Table
# """

# @pytest.fixture(scope='function')
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
#     os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
#     os.environ['AWS_SECURITY_TOKEN'] = 'testing'
#     os.environ['AWS_SESSION_TOKEN'] = 'testing'


# @pytest.fixture(scope='function')
# def dynamodb(aws_credentials):
#     with mock_dynamodb():
#         yield boto3.resource('dynamodb', region_name='eu-central-1')


# @pytest.fixture(scope='function')
# def dynamodb_table(dynamodb):
#     table = dynamodb.create_table(
#         TableName='fp-visualizer-requests-table-testing',
#         KeySchema=[
#             {
#                 'AttributeName': 'identifier_key',
#                 'KeyType': 'HASH'
#             },
#             {
#                 'AttributeName': 'invoked_at',
#                 'KeyType': 'RANGE'
#             }
#         ],
#         AttributeDefinitions=[
#             {
#                 'AttributeName': 'identifier_key',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'invoked_at',
#                 'AttributeType': 'S'
#             }
#         ],
#         ProvisionedThroughput={
#             'ReadCapacityUnits': 1,
#             'WriteCapacityUnits': 1
#         }
#     )
#     table.meta.client.get_waiter('table_exists').wait(TableName='fp-visualizer-requests-table-testing')

#     yield


# @pytest.fixture(scope='function')
# def aws_request_table(dynamodb_table):
#     request_table = AWSRequestTable(
#         table_name='fp-visualizer-requests-table-testing',
#         region_name='eu-central-1')

#     yield request_table


# def test_aws_request_table_without_region():
#     with pytest.raises(ValueError):
#         AWSRequestTable(
#             table_name='fp-visualizer-requests-table-testing',
#             region_name=None)

# def test_aws_request_table_table_name():
#     with pytest.raises(ValueError):
#         AWSRequestTable(
#             table_name=None,
#             region_name='eu-central-1')

# def test_record_outbound_request_without_identifier(aws_request_table):
#     outbound_request = OutboundContext(
#         provider=Provider.AWS,
#         service=AWSService.SES,
#         operation=AWSOperation.SES_EMAIL_RECEIVE,
#         identifier={})

#     tracing_context = TracingContext(trace_id=uuid4(), record_id=uuid4(), parent_id=None)

#     with pytest.raises(InvalidContextException):
#         aws_request_table.record_outbound_request(outbound_request, tracing_context)

# def test_record_outbound_request_without_invoked_at(aws_request_table):
#     outbound_request = OutboundContext(
#         provider=Provider.AWS,
#         service=AWSService.SES,
#         operation=AWSOperation.SES_EMAIL_RECEIVE,
#         identifier={ "key_a": "value", "request_id": 1234, "foo": "bar" })

#     tracing_context = TracingContext(trace_id=uuid4(), record_id=uuid4(), parent_id=None)

#     with pytest.raises(InvalidContextException):
#         aws_request_table.record_outbound_request(outbound_request, tracing_context)


# def test_record_outbound_request(aws_request_table):
#     outbound_request = OutboundContext(
#         provider=Provider.AWS,
#         service=AWSService.SES,
#         operation=AWSOperation.SES_EMAIL_RECEIVE,
#         invoked_at=datetime.now(),
#         identifier={ "key_a": "value", "request_id": 1234, "foo": "bar" })

#     tracing_context = TracingContext(trace_id=uuid4(), record_id=uuid4(), parent_id=None)

#     aws_request_table.record_outbound_request(outbound_request, tracing_context)


