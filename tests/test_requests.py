#!/usr/bin/env python3
# -*- coding: utf-8 -*

import pytest

from datetime import datetime, timedelta
from typing import Type
from uuid import UUID

from conftest import DYNAMODB_REQUESTS_TABLE_NAME

from faas_profiler_core.constants import AWSOperation, AWSService, Provider
from faas_profiler_core.models import InboundContext, OutboundContext, TracingContext
from faas_profiler_core.requests import ( 
    InvalidContextException,
    RequestTable,
    NoopRequestTable,
    AWSRequestTable,
    RecordType,
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
    invoked_at: datetime = datetime(2000, 1, 1, 12, 0, 59, 123456),
    identifier: dict = { "key_b": "value_b", "key_c": "value_c", "key_a": "value_a" }
) -> Type[InboundContext]:
    return InboundContext(
        provider=Provider.AWS,
        service=AWSService.S3,
        operation=AWSOperation.S3_OBJECT_CREATE,
        invoked_at=invoked_at,
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
    (create_outbound_context(), RecordType.OUTBOUND),
    (create_inbound_context(), RecordType.INBOUND),
])
def test_make_record_without_tracing_context(bound_context, record_type):
    tracing_context = TracingContext(None, None, None)

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)


@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(identifier={ "valid": "in#valid" }), RecordType.OUTBOUND),
    (create_inbound_context(identifier={ "valid": "invalid#" }), RecordType.INBOUND),
    (create_outbound_context(identifier={ "#invalid": "foo" }), RecordType.OUTBOUND),
    (create_inbound_context(identifier={ "#invalid": "foo" }), RecordType.INBOUND),
])
def test_make_record_with_delimiter_in_identifier(bound_context, record_type):
    tracing_context = create_tracing_context()

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)

@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(identifier={ "key": "INBOUND_foo" }), RecordType.OUTBOUND),
    (create_inbound_context(identifier={ "OUTBOUND_bar": "value" }), RecordType.INBOUND),
    (create_outbound_context(identifier={ "INBOUND": "INBOUNDOUTBOUND" }), RecordType.OUTBOUND),
    (create_inbound_context(identifier={ "OUTBOUND": "INBOUND" }), RecordType.INBOUND),
])
def test_make_record_with_record_type_in_identifier(bound_context, record_type):
    tracing_context = create_tracing_context()

    with pytest.raises(InvalidContextException):
        make_record(record_type, bound_context, tracing_context)

@pytest.mark.parametrize("bound_context, record_type", [
    (create_outbound_context(invoked_at=None), RecordType.OUTBOUND),
    (create_inbound_context(invoked_at=None), RecordType.INBOUND),
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

    record = make_record(RecordType.OUTBOUND, outbound_context, tracing_context)
    assert record == {
        "identifier": "OUTBOUND##foo_bar#bar_foo##id#1234##x_a#a_value",
        "record_id": record_id,
        "trace_id": trace_id,
        "time_constraint": "2000-01-01T12:00:59.123456"}

def test_make_record_for_inbound_request():
    trace_id = 'd90c4f58-c958-4a51-a6ed-c37219e0473b'
    record_id = 'a702a33b-facc-43e1-8c6e-e63a0748e7cb'
    tracing_context = create_tracing_context(UUID(trace_id), UUID(record_id))

    invoked_at = datetime(2000, 1, 1, 12, 59, 59, 123456)
    identifier = { "my_key": "my_value", 42: "x_value", "test_key": 42.42 }
    outbound_context = create_inbound_context(invoked_at, identifier)

    record = make_record(RecordType.INBOUND, outbound_context, tracing_context)
    assert record == {
        "identifier": "INBOUND##42#x_value##my_key#my_value##test_key#42.42",
        "record_id": record_id,
        "trace_id": trace_id,
        "time_constraint": "2000-01-01T12:59:59.123456"}

# """
# Testing AWS Request Table
# """

@pytest.fixture()
def aws_request_table(dynamodb_requests_table):
    request_table = AWSRequestTable(
        table_name=DYNAMODB_REQUESTS_TABLE_NAME,
        region_name='eu-central-1')

    yield request_table


def test_aws_request_table_without_region():
    with pytest.raises(ValueError):
        AWSRequestTable(
            table_name='fp-visualizer-requests-table-testing',
            region_name=None)

def test_aws_request_table_table_name():
    with pytest.raises(ValueError):
        AWSRequestTable(
            table_name=None,
            region_name='eu-central-1')


def test_record_outbound_request(aws_request_table):
    tracing_context = create_tracing_context()
    outbound_context = create_outbound_context()

    aws_request_table.record_outbound_request(outbound_context, tracing_context)


def test_record_inbound_request(aws_request_table):
    tracing_context = create_tracing_context()
    inbound_context = create_inbound_context()

    aws_request_table.record_inbound_request(inbound_context, tracing_context)


def test_find_tracing_context_by_outbound_request(aws_request_table):
    trace_id = 'd344e3fe-74f4-454a-9c69-85d7e0366f5a'
    record_id = '8bc1e409-d61f-4f3a-a0d8-ec1269e2ee40'
    tracing_context = create_tracing_context(trace_id, record_id)

    outbound_invoked_at = datetime(2000, 1, 1, 12, 0, 59, 123456)
    inbound_invoked_at = outbound_invoked_at + timedelta(microseconds=100)

    identifier = { "foo": "bar", "hello": "world" }

    outbound_context = create_outbound_context(identifier=identifier, invoked_at=outbound_invoked_at)
    inbound_context = create_inbound_context(identifier=identifier, invoked_at=inbound_invoked_at)

    aws_request_table.record_inbound_request(inbound_context, tracing_context)

    tracing_context = aws_request_table.find_tracing_context_by_outbound_request(outbound_context)
    
    assert str(tracing_context.trace_id) == trace_id
    assert str(tracing_context.record_id) == record_id


def test_find_tracing_context_by_inbound_request(aws_request_table):
    trace_id = '7b4a3af4-995c-472b-8daa-5d43cb3655a7'
    record_id = '03a252ae-76f9-40d2-a4e4-73193c661849'
    tracing_context = create_tracing_context(trace_id, record_id)

    outbound_invoked_at = datetime(2000, 1, 1, 12, 0, 59, 123456)
    inbound_invoked_at = outbound_invoked_at + timedelta(microseconds=420)

    identifier = { "x": "id_test", "a": "foo" }

    outbound_context = create_outbound_context(identifier=identifier, invoked_at=outbound_invoked_at)
    inbound_context = create_inbound_context(identifier=identifier, invoked_at=inbound_invoked_at)

    aws_request_table.record_outbound_request(outbound_context, tracing_context)
    tracing_context = aws_request_table.find_tracing_context_by_inbound_context(inbound_context)
    
    assert str(tracing_context.trace_id) == trace_id
    assert str(tracing_context.record_id) == record_id
