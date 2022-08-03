#!/usr/bin/env python3
# -*- coding: utf-8 -*

from uuid import UUID
import marshmallow
import pytest

from faas_profiler_core.constants import AWSOperation, AWSService, Provider, Runtime

from faas_profiler_core.models import (
    FunctionContext,
    RequestContext,
    TracingContext
)

"""
Testing Function Context
"""


@pytest.mark.parametrize("data", [
    {"provider": "", "runtime": "python", "function_name": "foo-bar", "handler": "foo.bar"},
    {"provider": "aws", "runtime": "", "function_name": "foo-bar", "handler": "foo.bar"},
    {"provider": "aws", "runtime": "python", "function_name": "", "handler": "foo.bar"},
    {"provider": "aws", "runtime": "python", "function_name": "foo-bar", "handler": ""}
])
def test_load_with_missing_data(data):
    with pytest.raises(marshmallow.ValidationError):
        FunctionContext.load(data)


def test_load_with_aws_context():
    func_ctx = FunctionContext.load({
        "provider": "aws",
        "runtime": "python",
        "function_name": "my-sample-function",
        "handler": "function.handler"})

    assert func_ctx.provider == Provider.AWS
    assert func_ctx.runtime == Runtime.PYTHON
    assert func_ctx.function_name == "my-sample-function"
    assert func_ctx.handler == "function.handler"

def test_load_with_azure_context():
    func_ctx = FunctionContext.load({
        "provider": "azure",
        "runtime": "node",
        "function_name": "my-azure-function",
        "handler": "function.handler"})

    assert func_ctx.provider == Provider.AZURE
    assert func_ctx.runtime == Runtime.NODE
    assert func_ctx.function_name == "my-azure-function"
    assert func_ctx.handler == "function.handler"

def test_load_with_gcp_context():
    func_ctx = FunctionContext.load({
        "provider": "gcp",
        "runtime": "node",
        "function_name": "my-gcp-function",
        "handler": "main"})

    assert func_ctx.provider == Provider.GCP
    assert func_ctx.runtime == Runtime.NODE
    assert func_ctx.function_name == "my-gcp-function"
    assert func_ctx.handler == "main"


"""
Testing Tracing Context
"""


@pytest.mark.parametrize("tracing_data", [
    {
        "trace_id": "d5769042-8201-40f3-b972-691fd201829c",
        "record_id": "3d116d45-7de3-4da4-a5be-4fbe3620f844",
        "parent_id": "49305021-79b1-4d64-8ec4-4c153947e4ca"
    },
    {
        "trace_id": "e496ef66-39c0-43fc-bbd8-f5fac5adfcfc",
        "record_id": "9db8926d-056b-4733-a55f-1a43dfcb2eca",
        "parent_id": None
    },
])
def test_load_tracing_context(tracing_data):
    tracing_context = TracingContext.load(tracing_data)

    assert tracing_context.trace_id == UUID(tracing_data["trace_id"])
    assert tracing_context.record_id == UUID(tracing_data["record_id"])

    if tracing_data["parent_id"]:    
        assert tracing_context.parent_id == UUID(tracing_data["parent_id"])

def test_tracing_context_to_injectable_with_parent():
    tracing_context = TracingContext.load({
        "trace_id": "d5769042-8201-40f3-b972-691fd201829c",
        "record_id": "3d116d45-7de3-4da4-a5be-4fbe3620f844",
        "parent_id": "49305021-79b1-4d64-8ec4-4c153947e4ca"
    })
    injectable_tracing_data = tracing_context.to_injectable()

    assert injectable_tracing_data == {
        'FaaS-Profiler-Trace-ID': 'd5769042-8201-40f3-b972-691fd201829c',
        'FaaS-Profiler-Record-ID': '3d116d45-7de3-4da4-a5be-4fbe3620f844',
        'FaaS-Profiler-Parent-ID': '49305021-79b1-4d64-8ec4-4c153947e4ca'
    }

def test_tracing_context_to_injectable_without_parent():
    tracing_context = TracingContext.load({
        "trace_id": "d5769042-8201-40f3-b972-691fd201829c",
        "record_id": "3d116d45-7de3-4da4-a5be-4fbe3620f844",
    })
    injectable_tracing_data = tracing_context.to_injectable()

    assert injectable_tracing_data == {
        'FaaS-Profiler-Trace-ID': 'd5769042-8201-40f3-b972-691fd201829c',
        'FaaS-Profiler-Record-ID': '3d116d45-7de3-4da4-a5be-4fbe3620f844',
    }


"""
Testing Request Context
"""

@pytest.mark.parametrize("data,expected_service,expected_operation", [
    ({"provider": "aws", "service": "s3", "operation": "ObjectCreated"}, AWSService.S3, AWSOperation.S3_OBJECT_CREATE),
    ({"provider": "aws", "service": "sqs", "operation": "ReceiveMessage"}, AWSService.SQS, AWSOperation.SQS_RECEIVE),
    ({"provider": "aws", "service": "sns", "operation": "TopicNotification"}, AWSService.SNS, AWSOperation.SNS_TOPIC_NOTIFICATION),
    ({"provider": "aws", "service": "dynamodb", "operation": "Update"}, AWSService.DYNAMO_DB, AWSOperation.DYNAMO_DB_UPDATE),
])
def test_load_aws_request(data, expected_service, expected_operation):
    request_ctx = RequestContext.load(data)

    assert request_ctx.provider == Provider.AWS
    assert request_ctx.service == expected_service
    assert request_ctx.operation == expected_operation