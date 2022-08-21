#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum

"""
Constants
"""

TRACE_ID_HEADER = "FaaS-Profiler-Trace-ID"
RECORD_ID_HEADER = "FaaS-Profiler-Record-ID"
PARENT_ID_HEADER = "FaaS-Profiler-Parent-ID"

TRACE_CONTEXT_KEY = "_faas_profiler_context"


"""
Enums
"""


class Runtime(Enum):
    """
    Enumeration of different runtimes.
    """
    UNIDENTIFIED = "unidentified"
    PYTHON = "python"
    NODE = "node"


class Provider(Enum):
    """
    Enumeration of different cloud providers.
    """
    UNIDENTIFIED = "unidentified"
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


class Region(Enum):
    """
    Base class for provider regions
    """


class Service(Enum):
    """
    Base class for provider services
    """


class Operation(Enum):
    """
    Base class for operations on provider services.
    """


class TriggerSynchronicity(Enum):
    """
    Enumeration of different trigger synchronicities
    """
    UNIDENTIFIED = 'unidentified'
    ASYNC = "async"
    SYNC = "sync"


"""
Base Service, Operation and Region
"""


class UnidentifiedService(Service):
    """
    Default service for unidentified services.
    """
    UNIDENTIFIED = 'unidentified'


class UnidentifiedOperation(Operation):
    """
    Default service for unidentified services.
    """
    UNIDENTIFIED = 'unidentified'


"""
Amazon Web Services - Services, Operations and Regions
"""


class AWSService(Service):
    """
    Enumeration of different AWS services
    """
    UNIDENTIFIED = 'unidentified'
    LAMBDA = "lambda"
    CLOUDFRONT = 'cloudfront'
    DYNAMO_DB = 'dynamodb'
    CLOUD_FORMATION = 'cloud_formation'
    SNS = 'sns'
    SES = 'ses'
    SQS = 'sqs'
    S3 = "s3"
    CODE_COMMIT = 'code_commit'
    AWS_CONFIG = 'aws_config'
    KINESIS = 'kinesis'
    API_GATEWAY = 'api_gateway'


class AWSOperation(Operation):
    """
    Enumeration of different AWS Operations
    """
    UNIDENTIFIED = 'unidentified'
    # Lambda
    LAMBDA_INVOKE = "invoke"

    # S3
    S3_OBJECT_CREATE = "ObjectCreated"  # Combines: PUT, POST, COPY
    S3_OBJECT_REMOVED = "ObjectRemoved"  # Combines: permanently and marked deleted

    # Dynamo DB
    DYNAMO_DB_UPDATE = "Update"  # Combines: INSERT, MODIFY, DELETE

    # GATEWAY
    API_GATEWAY_AWS_PROXY = 'GatewayProxy'
    API_GATEWAY_HTTP = 'GatewayAPI'
    API_GATEWAY_AUTHORIZER = 'GatewayAuthorizer'

    # SQS
    SQS_RECEIVE = "ReceiveMessage"

    # SNS
    SNS_TOPIC_NOTIFICATION = "TopicNotification"

    # SES
    SES_EMAIL_RECEIVE = "ReceiveEmail"


class AWSRegion(Region):
    """
    Enumeration of different AWS Regions
    """
    EU_CENTRAL = "eu-central-1"


def service_proxy(provider: Provider) -> Service:
    if provider == provider.AWS:
        return AWSService
    else:
        return UnidentifiedService


def operation_proxy(provider: Provider) -> Operation:
    if provider == provider.AWS:
        return AWSOperation
    else:
        return UnidentifiedOperation
