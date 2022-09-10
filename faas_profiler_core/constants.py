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


class RecordDataType(Enum):
    """
    Enumeration of different record data types
    """
    UNCATEGORIZED = "uncategorized"
    SIMPLE_MEASUREMENT = "simple_measurement"
    PERIODIC_MEASUREMENT = "periodic_measurement"
    CAPTURE = "capture"


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
    INTERNAL = "internal"


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
Internal
"""


class InternalService(Service):
    """
    Enumeration of different internal services
    """
    UNIDENTIFIED = 'unidentified'
    IO = "IO"


class InternalOperation(Operation):
    """
    Enumeration of different internal operations
    """
    UNIDENTIFIED = 'unidentified'

    IO_READ = "io_read"
    IO_WRITE = "io_write"
    IO_READ_WRITE = "io_read_write"


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
    EVENTBRIDGE = "eventbridge"
    CLOUDWATCH = "cloudwatch"


class AWSOperation(Operation):
    """
    Enumeration of different AWS Operations
    """
    UNIDENTIFIED = 'unidentified'
    # Lambda
    LAMBDA_INVOKE = "invoke"
    LAMBDA_FUNCTION_URL = "FunctionUrl"

    # S3
    S3_OBJECT_CREATE = "ObjectCreated"  # Combines: PUT, POST, COPY
    S3_OBJECT_REMOVED = "ObjectRemoved"  # Combines: permanently and marked deleted
    S3_OBJECT_GET = "ObjectGet"
    S3_OBJECT_HEAD = "ObjectHead"
    S3_BUCKET_HEAD = "BucketHead"

    # Dynamo DB
    DYNAMO_DB_UPDATE = "Update"  # Combines: INSERT, MODIFY, DELETE

    # GATEWAY
    API_GATEWAY_AWS_PROXY = 'GatewayProxy'
    API_GATEWAY_HTTP = 'GatewayAPI'
    API_GATEWAY_AUTHORIZER = 'GatewayAuthorizer'

    # SQS
    SQS_RECEIVE = "ReceiveMessage"
    SQS_SEND = "SendMessage"
    SQS_SEND_BATCH = "SendMessageBatch"
    SQS_DELETE = "DeleteMessage"

    # SNS
    SNS_TOPIC_NOTIFICATION = "TopicNotification"

    # SES
    SES_EMAIL_RECEIVE = "ReceiveEmail"

    # Eventbridge
    EVENTBRIDGE_SCHEDULED_EVENT = "ScheduledEvent"

    # CloudWatch
    CLOUDWATCH_LOGS = "Logs"


class AWSRegion(Region):
    """
    Enumeration of different AWS Regions
    """
    EU_CENTRAL = "eu-central-1"


"""
Google Cloud Platform - Services, Operations and Regions
"""


class GCPService(Service):
    """
    Enumeration of different GCP Services
    """
    # Computing
    FUNCTIONS = "functions"
    CLOUD_RUN = "cloud_run"
    APP_ENGINE = "app_engine"

    # Storage
    STORAGE = "cloud_storage"
    FIRESTORE = "fire_store"

    # Messaging
    PUB_SUB = "pub_sub"


class GCPOperation(Operation):
    """
    Enumeration of different GCP Operations
    """
    FUNCTIONS_INVOKE = "invoke"


def service_proxy(provider: Provider) -> Service:
    if provider == Provider.AWS:
        return AWSService
    elif provider == Provider.GCP:
        return GCPService
    elif provider == Provider.INTERNAL:
        return InternalService
    else:
        return UnidentifiedService


def operation_proxy(provider: Provider) -> Operation:
    if provider == Provider.AWS:
        return AWSOperation
    elif provider == Provider.GCP:
        return GCPOperation
    elif provider == Provider.INTERNAL:
        return InternalOperation
    else:
        return UnidentifiedOperation
