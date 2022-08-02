#!/usr/bin/env python3
# -*- coding: utf-8 -*

import pytest
import os
import boto3

from moto import mock_dynamodb

DYNAMODB_REQUESTS_TABLE_NAME = 'fp-visualizer-requests-table-testing'

@pytest.fixture()
def aws_credentials():
    """
    Mocked AWS Credentials for moto.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture()
def dynamodb(aws_credentials):
    """
    Mocking dynamodb resource
    """
    with mock_dynamodb():
        yield boto3.resource('dynamodb', region_name='eu-central-1')


@pytest.fixture()
def dynamodb_requests_table(dynamodb):
    """
    Create requests mock table
    """
    table = dynamodb.create_table(
        TableName=DYNAMODB_REQUESTS_TABLE_NAME,
        KeySchema=[
            {
                'AttributeName': 'identifier',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'time_constraint',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'identifier',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'time_constraint',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=DYNAMODB_REQUESTS_TABLE_NAME)

    yield
