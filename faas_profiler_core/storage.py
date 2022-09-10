#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Storage interface
"""
from __future__ import annotations

import json

import boto3
from google.cloud import storage

from abc import ABC, abstractproperty, abstractmethod
from typing import Type
from botocore.exceptions import ClientError
from marshmallow import ValidationError
from functools import cached_property
from uuid import UUID
from os.path import basename, splitext

from faas_profiler_core.logging import Loggable
from faas_profiler_core.models import Profile, Trace, TraceRecord


class RecordStorageError(RuntimeError):
    pass


def safe_json_serialize(obj: dict) -> str:
    """
    Safe serialize JSON
    """
    default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"
    return json.dumps(
        obj,
        ensure_ascii=False,
        indent=None,
        default=default
    ).encode('utf-8')

class RecordStorage(ABC, Loggable):
    """
    Base class for all storage abstraction.
    Used this class to interact with the record storage.
    """

    PROFILES_PREFIX = "profiles/"
    PROFILES_FORMAT = PROFILES_PREFIX + "{profile_id}.json"

    TRACES_PREFIX = "traces/"
    TRACE_FORMAT = TRACES_PREFIX + "{trace_id}.json"

    UNPROCESSED_RECORDS_PREFIX = "unprocessed_records/"
    UNPROCESSED_RECORDS_FORMAT = UNPROCESSED_RECORDS_PREFIX + \
        "{record_id}.json"

    PROCESSED_RECORDS_PREFIX = "records/"
    PROCESSED_RECORDS_FORMAT = PROCESSED_RECORDS_PREFIX + "{record_id}.json"

    def __init__(self):
        super().__init__()

    """
    Profile methods
    """

    @abstractproperty
    def profile_ids(self) -> list[UUID]:
        """
        Returns a list of all profile IDs
        """
        pass

    @abstractproperty
    def number_of_profiles(self) -> int:
        """
        Returns the number of recorded profiles.
        """
        pass

    @property
    def has_profiles(self) -> bool:
        """
        Returns True if traces exists
        """
        return self.number_of_profiles > 0

    @abstractmethod
    def get_profile(self, profile_id: str) -> Type[Profile]:
        """
        Get a single profile.
        """
        pass

    @abstractmethod
    def store_profile(self, profile: Type[Profile]) -> None:
        """
        Stores a new profile.
        """
        pass

    """
    Trace methods
    """

    @abstractmethod
    def get_trace(self, trace_id: UUID) -> Type[Trace]:
        """
        Gets a single trace.
        """
        pass

    @abstractmethod
    def store_trace(self, trace: Type[Trace]):
        """
        Store a new trace
        """
        pass

    """
    Record methods
    """

    @abstractproperty
    def number_of_unprocessed_records(self) -> int:
        """
        Returns the number of unprocessed records.
        """
        pass

    @property
    def has_unprocessed_records(self) -> bool:
        """
        Returns True if unprocessed records exists.
        """
        return self.number_of_unprocessed_records > 0

    @abstractmethod
    def store_unprocessed_record(self, record: Type[TraceRecord]) -> None:
        """
        Stores a new unprocessed record
        """
        pass

    @abstractmethod
    def unprocessed_records(self):
        """
        Generator for all unprocessed records.
        """
        pass

    @abstractmethod
    def mark_record_as_resolved(self, record_id: str):
        """
        Marks a record as resolved
        """
        pass


class S3RecordStorage(RecordStorage):
    """
    Storage implementation for AWS S3.
    """

    def __init__(self, bucket_name: str, region_name: str) -> None:
        super().__init__()

        self.bucket_name = bucket_name
        self.region_name = region_name
        self.client = boto3.client('s3', region_name=self.region_name)

    @cached_property
    def profile_ids(self) -> list[UUID]:
        """
        Returns a list of all profile IDs
        """
        all_profile_keys = self._list_objects_with_paginator(
            prefix=self.PROFILES_PREFIX)

        if all_profile_keys is None or len(all_profile_keys) == 0:
            return []

        profile_ids = []
        all_profile_keys = sorted(
            all_profile_keys, key=lambda x: x["LastModified"], reverse=False)

        for profile_key in all_profile_keys:
            try:
                base = basename(profile_key.get("Key", ""))
                profile_ids.append(
                    UUID(splitext(base)[0]))
            except Exception as err:
                self.logger.error(
                    f"Failed to load profile ID: {err}")

        return profile_ids

    @property
    def number_of_profiles(self) -> int:
        """
        Returns the number of recorded profiles.
        """
        return len(self.profile_ids)

    def get_profile(self, profile_id: UUID):
        """
        Get a single profile.
        """
        _key = self.PROFILES_FORMAT.format(profile_id=str(profile_id))
        try:
            obj = self.client.get_object(Bucket=self.bucket_name, Key=_key)
        except ClientError as err:
            raise RecordStorageError(
                f"Failed to get object {_key}: {err}")

        if "Body" in obj:
            body = json.loads(obj["Body"].read().decode('utf-8'))
            try:
                return Profile.load(body)
            except ValidationError as err:
                raise RecordStorageError(
                    f"Failed to deserialize {body}: {err}")

    @cached_property
    def unprocessed_record_keys(self) -> list[str]:
        """
        Returns a list of unprocessed records sorted by last modified
        """
        record_keys = self._list_objects_with_paginator(
            prefix=self.UNPROCESSED_RECORDS_PREFIX)

        return [
            obj["Key"] for obj in sorted(
                record_keys,
                key=lambda x: x["LastModified"],
                reverse=False)]

    @property
    def number_of_unprocessed_records(self) -> int:
        """
        Returns the number of unprocessed records.
        """
        return len(self.unprocessed_record_keys)

    def unprocessed_records(self):
        """
        Generator for all unprocessed records.
        """
        for key in self.unprocessed_record_keys:
            try:
                obj = self.client.get_object(Bucket=self.bucket_name, Key=key)
            except ClientError as err:
                self.logger.error(
                    f"Failed to get object {key}: {err}")
                continue

            if "Body" in obj:
                body = json.loads(obj["Body"].read().decode('utf-8'))
                try:
                    yield TraceRecord.load(body)
                except ValidationError as err:
                    self.logger.error(
                        f"Failed to deserialize {body}: {err}")
                    continue

    def store_unprocessed_record(self, record: Type[TraceRecord]) -> None:
        """
        Stores a new unprocessed record
        """
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=record.record_id)
        record_json = safe_json_serialize(record.dump())

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=_record_key,
            Body=record_json)

    def mark_record_as_resolved(self, record_id: str):
        """
        Marks a record as resolved
        """
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=record_id)
        if _record_key in self.unprocessed_record_keys:
            _new_record_key = self.PROCESSED_RECORDS_FORMAT.format(
                record_id=record_id)
            self.client.copy_object(
                Bucket=self.bucket_name,
                CopySource=f"{self.bucket_name}/{_record_key}",
                Key=_new_record_key)
            self.client.delete_object(Bucket=self.bucket_name, Key=_record_key)
        else:
            self.logger.info(
                f"Record with ID {record_id} is already processed.")

    def store_profile(self, profile: Type[Profile]) -> None:
        """
        Stores a new profile.
        """
        profile_data = profile.dump()
        profile_json = safe_json_serialize(profile_data)

        _key_name = f"{self.PROFILES_PREFIX}{profile.profile_id}.json"
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=_key_name,
            Body=profile_json)

    def store_trace(self, trace: Type[Trace]):
        """
        Stores a new trace.
        """
        trace_data = trace.dump()
        trace_json = safe_json_serialize(trace_data)

        _key_name = f"{self.PROCESSED_TRACES_PREFIX}{trace.trace_id}.json"
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=_key_name,
            Body=trace_json)

    def get_trace(self, trace_id: UUID) -> Type[Trace]:
        """
        Gets a single trace.
        """
        _key = self.TRACE_FORMAT.format(trace_id=str(trace_id))
        try:
            obj = self.client.get_object(Bucket=self.bucket_name, Key=_key)
        except ClientError as err:
            raise RecordStorageError(
                f"Failed to get object {_key}: {err}")

        if "Body" in obj:
            body = json.loads(obj["Body"].read().decode('utf-8'))
            try:
                return Trace.load(body)
            except ValidationError as err:
                raise RecordStorageError(
                    f"Failed to deserialize {body}: {err}")

    """
    Private methods
    """

    def _list_objects_with_paginator(self, prefix: str = None) -> list:
        """
        Returns a list of object with pagination
        """
        keys = []
        paginator = self.client.get_paginator('list_objects')
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix)

        for page in page_iterator:
            if "Contents" in page:
                keys += page["Contents"]

        return keys


class GCPRecordStorage(RecordStorage):
    """
    Storage implementation for GCP Cloud Storage.
    """

    def __init__(
        self,
        project: str,
        region_name: str,
        bucket_name: str
    ) -> None:
        super().__init__()

        self.project = project
        self.region_name = region_name
        self.bucket_name = bucket_name

        self.client = storage.Client(self.project)
        self.bucket = self.client.bucket(self.bucket_name)

    @property
    def profile_ids(self) -> list[UUID]:
        """
        Returns a list of all profile IDs
        """
        profile_blobs = self.client.list_blobs(
            self.bucket_name,
            prefix=self.PROFILES_PREFIX,
            delimiter="/")

        profile_ids = []

        for profile_blob in profile_blobs:
            try:
                base = basename(profile_blob.name)
                profile_ids.append(
                    UUID(splitext(base)[0]))
            except Exception as err:
                self.logger.error(
                    f"Failed to load profile ID: {err}")

        return profile_ids

    @property
    def number_of_profiles(self) -> int:
        """
        Returns the number of recorded profiles.
        """
        return len(self.profile_ids)

    def get_profile(self, profile_id: str) -> Type[Profile]:
        """
        Get a single profile.
        """
        _profile_key = self.PROFILES_FORMAT.format(profile_id=str(profile_id))
        profile_blob = self.bucket.blob(_profile_key)
        profile_json = profile_blob.download_as_bytes()
        profile_data = json.loads(profile_json.decode('utf-8'))
        try:
            return Profile.load(profile_data)
        except ValidationError as err:
            raise RecordStorageError(
                f"Failed to deserialize {profile_data}: {err}")

    def store_profile(self, profile: Type[Profile]) -> None:
        """
        Stores a new profile.
        """
        _profile_key = self.PROFILES_FORMAT.format(
            profile_id=str(profile.profile_id))
        profile_json = safe_json_serialize(profile.dump())

        profile_blob = self.bucket.blob(_profile_key)
        profile_blob.upload_from_string(profile_json)

    """
    Trace methods
    """

    def get_trace(self, trace_id: UUID) -> Type[Trace]:
        """
        Gets a single trace.
        """
        _trace_key = self.TRACE_FORMAT.format(trace_id=str(trace_id))
        trace_blob = self.bucket.blob(_trace_key)
        trace_json = trace_blob.download_as_bytes()
        trace_data = json.loads(trace_json.decode('utf-8'))
        try:
            return Trace.load(trace_data)
        except ValidationError as err:
            raise RecordStorageError(
                f"Failed to deserialize {trace_data}: {err}")

    def store_trace(self, trace: Type[Trace]):
        """
        Store a new trace
        """
        _trace_key = self.TRACE_FORMAT.format(
            trace_id=str(trace.trace_id))
        trace_json = safe_json_serialize(trace.dump())

        trace_blob = self.bucket.blob(_trace_key)
        trace_blob.upload_from_string(trace_json)

    """
    Record methods
    """

    @cached_property
    def unprocessed_record_keys(self) -> list[str]:
        """
        Returns a list of unprocessed records.
        """
        record_blobs = self.client.list_blobs(
            self.bucket_name,
            prefix=self.UNPROCESSED_RECORDS_PREFIX,
            delimiter="/")

        return [b.name for b in record_blobs]

    @property
    def number_of_unprocessed_records(self) -> int:
        """
        Returns the number of unprocessed records.
        """
        return len(self.unprocessed_record_keys)

    def store_unprocessed_record(self, record: Type[TraceRecord]) -> None:
        """
        Stores a new unprocessed record
        """
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=str(record.record_id))
        record_json = safe_json_serialize(record.dump())

        record_blob = self.bucket.blob(_record_key)
        record_blob.upload_from_string(record_json)

    def unprocessed_records(self):
        """
        Generator for all unprocessed records.
        """
        for record_key in self.unprocessed_record_keys:
            record_blob = self.bucket.blob(record_key)
            record_json = record_blob.download_as_bytes()
            record_data = json.loads(record_json.decode('utf-8'))
            try:
                yield TraceRecord.load(record_data)
            except ValidationError as err:
                self.logger.error(
                    f"Failed to deserialize {record_data}: {err}")
                continue

    def mark_record_as_resolved(self, record_id: str) -> None:
        """
        Marks a record as resolved
        """
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=str(record_id))
        _new_record_key = self.PROCESSED_RECORDS_FORMAT.format(
            record_id=str(record_id))

        record_blob = self.bucket.blob(_record_key)

        self.bucket.copy_blob(record_blob, self.bucket, _new_record_key)
        self.bucket.delete_blob(_record_key)
