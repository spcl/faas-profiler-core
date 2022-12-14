#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Storage interface
"""
from __future__ import annotations

import json

from abc import ABC, abstractproperty, abstractmethod
from typing import Type
from botocore.exceptions import ClientError
from marshmallow import ValidationError
from uuid import UUID, uuid4
from os.path import basename, splitext

from faas_profiler_core.logging import Loggable
from faas_profiler_core.models import Profile, Trace, TraceRecord


class RecordStorageError(RuntimeError):
    pass


def safe_json_serialize(obj: dict) -> str:
    """
    Safe serialize JSON
    """
    def default(o): return f"<<non-serializable: {type(o).__qualname__}>>"

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

    GRAPHS_PREFIX = "graphs/"
    GRAPH_FORMAT = GRAPHS_PREFIX + "{trace_id}.json"

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

    def profiles(self) -> list[Profile]:
        """
        Returns all profiles
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
    def store_unprocessed_record(self, trace_record: dict) -> None:
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
        import boto3

        self.bucket_name = bucket_name
        self.region_name = region_name
        self.client = boto3.client('s3', region_name=self.region_name)

        self._profile_ids = None
        self._unprocessed_record_keys = None

    @property
    def profile_ids(self) -> list[UUID]:
        """
        Returns a list of all profile IDs
        """
        if self._profile_ids is not None:
            return self._profile_ids

        all_profile_keys = self._list_objects_with_paginator(
            prefix=self.PROFILES_PREFIX)

        if all_profile_keys is None or len(all_profile_keys) == 0:
            return []

        self._profile_ids = []
        all_profile_keys = sorted(
            all_profile_keys, key=lambda x: x["LastModified"], reverse=False)

        for profile_key in all_profile_keys:
            try:
                base = basename(profile_key.get("Key", ""))
                self._profile_ids.append(
                    UUID(splitext(base)[0]))
            except Exception as err:
                self.logger.error(
                    f"Failed to load profile ID: {err}")

        return self._profile_ids

    def profiles(self) -> list[Profile]:
        """
        Returns all profiles
        """
        all_profile_keys = self._list_objects_with_paginator(
            prefix=self.PROFILES_PREFIX)

        if all_profile_keys is None or len(all_profile_keys) == 0:
            return []

        for profile_meta in all_profile_keys:
            _key = profile_meta.get("Key")
            if not _key:
                continue

            try:
                obj = self.client.get_object(Bucket=self.bucket_name, Key=_key)
            except ClientError as err:
                self.logger.error(
                    f"Failed to get object {_key}: {err}")
                continue

            if "Body" in obj:
                body = json.loads(obj["Body"].read().decode('utf-8'))
                try:
                    yield Profile.load(body)
                except ValidationError as err:
                    self.logger.error(
                        f"Failed to deserialize {body}: {err}")
                    continue

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

    @property
    def unprocessed_record_keys(self) -> list[str]:
        """
        Returns a list of unprocessed records sorted by last modified
        """
        if self._unprocessed_record_keys is not None:
            self._unprocessed_record_keys

        record_keys = self._list_objects_with_paginator(
            prefix=self.UNPROCESSED_RECORDS_PREFIX)

        self._unprocessed_record_keys = [
            obj["Key"] for obj in sorted(
                record_keys,
                key=lambda x: x["LastModified"],
                reverse=False)]

        return self._unprocessed_record_keys

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

    def store_unprocessed_record(self, trace_record: dict) -> None:
        """
        Stores a new unprocessed record
        """
        _record_id = trace_record.get("tracing_context", {}).get(
            "record_id", uuid4())
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=_record_id)
        record_json = safe_json_serialize(trace_record)

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

        _key_name = self.TRACE_FORMAT.format(trace_id=str(trace.trace_id))
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=_key_name,
            Body=trace_json)

    def store_graph_data(
        self,
        trace_id: UUID,
        graph_data: dict
    ) -> None:
        """
        Stores a graph pickle file.
        """
        graph_key = self.GRAPH_FORMAT.format(trace_id=str(trace_id))
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=graph_key,
            Body=safe_json_serialize(graph_data))

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

    def get_graph_data(
        self,
        trace_id: UUID,
    ) -> str:
        """
        Gets graph pickle
        """
        graph_key = self.GRAPH_FORMAT.format(trace_id=str(trace_id))
        obj = self.client.get_object(Bucket=self.bucket_name, Key=graph_key)

        return json.loads(obj["Body"].read().decode('utf-8'))

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
        from google.cloud import storage

        self.project = project
        self.region_name = region_name
        self.bucket_name = bucket_name

        self.client = storage.Client(self.project)
        self.bucket = self.client.bucket(self.bucket_name)

        self._unprocessed_record_keys = None

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

    def profiles(self) -> list[Profile]:
        """
        Returns all profiles
        """
        profile_blobs = self.client.list_blobs(
            self.bucket_name,
            prefix=self.PROFILES_PREFIX,
            delimiter="/")

        for profile_blob in profile_blobs:
            profile_json = profile_blob.download_as_bytes()
            profile_data = json.loads(profile_json.decode('utf-8'))
            try:
                yield Profile.load(profile_data)
            except ValidationError as err:
                self.logger.error(
                    f"Failed to deserialize {profile_data}: {err}")
                continue

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

    @property
    def unprocessed_record_keys(self) -> list[str]:
        """
        Returns a list of unprocessed records.
        """
        if self._unprocessed_record_keys is not None:
            return self._unprocessed_record_keys

        record_blobs = self.client.list_blobs(
            self.bucket_name,
            prefix=self.UNPROCESSED_RECORDS_PREFIX,
            delimiter="/")

        self._unprocessed_record_keys = [b.name for b in record_blobs]

        return self._unprocessed_record_keys

    @property
    def number_of_unprocessed_records(self) -> int:
        """
        Returns the number of unprocessed records.
        """
        return len(self.unprocessed_record_keys)

    def store_unprocessed_record(self, trace_record: dict) -> None:
        """
        Stores a new unprocessed record
        """
        _record_id = trace_record.get("tracing_context", {}).get(
            "record_id", uuid4())
        _record_key = self.UNPROCESSED_RECORDS_FORMAT.format(
            record_id=_record_id)
        record_json = safe_json_serialize(trace_record)

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

    def store_graph_data(
        self,
        trace_id: UUID,
        graph_data: dict
    ) -> None:
        """
        Stores a graph pickle file.
        """
        graph_key = self.GRAPH_FORMAT.format(trace_id=str(trace_id))
        record_blob = self.bucket.blob(graph_key)
        record_blob.upload_from_string(safe_json_serialize(graph_data))

    def get_graph_data(
        self,
        trace_id: UUID,
    ) -> str:
        """
        Gets graph pickle
        """
        graph_key = self.GRAPH_FORMAT.format(trace_id=str(trace_id))
        graph_blob = self.bucket.blob(graph_key)
        graph_json = graph_blob.download_as_bytes()
        return json.loads(graph_json.decode('utf-8'))
        