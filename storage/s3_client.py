"""
AWS S3 Data Lake Storage Writer & Reader.
Handles raw JSON partitioned landing with GZIP compression and local fallback.
"""

import gzip
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

from config.settings import StorageSettings, get_settings

logger = logging.getLogger(__name__)


def _custom_json_serializer(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    raise TypeError(f"Type {type(obj)} not serializable")


class S3DataLakeWriter:
    """
    Writes raw extracted JSON batches to AWS S3 (or local filesystem staging)
    following standard data lake partitioning:
    raw/<source>/<entity>/year=YYYY/month=MM/day=DD/<batch_id>.json.gz
    """

    def __init__(self, settings: Optional[StorageSettings] = None):
        self.settings = settings or get_settings().storage
        self.storage_mode = self.settings.storage_mode
        self.bucket_name = self.settings.s3_bucket_name
        self.local_path = Path(self.settings.local_storage_path)

        # Initialize boto3 client if in S3 mode
        self._s3_client = None
        if self.storage_mode == "s3":
            try:
                client_kwargs = {
                    "region_name": self.settings.aws_region,
                }
                if self.settings.aws_access_key_id and self.settings.aws_secret_access_key:
                    client_kwargs["aws_access_key_id"] = self.settings.aws_access_key_id.get_secret_value()
                    client_kwargs["aws_secret_access_key"] = self.settings.aws_secret_access_key.get_secret_value()
                if self.settings.s3_endpoint_url:
                    client_kwargs["endpoint_url"] = self.settings.s3_endpoint_url

                self._s3_client = boto3.client("s3", **client_kwargs)
                logger.info(f"Initialized S3 Data Lake Writer for bucket: {self.bucket_name}")
            except Exception as e:
                logger.warning(f"Could not initialize S3 client ({e}). Falling back to local storage.")
                self.storage_mode = "local"

    def build_partition_key(
        self,
        source: str,
        entity: str,
        batch_id: str,
        timestamp: Optional[datetime] = None,
        compressed: bool = True,
    ) -> str:
        """Constructs hive-partitioned S3 key path."""
        ts = timestamp or datetime.now(timezone.utc)
        ext = ".json.gz" if compressed else ".json"
        return (
            f"raw/{source.lower()}/{entity.lower()}/"
            f"year={ts.year:04d}/month={ts.month:02d}/day={ts.day:02d}/"
            f"{batch_id}{ext}"
        )

    def write_raw_batch(
        self,
        source: str,
        entity: str,
        records: List[Dict[str, Any]],
        batch_id: str,
        timestamp: Optional[datetime] = None,
        compress: bool = True,
    ) -> str:
        """
        Serializes and writes raw batch records to S3 or local storage.
        Returns the storage path / URI.
        """
        ts = timestamp or datetime.now(timezone.utc)
        partition_key = self.build_partition_key(
            source=source,
            entity=entity,
            batch_id=batch_id,
            timestamp=ts,
            compressed=compress,
        )

        # Wrap records with ingestion metadata
        payload = {
            "metadata": {
                "source": source,
                "entity": entity,
                "batch_id": batch_id,
                "record_count": len(records),
                "extracted_at": ts.isoformat(),
            },
            "records": records,
        }

        json_bytes = json.dumps(
            payload, default=_custom_json_serializer, indent=2 if not compress else None
        ).encode("utf-8")

        data_to_write = gzip.compress(json_bytes) if compress else json_bytes

        if self.storage_mode == "s3" and self._s3_client:
            return self._write_to_s3(partition_key, data_to_write, compress)
        else:
            return self._write_to_local(partition_key, data_to_write)

    def _write_to_s3(self, key: str, data: bytes, compressed: bool) -> str:
        """Uploads bytes directly to AWS S3 bucket."""
        try:
            extra_args = {
                "ContentType": "application/json",
            }
            if compressed:
                extra_args["ContentEncoding"] = "gzip"

            self._s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                **extra_args,
            )
            s3_uri = f"s3://{self.bucket_name}/{key}"
            logger.info(f"Successfully wrote raw batch to {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Failed to write to S3 bucket {self.bucket_name}: {e}")
            raise

    def _write_to_local(self, key: str, data: bytes) -> str:
        """Writes bytes to local directory mimicking S3 partition structure."""
        target_file = self.local_path / key
        target_file.parent.mkdir(parents=True, exist_ok=True)

        with open(target_file, "wb") as f:
            f.write(data)

        local_uri = str(target_file.resolve())
        logger.info(f"Successfully staged raw batch locally at: {local_uri}")
        return local_uri

    def read_raw_batch(self, path_or_key: str) -> Dict[str, Any]:
        """
        Reads and deserializes raw batch JSON from S3 URI or local file path.
        """
        if path_or_key.startswith("s3://") and self._s3_client:
            parts = path_or_key.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1]
            response = self._s3_client.get_object(Bucket=bucket, Key=key)
            raw_data = response["Body"].read()
        else:
            local_file = Path(path_or_key)
            with open(local_file, "rb") as f:
                raw_data = f.read()

        # Check if gzip compressed (magic numbers 0x1f, 0x8b)
        if raw_data[:2] == b"\x1f\x8b":
            raw_data = gzip.decompress(raw_data)

        return json.loads(raw_data.decode("utf-8"))

    def list_raw_batches(self, source: str, entity: str) -> List[str]:
        """Lists available batch keys for a specific source and entity."""
        prefix = f"raw/{source.lower()}/{entity.lower()}/"
        results = []

        if self.storage_mode == "s3" and self._s3_client:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    results.append(f"s3://{self.bucket_name}/{obj['Key']}")
        else:
            base_dir = self.local_path / prefix
            if base_dir.exists():
                for p in base_dir.glob("**/*.*"):
                    if p.is_file() and (p.name.endswith(".json") or p.name.endswith(".json.gz")):
                        results.append(str(p.resolve()))

        return sorted(results)


def get_storage_writer() -> S3DataLakeWriter:
    """Factory for S3DataLakeWriter."""
    return S3DataLakeWriter()
