from __future__ import annotations
from typing import Optional, Tuple
import io
import time
import datetime as dt

import cv2
from minio import Minio # type: ignore
from minio.error import S3Error # type: ignore


class MinIOClient:
    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 secure: bool = False, default_bucket: Optional[str] = None,
                 presign_expire_seconds: int = 3600):

        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.default_bucket = default_bucket
        self.presign_expire_seconds = int(presign_expire_seconds)

        if default_bucket:
            self._ensure_bucket(default_bucket)

    def ensure_bucket(self, bucket: str) -> None:
        self._ensure_bucket(bucket)

    def put_image(self, key: str, img_bgr, bucket: Optional[str] = None,
                  quality_jpeg: int = 90, return_presigned: bool = False) -> str:
 
        if bucket is None:
            if not self.default_bucket:
                raise ValueError("Bucket is not provided and default_bucket is None.")
            bucket = self.default_bucket
        self._ensure_bucket(bucket)

        ok, buf = cv2.imencode(".jpg", img_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality_jpeg)])
        if not ok:
            raise RuntimeError("Failed to encode image as JPEG.")

        bio = io.BytesIO(buf.tobytes())
        size = len(bio.getvalue())

        self.client.put_object(
            bucket_name=bucket,
            object_name=key,
            data=bio,
            length=size,
            content_type="image/jpeg",
        )

        if return_presigned:
            url = self.client.presigned_get_object(bucket, key, expires=dt.timedelta(seconds=self.presign_expire_seconds))
            return url
        return f"s3://{bucket}/{key}"

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream",
                  bucket: Optional[str] = None, return_presigned: bool = False) -> str:

        if bucket is None:
            if not self.default_bucket:
                raise ValueError("Bucket is not provided and default_bucket is None.")
            bucket = self.default_bucket
        self._ensure_bucket(bucket)

        bio = io.BytesIO(data)
        self.client.put_object(
            bucket_name=bucket,
            object_name=key,
            data=bio,
            length=len(data),
            content_type=content_type,
        )
        if return_presigned:
            url = self.client.presigned_get_object(bucket, key, expires=dt.timedelta(seconds=self.presign_expire_seconds))
            return url
        return f"s3://{bucket}/{key}"

    @staticmethod
    def make_overlay_key(product_code: str, event_id: str, ts: Optional[int] = None) -> str:

        if ts is None:
            ts = int(time.time() * 1000)
        d = dt.datetime.utcfromtimestamp(ts / 1000.0)
        return f"overlay/{product_code}/{d:%Y}/{d:%m}/{d:%d}/{event_id}_overlay.jpg"

    @staticmethod
    def make_raw_key(product_code: str, event_id: str, ts: Optional[int] = None) -> str:
        return MinIOClient.make_overlay_key(product_code, event_id, ts).replace("_overlay.jpg", ".jpg")


    def _ensure_bucket(self, bucket: str) -> None:
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
        except S3Error as e:
            if not self.client.bucket_exists(bucket):
                raise