"""Cloudflare R2 client for dian-scraper-test.

Used only when STORAGE_MODE=r2. The scraper writes one ZIP per CUFE
directly to NUVARA's R2 bucket under the prefix
`dian-scraper-alt/{company_id}/{job_id}/{cufe}.zip`, then surfaces the
`r2_key + r2_url` in the API response. NUVARA reads those bytes from
R2 itself — there is no second HTTP hop between NUVARA and this
scraper to fetch the file.

boto3 is sync; we use `asyncio.to_thread` to keep the FastAPI event
loop unblocked during uploads. ZIPs from DIAN are small (~80 KB on
average, <1 MB worst case) so a thread-pool hop costs us nothing
compared to the network IO itself.

Errors propagate. botocore already retries transient 5xx and network
drops; anything that bubbles out of here is durable and the worker
will surface it as `status=failed` on the job row.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

# boto3 + botocore are only needed when STORAGE_MODE=r2. Defer the
# runtime import to `from_env` so the legacy local-filesystem path
# can boot without these deps. The TYPE_CHECKING guard keeps the type
# hints alive for IDE/mypy without paying the import cost at runtime.
if TYPE_CHECKING:
    import boto3  # noqa: F401
    from botocore.exceptions import ClientError  # noqa: F401

logger = logging.getLogger("dian-scraper.r2")


@dataclass(frozen=True)
class UploadResult:
    """What a successful upload returns to the caller."""

    key: str
    url: str
    size_bytes: int


class R2Storage:
    """Thin wrapper over boto3 S3 client configured for Cloudflare R2.

    Lifetime: created once at FastAPI startup via `from_env`. The
    underlying boto3 client is thread-safe; we just keep one instance
    for the whole process.

    Bucket layout (matches what NUVARA already uses):

        dian-scraper-alt/{company_id}/{job_id}/{cufe}.zip

    Why this prefix tree:

      - Per-tenant scoping for cost reporting (R2 bills can be sliced
        by prefix).
      - Per-job nesting keeps clean-up trivial — wipe one job by
        deleting a prefix, no row-by-row lookup needed.
      - The CUFE is the natural unique key, no risk of collision
        across companies.
    """

    def __init__(
        self,
        *,
        client,
        bucket: str,
        public_base_url: Optional[str] = None,
    ) -> None:
        self._client = client
        self._bucket = bucket
        # When the bucket is fronted by a public custom domain (e.g.
        # pub-xxx.r2.dev or your own CNAME), we hand back a stable URL
        # that NUVARA can use without presigning. Falls back to None
        # → callers should presign on demand.
        self._public_base_url = (
            public_base_url.rstrip("/") if public_base_url else None
        )

    # ── construction ───────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "R2Storage":
        """Build an R2Storage from R2_* env vars.

        Required:
          R2_ENDPOINT      — full URL, e.g. https://<acct>.r2.cloudflarestorage.com
          R2_BUCKET        — bucket name
          R2_ACCESS_KEY    — access key id
          R2_SECRET_KEY    — secret access key

        Optional:
          R2_REGION        — defaults to 'auto' (R2 ignores it)
          R2_PUBLIC_URL    — public base URL for built URLs
                             (e.g. https://pub-xxx.r2.dev). When set,
                             returned `url` is `{base}/{key}`. When
                             unset, returned `url` is the S3-endpoint
                             URL (still works but routes via R2 API).

        Imports are runtime-deferred so the legacy in-memory backend
        does not require boto3 to be installed.
        """
        import boto3  # noqa: PLC0415
        from botocore.client import Config  # noqa: PLC0415

        endpoint = _required("R2_ENDPOINT")
        bucket = _required("R2_BUCKET")
        access_key = _required("R2_ACCESS_KEY")
        secret_key = _required("R2_SECRET_KEY")
        region = os.environ.get("R2_REGION", "auto").strip() or "auto"
        public_base = os.environ.get("R2_PUBLIC_URL", "").strip() or None

        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            # R2 is virtual-hosted style by default but the S3 client
            # defaults work fine. Force path-style addressing when in
            # doubt — it works against both R2 and minio test setups.
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                # Retries are best-effort for transient failures.
                # Stick with the boto3 defaults (3 retries, exponential
                # backoff) — overriding them per-call only when a
                # specific upload needs different behaviour.
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        logger.info(
            "R2Storage: endpoint=%s bucket=%s region=%s public_base=%s",
            endpoint, bucket, region, public_base or "(none)",
        )
        return cls(client=client, bucket=bucket, public_base_url=public_base)

    # ── operations ─────────────────────────────────────────────────────

    async def upload_zip(
        self,
        *,
        company_id: str,
        job_id: str,
        cufe: Optional[str],
        filename: str,
        body: bytes,
    ) -> UploadResult:
        """Upload one ZIP under the canonical prefix tree.

        The CUFE (when present) becomes the filename basename so the
        consumer can identify the invoice without parsing the body. If
        the engine could not extract a CUFE, the original filename is
        kept verbatim — those rows show up as orphans in NUVARA and
        the operator decides what to do.
        """
        # Sanitize filename like the legacy local-storage code does, so
        # the R2 key never carries shell-unfriendly characters.
        safe_name = _sanitize_filename(filename)
        key = self._build_key(
            company_id=company_id,
            job_id=job_id,
            cufe=cufe,
            filename=safe_name,
        )

        # Sync put_object on a worker thread → frees the event loop
        # while boto3 negotiates with R2.
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/zip",
        )

        return UploadResult(
            key=key,
            url=self._build_url(key),
            size_bytes=len(body),
        )

    async def delete_job_prefix(
        self,
        *,
        company_id: str,
        job_id: str,
    ) -> int:
        """Wipe every file written by a job. Used by the cancel flow
        and by the ops cleanup script (when we add one).

        Returns the count of deleted objects. Idempotent — running it
        twice on the same job returns 0 the second time.
        """
        prefix = f"dian-scraper-alt/{company_id}/{job_id}/"
        return await asyncio.to_thread(self._delete_prefix_sync, prefix)

    def _delete_prefix_sync(self, prefix: str) -> int:
        """Sync helper invoked under asyncio.to_thread.

        S3 batch-delete accepts up to 1000 keys per call; we page
        through with a paginator so the call never overflows.
        """
        deleted = 0
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            contents = page.get("Contents") or []
            if not contents:
                continue
            objects = [{"Key": obj["Key"]} for obj in contents]
            self._client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
            deleted += len(objects)
        return deleted

    async def health_check(self) -> bool:
        """Tiny operation to verify creds + connectivity at startup.

        Tries HEAD on the bucket. Returns False (and logs) if anything
        fails — callers can decide whether to block startup on this.
        """
        # Import here too so an in-memory backend that never hits R2
        # never pays the cost of loading botocore.
        from botocore.exceptions import ClientError  # noqa: PLC0415
        try:
            await asyncio.to_thread(
                self._client.head_bucket, Bucket=self._bucket
            )
            return True
        except ClientError as e:
            logger.warning("R2 health check failed: %s", e)
            return False

    # ── key + url builders ─────────────────────────────────────────────

    @staticmethod
    def _build_key(
        *,
        company_id: str,
        job_id: str,
        cufe: Optional[str],
        filename: str,
    ) -> str:
        """Build the canonical R2 key for an uploaded file.

        When CUFE is known we use `{cufe[:24]}.zip` so the key is
        predictable and human-grep-able in the R2 console. The 24-char
        prefix is plenty unique for a per-job folder and short enough
        to read without scrolling.
        """
        if cufe:
            # We keep the original suffix (zip) since `filename` already
            # has the right one — just override the basename with cufe.
            ext = filename.rsplit(".", 1)[-1] if "." in filename else "zip"
            basename = f"{cufe[:24]}.{ext}"
        else:
            basename = filename
        return f"dian-scraper-alt/{company_id}/{job_id}/{basename}"

    def _build_url(self, key: str) -> str:
        """Return a URL the consumer can use to identify the object.

        If a public base is configured we point there (stable, no
        signing). Otherwise we return the S3-endpoint URL — NUVARA
        will not actually fetch it; it uses the key against its own
        R2 client. The URL is there for human debugging.
        """
        if self._public_base_url:
            return f"{self._public_base_url}/{key}"
        # Fall back to a synthetic URL based on the S3 endpoint. This
        # is NOT a public URL — anyone hitting it without credentials
        # gets 403. We surface it anyway because (a) the public_url
        # is optional and (b) logs/audit reads benefit from a stable
        # identifier.
        return f"{self._client.meta.endpoint_url.rstrip('/')}/{self._bucket}/{key}"


# ──────────────────────────────────────────────────────────────────────────
# Small helpers — not exported; only useful inside this module.
# ──────────────────────────────────────────────────────────────────────────


def _required(name: str) -> str:
    """Read a required env var or raise — call at boot, not per-request.

    We crash early on missing config so a deploy with half the R2 vars
    set fails the healthcheck instead of silently dropping uploads.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise RuntimeError(
            f"STORAGE_MODE=r2 requires {name} — got empty string"
        )
    return raw


def _sanitize_filename(name: str) -> str:
    """Strip characters that survive in the wild but break R2 keys.

    Matches the convention the legacy local-storage code used:
    keep alphanumerics, dashes, dots, underscores; replace everything
    else with `_`. Prevents leading dots that would create hidden
    files in the rare case the engine misreports a filename.
    """
    import re
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name).lstrip(".")
    return safe or "file"
