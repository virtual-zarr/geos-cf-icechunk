import os
from typing import Any, Literal

from pydantic_settings import BaseSettings

print("STAGE from env:", os.getenv("STAGE"))


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    PROJECT_NAME: str = "geos-cf-virtualizarr-data-pipelines"
    STACK_NAME: str = "geos-cf-virtualizarr-data-pipelines"
    STAGE: Literal["dev", "prod"]
    ACCOUNT_ID: str
    ACCOUNT_REGION: str = "us-east-1"
    ICECHUNK_BUCKET_NAME: str = "icechunk-output"
    ICECHUNK_BUCKET: str | None = None
    ICECHUNK_PREFIX: str
    PROJECT: str = "geos-cf-virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 25

    # search_latest Lambda settings
    THREDDS_ANALYSIS_V2_CATALOG: str | None = None
    THREDDS_QUERY_PATTERN: str | None = None
    SEARCH_LATEST_SCHEDULE_MINUTES: int | None = None

    # Freguency in days to run garbage collection.
    GARBAGE_COLLECTION_FREQUENCY: int | None = None

    VPC_ID: str | None = None
    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
    )

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10
