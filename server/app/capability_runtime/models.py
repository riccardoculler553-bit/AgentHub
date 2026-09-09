"""Pydantic schemas for the Capability Runtime APIs (V1.4 §61-§62)."""

from datetime import datetime

from pydantic import BaseModel, Field


class CapabilityCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    display_name: str = Field(default="", max_length=200)
    description: str = Field(default="", max_length=1000)
    # ATOMIC | BUSINESS
    type: str = Field(default="ATOMIC", max_length=16)
    # YINGDAO | PYTHON | HTTP | LOCAL
    runtime_type: str = Field(max_length=16)
    input_schema: dict = Field(default_factory=dict)
    output_schema: dict = Field(default_factory=dict)
    risk_level: str = Field(default="READ", max_length=16)
    requires_confirmation: bool = False


class CapabilityUpdateIn(BaseModel):
    enabled: bool | None = None
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=1000)


class CapabilityOut(BaseModel):
    name: str
    display_name: str
    description: str
    type: str
    runtime_type: str
    enabled: bool
    current_version: str | None
    input_schema: dict
    output_schema: dict
    risk_level: str
    requires_confirmation: bool
    created_at: datetime
    updated_at: datetime


class CapabilityVersionOut(BaseModel):
    id: int
    capability_name: str
    version: str
    package_id: str
    status: str
    input_schema: dict
    output_schema: dict
    entrypoint: str
    checksum: str
    created_at: datetime


class WorkerCapabilityOut(BaseModel):
    worker_id: str
    capabilities: list[dict]
