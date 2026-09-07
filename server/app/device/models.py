"""Pydantic schemas for device APIs."""

from datetime import datetime

from pydantic import BaseModel, Field


class CreateRegistrationCodeIn(BaseModel):
    device_name: str = Field(default="", max_length=100)


class RegistrationCodeOut(BaseModel):
    registration_id: str
    code: str
    expires_at: datetime


class DeviceRegisterIn(BaseModel):
    registration_code: str = Field(min_length=1, max_length=64)
    device_id: str | None = None
    device_name: str = Field(default="", max_length=100)
    hostname: str | None = Field(default=None, max_length=255)
    platform: str | None = Field(default=None, max_length=50)
    client_version: str | None = Field(default=None, max_length=50)


class DeviceRegisterOut(BaseModel):
    device_id: str
    device_token: str
    token_type: str = "device_token"


class DeviceOut(BaseModel):
    device_id: str
    name: str
    hostname: str | None
    platform: str | None
    client_version: str | None
    status: str
    last_seen_at: datetime | None
    created_at: datetime
    revoked_at: datetime | None
    connection_count: int = 0
    online: bool = False


class DeviceMessageIn(BaseModel):
    type: str = Field(default="message", pattern=r"^[a-z_.]{1,64}$")
    data: dict = Field(default_factory=dict)


class DeviceMessageOut(BaseModel):
    message_id: str
    sent: int
