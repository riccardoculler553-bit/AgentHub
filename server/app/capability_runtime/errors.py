"""Capability Runtime domain errors (HTTP-mappable, structured codes)."""

from app.core.exceptions import DeviceLinkError


class CapabilityError(DeviceLinkError):
    status_code = 500
    code = "capability_error"


class CapabilityNotFound(CapabilityError):
    status_code = 404
    code = "CAPABILITY_NOT_FOUND"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"capability not found: {name}")


class CapabilityInvalidName(CapabilityError):
    status_code = 422
    code = "CAPABILITY_INVALID_NAME"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(
            f"capability name must be <domain>.<resource>.<action> (lowercase): {name}"
        )


class CapabilityAlreadyExists(CapabilityError):
    status_code = 409
    code = "CAPABILITY_ALREADY_EXISTS"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"capability already exists: {name}")


class CapabilityDisabled(CapabilityError):
    status_code = 409
    code = "CAPABILITY_DISABLED"

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"capability is disabled: {name}")


class CapabilityVersionNotFound(CapabilityError):
    status_code = 404
    code = "CAPABILITY_VERSION_NOT_FOUND"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"capability version not found: {name}@{version}")


class CapabilityVersionExists(CapabilityError):
    status_code = 409
    code = "CAPABILITY_VERSION_EXISTS"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"capability version already exists (immutable): {name}@{version}")


class CapabilityVersionNotPublished(CapabilityError):
    status_code = 409
    code = "CAPABILITY_VERSION_NOT_PUBLISHED"

    def __init__(self, name: str, version: str, status: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"capability version {name}@{version} is {status}, not PUBLISHED")


class PackageInvalid(CapabilityError):
    status_code = 422
    code = "CAPABILITY_PACKAGE_INVALID"

    def __init__(self, message: str) -> None:
        super().__init__(f"invalid capability package: {message}")


class PackageNotFound(CapabilityError):
    status_code = 404
    code = "CAPABILITY_PACKAGE_NOT_FOUND"

    def __init__(self, package_id: str) -> None:
        self.package_id = package_id
        super().__init__(f"capability package not found: {package_id}")


class CapabilityNoWorker(CapabilityError):
    status_code = 409
    code = "CAPABILITY_NO_WORKER"

    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version
        super().__init__(f"no online worker can run {name}@{version}")
