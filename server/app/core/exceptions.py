"""Domain exceptions with machine-readable codes and HTTP status."""


class DeviceLinkError(Exception):
    code = "internal_error"
    status_code = 500
    message = "internal error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.message)


class RegistrationCodeInvalid(DeviceLinkError):
    code = "registration_code_invalid"
    status_code = 400
    message = "registration code is invalid"


class RegistrationCodeExpired(DeviceLinkError):
    code = "registration_code_expired"
    status_code = 400
    message = "registration code is expired"


class RegistrationCodeUsed(DeviceLinkError):
    code = "registration_code_used"
    status_code = 400
    message = "registration code has already been used"


class DeviceNotFound(DeviceLinkError):
    code = "device_not_found"
    status_code = 404
    message = "device not found"


class DeviceRevoked(DeviceLinkError):
    code = "device_revoked"
    status_code = 403
    message = "device has been revoked"


class TokenInvalid(DeviceLinkError):
    code = "token_invalid"
    status_code = 401
    message = "device token is invalid"


class TokenRevoked(DeviceLinkError):
    code = "token_revoked"
    status_code = 401
    message = "device token has been revoked"
