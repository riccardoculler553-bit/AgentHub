"""Security primitives: registration code and device token generation/hashing."""

import hashlib
import secrets

# Unambiguous alphabet (no I, O, 0, 1)
_CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


def generate_registration_code() -> str:
    chars = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(8))
    return f"DL-{chars[:4]}-{chars[4:]}"


def generate_device_token() -> str:
    return "dl_dev_" + secrets.token_urlsafe(32)


def hash_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
