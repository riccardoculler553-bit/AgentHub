"""DeviceIdentity: the device's stable, persistent identity.

V1.0 stores device_id + device_token. The storage schema reserves the
`token` field name so a future access/refresh token upgrade does not
break existing installs.
"""

from dataclasses import dataclass

import storage


@dataclass
class DeviceIdentity:
    device_id: str
    token: str

    def to_dict(self) -> dict:
        return {"device_id": self.device_id, "device_token": self.token}


class IdentityManager:
    def load(self) -> DeviceIdentity | None:
        data = storage.load_identity()
        if not data or not data.get("device_id") or not data.get("device_token"):
            return None
        return DeviceIdentity(device_id=data["device_id"], token=data["device_token"])

    def save(self, identity: DeviceIdentity) -> None:
        storage.save_identity(identity.to_dict())

    def clear(self) -> None:
        storage.clear_identity()
