"""DeviceLinkService: the only seam between AgentHub and DeviceLink.

AgentHub business code never touches ConnectionHub/WebSocket directly - it
asks this service to deliver envelopes. Swapping the transport layer later
only means changing this class.
"""

from app.websocket.hub import ConnectionHub
from app.websocket.protocol import Envelope


class DeviceLinkService:
    def __init__(self, hub: ConnectionHub) -> None:
        self.hub = hub

    async def send_task(self, device_id: str, envelope: Envelope) -> int:
        """Deliver an envelope to every live connection of the device.

        Returns the number of successful sends (0 = device not reachable).
        This is a TRANSPORT level result, not a task level one."""
        return await self.hub.send_to_device(device_id, envelope)

    def is_online(self, device_id: str) -> bool:
        return self.hub.is_device_online(device_id)
