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
        """Deliver a task envelope to exactly ONE worker connection of the
        device (never broadcast - double execution risk). Returns 1 on
        success, 0 = device not reachable. This is a TRANSPORT level result,
        not a task level one."""
        return await self.hub.send_to_worker(device_id, envelope)

    def is_online(self, device_id: str) -> bool:
        return self.hub.is_device_online(device_id)
