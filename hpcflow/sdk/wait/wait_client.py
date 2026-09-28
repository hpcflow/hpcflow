from __future__ import annotations

import asyncio

import zmq
import zmq.asyncio

from hpcflow.sdk.utils.async_utils import recv_json
from hpcflow.sdk.wait.protocol import WaitWakeup


class WaitClient:
    def __init__(self, endpoint: str, *, timeout: float = 2.0) -> None:
        """
        Parameters
        ----------
        endpoint: str
            The server endpoint to connect to.
        timeout: float
            This should be short. Otherwise a compute node might be hanging around for a
            long time while trying to notify an unreachable login node (in the case
            compute node -> login node connectivity is not possible).
        """
        self.endpoint = endpoint
        self.timeout = timeout
        self.context = zmq.asyncio.Context.instance()

    async def notify(self, notification: WaitWakeup) -> bool:
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self.endpoint)

        try:
            await socket.send_json(notification.to_dict())

            try:
                response = await asyncio.wait_for(recv_json(socket), timeout=self.timeout)
            except asyncio.TimeoutError:
                return False

            return response.get("ok") is True

        finally:
            socket.close(linger=0)
