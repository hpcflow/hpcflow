from __future__ import annotations

import asyncio
import socket
import uuid

import zmq
import zmq.asyncio

from hpcflow.sdk.execution.protocol import ErrorResponse, OkResponse
from hpcflow.sdk.utils.async_utils import recv_json
from hpcflow.sdk.wait.protocol import InvalidWaitNotificationError, WaitWakeup


class WaitServer:
    """A server that is started on the host that has requested to wait for completion of a
    workflow submission.

    """

    def __init__(
        self,
        *,
        bind_address: str = "tcp://*",
        advertise_host: str | None = None,
    ) -> None:
        self.bind_address = bind_address
        self.advertise_host = advertise_host or socket.gethostname()

        self.context = zmq.asyncio.Context.instance()
        self.socket: zmq.asyncio.Socket | None = None
        self.port_number: int | None = None

    @property
    def endpoint(self) -> str:
        if self.port_number is None:
            raise RuntimeError("WaitServer is not running.")

        return f"tcp://{self.advertise_host}:{self.port_number}"

    async def start(self) -> str:
        if self.socket is not None:
            raise RuntimeError("WaitServer is already running.")

        self.socket = self.context.socket(zmq.REP)
        self.port_number = self.socket.bind_to_random_port(self.bind_address)

        return self.endpoint

    async def wait(self, timeout: float | None = None) -> WaitWakeup | None:

        if self.socket is None:
            raise RuntimeError("WaitServer.start() must be called first.")

        try:
            if timeout is None:
                data = await recv_json(self.socket)
            else:
                data = await asyncio.wait_for(recv_json(self.socket), timeout=timeout)
        except TimeoutError:
            return None

        try:
            notification = WaitWakeup.from_dict(data)
        except InvalidWaitNotificationError as exc:
            await self.socket.send_json(ErrorResponse(error=str(exc)).to_dict())
            return None

        await self.socket.send_json(OkResponse().to_dict())

        return notification

    async def stop(self) -> None:
        if self.socket is None:
            return

        self.socket.close(linger=0)
        self.socket = None
        self.port_number = None

    @staticmethod
    def get_new_waiter_id():
        return uuid.uuid4().hex
