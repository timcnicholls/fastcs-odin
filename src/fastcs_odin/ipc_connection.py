from collections.abc import Mapping

from odin_data.control.async_ipc_channel import AsyncIpcChannel, IpcChannelException
from odin_data.control.ipc_message import IpcMessage

ValueType = bool | int | float | str
JsonElementary = str | int | float | bool | None
JsonType = JsonElementary | list["JsonType"] | Mapping[str, "JsonType"]


class IPCConnection:
    def __init__(self, endpoint: str):
        self._endpoint = endpoint
        self._channel: AsyncIpcChannel | None = None
        self.msg_id: int = 0

    def open(self) -> None:
        self._channel = AsyncIpcChannel(
            AsyncIpcChannel.CHANNEL_TYPE_DEALER, endpoint=self._endpoint
        )
        self._channel.connect()

    async def close(self) -> None:
        if self._channel:
            self._channel.close()
            self._channel = None

    def _next_msg_id(self) -> int:
        self.msg_id += 1
        return self.msg_id

    async def get_adapters(self):
        if not self._channel:
            raise IpcChannelException("IPC connection not open")

        msg = IpcMessage("cmd", "request_adapters", id=self._next_msg_id())
        await self._channel.send(msg.encode())
        data = await self._channel.recv()
        response = IpcMessage(from_str=data)

        # Unpack adapter response to return list of adapter names. TODO the raw response
        # to this as as dict of adapter names and classes is actually useful to the
        # controller, so maybe we return that instead if the odin_control HTTP response
        # were to match
        adapters = {"adapters": list(response.get_params()["adapters"])}
        return adapters

    async def get(self, uri: str, with_metadata: bool = False) -> dict[str, JsonType]:
        if not self._channel:
            raise IpcChannelException("IPC connection not open")

        msg = IpcMessage("cmd", "get", id=self._next_msg_id())
        msg.set_param("paths", [uri])
        msg.set_param("metadata", with_metadata)
        await self._channel.send(msg.encode())
        data = await self._channel.recv()
        response = IpcMessage(from_str=data)
        return response.get_params()["paths"][uri]

    async def put(self, uri: str, value: ValueType) -> JsonType:
        if not self._channel:
            raise IpcChannelException("IPC connection not open")

        msg = IpcMessage("cmd", "set", id=self._next_msg_id())
        msg.set_param(
            "paths",
            {
                uri: value,
            },
        )
        print(msg)
        await self._channel.send(msg.encode())
        data = await self._channel.recv()
        response = IpcMessage(from_str=data)

        return response.get_params()["paths"][uri]
