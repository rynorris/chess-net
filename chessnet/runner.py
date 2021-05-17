from abc import ABC, abstractmethod
import asyncio
import logging
from typing import cast, Any, Optional

import chess
from chess.engine import UciProtocol
import docker  # type: ignore

from chessnet.storage import Engine


log = logging.getLogger(__name__)


class EngineRunner(ABC):
    @abstractmethod
    async def run(self) -> None:
        pass

    @abstractmethod
    async def play(self, board: chess.Board, limit: chess.engine.Limit) -> chess.engine.PlayResult:
        pass

    @abstractmethod
    async def shutdown(self, reason: str) -> None:
        pass

    @abstractmethod
    def engine(self) -> Engine:
        pass


class ProtocolAdapter(asyncio.Protocol):
    def __init__(self, protocol: UciProtocol):
        self.protocol = protocol

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = TransportAdapter(transport)
        self.protocol.connection_made(self.transport)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.transport.alive = False
        self.protocol.connection_lost(exc)

    def data_received(self, data: bytes) -> None:
        self.protocol.pipe_data_received(1, data)


class TransportAdapter(asyncio.SubprocessTransport, asyncio.ReadTransport, asyncio.WriteTransport):
    def __init__(self, transport: asyncio.BaseTransport):
        self.alive = True
        self.transport = transport

    def get_pipe_transport(self, fd: int) -> asyncio.BaseTransport:
        return self

    def write(self, data: bytes) -> None:
        cast(asyncio.WriteTransport, self.transport).write(data)

    def get_returncode(self) -> Optional[int]:
        return None if self.alive else 0

    def get_pid(self) -> int:
        return 0

    def close(self) -> None:
        self.transport.close()

    # Unimplemented: kill(), send_signal(signal), terminate(), and various flow
    # control methods.

class DockerFileRunner(EngineRunner):
    def __init__(self, client: Any, engine: Engine, local_port: int):
        self.client = client
        self._engine = engine
        self.image = engine.image
        self.local_port = local_port
        self.container = None
        self.protocol: Optional[chess.engine.UciProtocol] = None

    async def run(self) -> None:
        self.container = self.client.containers.run(self.image, detach=True, ports={"3333/tcp": self.local_port})
        try:
            log.info("Establishing connection...")
            _, adapter = await asyncio.get_running_loop().create_connection(lambda: ProtocolAdapter(chess.engine.UciProtocol()), host="localhost", port=self.local_port)
            self.protocol = cast(ProtocolAdapter, adapter).protocol

            log.info("Initializing engine...")
            await self.protocol.initialize()
        except:
            await self.shutdown("Error during initialization")
            raise

    async def play(self, board: chess.Board, limit: chess.engine.Limit) -> chess.engine.PlayResult:
        if self.protocol is None:
            raise Exception("Engine not initialized")
        return await self.protocol.play(board, limit)

    async def shutdown(self, reason: str) -> None:
        if self.container is not None:
            self.container.stop()
            self.container.remove()
            self.container = None

    def engine(self) -> Engine:
        return self._engine


