from abc import ABC, abstractmethod
import asyncio
import logging

import chess
from chess.engine import UciProtocol
import docker

from chessnet.storage import Engine


log = logging.getLogger(__name__)


class EngineRunner(ABC):
    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    async def play(self, board, limit):
        pass

    @abstractmethod
    def shutdown(self, reason):
        pass

    @abstractmethod
    def engine() -> Engine:
        pass


class ProtocolAdapter(asyncio.Protocol):
    def __init__(self, protocol):
        self.protocol = protocol

    def connection_made(self, transport):
        self.transport = TransportAdapter(transport)
        self.protocol.connection_made(self.transport)

    def connection_lost(self, exc):
        self.transport.alive = False
        self.protocol.connection_lost(exc)

    def data_received(self, data):
        self.protocol.pipe_data_received(1, data)


class TransportAdapter(asyncio.SubprocessTransport, asyncio.ReadTransport, asyncio.WriteTransport):
    def __init__(self, transport):
        self.alive = True
        self.transport = transport

    def get_pipe_transport(self, fd):
        return self

    def write(self, data):
        self.transport.write(data)

    def get_returncode(self):
        return None if self.alive else 0

    def get_pid(self):
        return None

    def close(self):
        self.transport.close()

    # Unimplemented: kill(), send_signal(signal), terminate(), and various flow
    # control methods.

class DockerFileRunner(EngineRunner):
    def __init__(self, client, engine, local_port):
        self.client = client
        self._engine = engine
        self.image = engine.image
        self.local_port = local_port
        self.container = None
        self.protocol = None

    async def run(self):
        self.container = self.client.containers.run(self.image, detach=True, ports={"3333/tcp": self.local_port})
        try:
            log.info("Establishing connection...")
            _, adapter = await asyncio.get_running_loop().create_connection(lambda: ProtocolAdapter(chess.engine.UciProtocol()), host="localhost", port=self.local_port)
            self.protocol = adapter.protocol

            log.info("Initializing engine...")
            await self.protocol.initialize()
        except:
            await self.shutdown()
            raise

    async def play(self, board, limit):
        return await self.protocol.play(board, limit)

    async def shutdown(self, reason):
        self.container.stop()
        self.container.remove()
        self.container = None

    def engine(self):
        return self._engine


