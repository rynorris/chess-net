from abc import ABC, abstractmethod
import asyncio

import chess
from chess.engine import UciProtocol
import docker

from chessnet.storage import Engine


class EngineRunner(ABC):
    @abstractmethod
    def run():
        pass

    @abstractmethod
    def shutdown():
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
    def __init__(self, client, image, local_port):
        self.client = client
        self.image = image
        self.local_port = local_port
        self.container = None
        self.engine = None

    async def run(self):
        self.container = self.client.containers.run(self.image, detach=True, ports={"3333/tcp": self.local_port})
        try:
            print("Establishing connection...")
            _, adapter = await asyncio.get_running_loop().create_connection(lambda: ProtocolAdapter(chess.engine.UciProtocol()), host="localhost", port=self.local_port)
            self.engine = adapter.protocol

            print("Initializing engine...")
            await self.engine.initialize()
        except:
            self.shutdown()
            raise

    async def play(self, board, limit):
        return await self.engine.play(board, limit)

    def shutdown(self):
        self.container.stop()
        self.container.remove()
        self.container = None



