import asyncio
from collections.abc import Iterable
from enum import Enum
from typing import Literal, Union
import uuid

from pydantic.dataclasses import dataclass

from chessnet.storage import Engine, Game, Move


EventType = Enum("EventType", [
    "START_GAME",
    "MAKE_MOVE",
    "END_GAME",
])


@dataclass(frozen=True)
class StartGameEvent:
    typ: Literal[EventType.START_GAME]
    game: Game


@dataclass(frozen=True)
class EndGameEvent:
    typ: Literal[EventType.END_GAME]
    game_id: str
    outcome: str


@dataclass(frozen=True)
class MakeMoveEvent:
    typ: Literal[EventType.MAKE_MOVE]
    game_id: str
    move: Move
    fen_before: str
    engine_id: str


Event = Union[
    StartGameEvent,
    EndGameEvent,
]


class Events:
    @staticmethod
    def start_game(game: Game):
        return StartGameEvent(EventType.START_GAME, game)

    @staticmethod
    def end_game(game_id: str, outcome: str):
        return EndGameEvent(EventType.END_GAME, game_id, outcome)

    @staticmethod
    def make_move(game_id: str, move: Move, fen_before: str, engine_id: str):
        return MakeMoveEvent(EventType.MAKE_MOVE, game_id, move, fen_before, engine_id)


class Subscription:
    def __init__(self, channel: str, typs: Iterable[EventType], callback):
        self.channel = channel
        self.typs = set(typs)
        self.callback = callback

    async def accept(self, event: Event):
        if event.typ in self.typs:
            await self.callback(event)


class Broker:
    def __init__(self):
        # Subscriptions per channel.
        # Special key "*" for all.
        self.subscriptions = {}

    def publish(self, channel: str, event: Event):
        for sub in self.subscriptions.get(channel, {}).values():
            asyncio.create_task(sub.accept(event))

        for sub in self.subscriptions.get("*", {}).values():
            asyncio.create_task(sub.accept(event))

    def subscribe(self, channel, typs: Iterable[EventType], callback) -> str:
        handle = str(uuid.uuid4())
        sub = Subscription(channel, typs, callback)
        if channel not in self.subscriptions:
            self.subscriptions[channel] = {}

        self.subscriptions[channel][handle] = sub
        return handle

    def unsubscribe(self, handle: str):
        channels = list(self.subscriptions.keys())
        for channel in channels:
            if handle in self.subscriptions[channel]:
                del self.subscriptions[channel][handle]

            if len(self.subscriptions[channel]) == 0:
                del self.subscriptions[channel]

