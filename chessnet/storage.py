from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

import json


@dataclass
class Engine:
    uuid: str
    family: str
    variant: str
    version: str
    parent: Optional[str]
    image: str


@dataclass
class Move:
    san: str
    time_ms: int


@dataclass
class Game:
    uuid: str
    white: str  # Engine UUID
    black: str  # Engine UUID
    moves: List[Move]
    result: str
    white_elo_before: int
    white_elo_after: int
    black_elo_before: int
    black_elo_after: int


class Storage(ABC):
    @abstractmethod
    def list_engines(self) -> List[Engine]:
        raise NotImplementedError()

    @abstractmethod
    def store_engine(self, engine: Engine):
        raise NotImplementedError()

    @abstractmethod
    def get_engine(self, uuid: str) -> Engine:
        raise NotImplementedError()

    @abstractmethod
    def delete_engine(self, uuid: str):
        raise NotImplementedError()

    @abstractmethod
    def store_game(self, game: Game):
        raise NotImplementedError()

    @abstractmethod
    def get_game(self, uuid: str) -> Game:
        raise NotImplementedError()

    @abstractmethod
    def games_for_engine(self, uuid: str) -> List[Game]:
        raise NotImplementedError()


class FileStorage(Storage):
    def __init__(self, path):
        self.path = path
        with open(self.path) as f:
            self.data = json.load(f)

    def list_engines(self) -> List[Engine]:
        return self.data.engines.values()

    def store_engine(self, engine: Engine):
        if engine.uuid in self.data.engines:
            raise Exception("Engine already exists with UUID: {}", engine.uuid)

        self.data.engines[engine.uuid] = engine
        self._flush_to_disk()

    def get_engine(self, uuid: str) -> Engine:
        if engine.uuid not in self.data.engines:
            raise Exception("No engine with UUID: {}", uuid)
        return self.data.engines[uuid]

    def delete_engine(self, uuid: str):
        del self.data.engines[uuid]
        self._flush_to_disk()

    def store_game(self, game: Game):
        if game.uuid in self.data.games:
            raise Exception("Game already exists with UUID: {}", game.uuid)

        self.data.games[game.uuid] = game
        self._flush_to_disk()

    def get_game(self, uuid: str) -> Game:
        if game.uuid not in self.data.games:
            raise Exception("No game with UUID: {}", uuid)
        return self.data.games[uuid]

    def games_for_engine(self, uuid: str) -> List[Game]:
        return [g for g in self.data.games if g.white == uuid or g.black == uuid]

    def _flush_to_disk(self):
        with open(self.path, 'w') as f:
            json.dump(self.data, f)
