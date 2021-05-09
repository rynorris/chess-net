from abc import ABC, abstractmethod
from pydantic.dataclasses import dataclass
from typing import List, Optional

import json
import os
import pickle


@dataclass
class Engine:
    family: str
    variant: str
    version: str
    image: str

    def id(self):
        return f"{self.family}#{self.variant}#{self.version}"

    def __str__(self):
        return self.id()


@dataclass
class Move:
    san: str
    time_ms: int


@dataclass
class Game:
    uuid: str
    timestamp: int  # Seconds since epoch
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
    def list_games(self) -> List[Game]:
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
        if os.path.exists(path):
            with open(self.path, 'rb+') as f:
                self.data = pickle.load(f)
        else:
            self.data = {"engines": {}, "games": {}}

    def list_engines(self) -> List[Engine]:
        return list(self.data["engines"].values())

    def store_engine(self, engine: Engine):
        if engine.id() in self.data["engines"]:
            raise Exception("Engine already exists with id: {}", engine.id())

        self.data["engines"][engine.id()] = engine
        self._flush_to_disk()

    def get_engine(self, engine_id: str) -> Engine:
        if engine_id not in self.data["engines"]:
            raise Exception("No engine with UUID: {}", engine_id)
        return self.data["engines"][engine_id]

    def delete_engine(self, uuid: str):
        del self.data.engines[uuid]
        self._flush_to_disk()

    def list_games(self) -> List[Game]:
        return list(self.data["games"].values())

    def store_game(self, game: Game):
        if game.uuid in self.data["games"]:
            raise Exception("Game already exists with UUID: {}", game.uuid)

        self.data["games"][game.uuid] = game
        self._flush_to_disk()

    def get_game(self, uuid: str) -> Game:
        if game.uuid not in self.data["games"]:
            raise Exception("No game with UUID: {}", uuid)
        return self.data["games"][uuid]

    def games_for_engine(self, engine_id: str) -> List[Game]:
        return [g for g in self.data["games"] if g.white == engine_id or g.black == engine_id]

    def _flush_to_disk(self):
        with open(self.path, 'wb') as f:
            pickle.dump(self.data, f)
