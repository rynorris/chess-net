from abc import ABC, abstractmethod
from pydantic.dataclasses import dataclass
from typing import List, Optional, AsyncIterable

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
    timestamp: int # ms since epoch


@dataclass
class Game:
    game_id: str
    timestamp: int  # ms since epoch
    white: str  # Engine UUID
    black: str  # Engine UUID
    outcome: Optional[str]


class Storage(ABC):
    @abstractmethod
    async def list_engines(self) -> AsyncIterable[Engine]:
        raise NotImplementedError()

    @abstractmethod
    async def store_engine(self, engine: Engine):
        raise NotImplementedError()

    @abstractmethod
    async def get_engine(self, engine_id: str) -> Engine:
        raise NotImplementedError()

    @abstractmethod
    async def delete_engine(self, engine_id: str):
        raise NotImplementedError()

    @abstractmethod
    async def list_games(self) -> AsyncIterable[Game]:
        raise NotImplementedError()

    @abstractmethod
    async def store_game(self, game: Game):
        raise NotImplementedError()

    @abstractmethod
    async def get_game(self, uuid: str) -> Game:
        raise NotImplementedError()

    @abstractmethod
    async def games_for_engine(self, uuid: str) -> List[Game]:
        raise NotImplementedError()

