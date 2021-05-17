from abc import ABC, abstractmethod
from pydantic.dataclasses import dataclass
from typing import List, NoReturn, Optional

import json
import os
import pickle


@dataclass(frozen=True)
class Engine:
    family: str
    variant: str
    version: str
    image: str

    def id(self):
        return f"{self.family}#{self.variant}#{self.version}"

    def __str__(self):
        return self.id()


@dataclass(frozen=True)
class Move:
    uci: str
    timestamp: int # ms since epoch


@dataclass(frozen=True)
class Game:
    game_id: str
    timestamp: int  # ms since epoch
    white: str  # Engine UUID
    black: str  # Engine UUID
    outcome: Optional[str]


class Storage(ABC):
    @abstractmethod
    async def list_engines(self) -> List[Engine]:
        raise NotImplementedError()

    @abstractmethod
    async def store_engine(self, engine: Engine) -> NoReturn:
        raise NotImplementedError()

    @abstractmethod
    async def get_engine(self, engine_id: str) -> Engine:
        raise NotImplementedError()

    @abstractmethod
    async def delete_engine(self, engine_id: str) -> NoReturn:
        raise NotImplementedError()

    @abstractmethod
    async def list_games(self) -> List[Game]:
        raise NotImplementedError()

    @abstractmethod
    async def store_game(self, game: Game) -> NoReturn:
        raise NotImplementedError()

    @abstractmethod
    async def finish_game(self, game_id: str, outcome: str) -> NoReturn:
        raise NotImplementedError()

    @abstractmethod
    async def get_game(self, game_id: str) -> Game:
        raise NotImplementedError()

    @abstractmethod
    async def games_for_engine(self, engine_id: str) -> List[Game]:
        raise NotImplementedError()

    @abstractmethod
    async def store_move(self, game_id: str, move: Move, fen_before: str, engine_id: str) -> NoReturn:
        raise NotImplementedError()

    @abstractmethod
    async def moves_in_game(self, game_id: str) -> List[Move]:
        raise NotImplementedError()

