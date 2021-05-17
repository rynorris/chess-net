import asyncio
from typing import List

from sqlalchemy import (
    delete, insert, select, update,
    ForeignKey, Index, MetaData, Table, Column,
    String, Integer,
)
from sqlalchemy.ext.asyncio import create_async_engine

from chessnet.storage import Game, Engine, Move, Storage


class SqlStorage(Storage):
    def __init__(self, db):
        self.db = db
        self.metadata = MetaData()

        self.engines_table = Table(
            "engines",
            self.metadata,
            Column("engine_id", String, primary_key=True),
            Column("family", String, nullable=False),
            Column("variant", String, nullable=False),
            Column("version", String, nullable=False),
            Column("image", String, nullable=False),
        )

        self.games_table = Table(
            "games",
            self.metadata,
            Column("game_id", String, primary_key=True),
            Column("timestamp", Integer, nullable=False),
            Column("white", ForeignKey("engines.engine_id"), nullable=False, index=True),
            Column("black", ForeignKey("engines.engine_id"), nullable=False, index=True),
            Column("outcome", String),
        )

        self.moves_table = Table(
            "moves",
            self.metadata,
            Column("game_id", ForeignKey("games.game_id"), nullable=False, index=True),
            Column("engine_id", ForeignKey("engines.engine_id"), nullable=False),
            Column("fen_before", String, nullable=False, index=True),
            Column("uci", String, nullable=False),
            Column("timestamp", Integer, nullable=False),

            Index("idx_engine_positions", "engine_id", "fen_before"),
        )


    async def initialize(self):
        async with self.db.begin() as conn:
            await conn.run_sync(self.metadata.create_all)


    async def list_engines(self) -> List[Engine]:
        async with self.db.begin() as conn:
            result = await conn.stream(select(self.engines_table))
            return [self._load_engine(row) async for row in result]

    async def store_engine(self, engine: Engine):
        async with self.db.begin() as conn:
            await conn.execute(insert(self.engines_table).values(**self._store_engine(engine)))

    async def get_engine(self, engine_id: str) -> Engine:
        async with self.db.begin() as conn:
            result = await conn.execute(
                    select(self.engines_table)
                    .where(self.engines_table.c.engine_id == engine_id))
            engines = [self._load_engine(row) for row in result]

        if len(engines) != 1:
            raise Exception("No engine with ID: {}", engine_id)
        return engines[0]

    async def delete_engine(self, engine_id: str):
        async with self.db.begin() as conn:
            await conn.execute(
                    delete(self.engines_table)
                    .where(self.engines_table.c.engine_id == engine_id))

    async def list_games(self) -> List[Game]:
        async with self.db.begin() as conn:
            result = await conn.stream(
                    select(self.games_table)
                    .order_by(self.games_table.c.timestamp.desc()))
            return [self._load_game(row) async for row in result]

    async def store_game(self, game: Game):
        async with self.db.begin() as conn:
            await conn.execute(insert(self.games_table).values(**self._store_game(game)))

    async def finish_game(self, game_id: str, outcome: str):
        async with self.db.begin() as conn:
            await conn.execute(
                    update(self.games_table)
                    .where(self.games_table.c.game_id == game_id)
                    .values(outcome=outcome))

    async def get_game(self, game_id: str) -> Game:
        async with self.db.begin() as conn:
            result = await conn.stream(
                    select(self.games_table)
                    .where(self.games_table.c.game_id == game_id))
            return self._load_game(await result.first())

    async def games_for_engine(self, engine_id: str) -> List[Game]:
        async with self.db.begin() as conn:
            result = await conn.stream(
                    select(self.games_table)
                    .where(self.games_table.c.white == engine_id or self.games_table.c.black == engine_id))
            return [self._load_game(row) async for row in result]

    async def store_move(self, game_id: str, move: Move, fen_before: str, engine_id: str):
        async with self.db.begin() as conn:
            await conn.execute(
                    insert(self.moves_table)
                    .values(**self._store_move(game_id, move, fen_before, engine_id)))

    async def moves_in_game(self, game_id: str) -> List[Move]:
        async with self.db.begin() as conn:
            result = await conn.stream(
                    select(self.moves_table.c.uci, self.moves_table.c.timestamp)
                    .where(self.moves_table.c.game_id == game_id)
                    .order_by(self.moves_table.c.timestamp))
            return [self._load_move(row) async for row in result]

    def _load_engine(self, row):
        return Engine(
            family=row.family,
            variant=row.variant,
            version=row.version,
            image=row.image,
        )

    def _store_engine(self, engine):
        return {
            "engine_id": engine.id(),
            "family": engine.family,
            "variant": engine.variant,
            "version": engine.version,
            "image": engine.image,
        }

    def _load_game(self, row):
        return Game(
            game_id=row.game_id,
            timestamp=row.timestamp,
            white=row.white,
            black=row.black,
            outcome=row.outcome,
        )

    def _store_game(self, game):
        return {
            "game_id": game.game_id,
            "timestamp": game.timestamp,
            "white": game.white,
            "black": game.black,
            "outcome": game.outcome,
        }

    def _load_move(self, row):
        return Move(
            uci=row.uci,
            timestamp=row.timestamp,
        )

    def _store_move(self, game_id: str, move: Move, fen_before: str, engine_id: str):
        return {
            "game_id": game_id,
            "uci": move.uci,
            "timestamp": move.timestamp,
            "fen_before": fen_before,
            "engine_id": engine_id,
        }

