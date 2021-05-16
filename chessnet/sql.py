import asyncio
from typing import List

from sqlalchemy import (
    insert, text,
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
            Column("san", String, nullable=False),
            Column("timestamp", Integer, nullable=False),

            Index("idx_engine_positions", "engine_id", "fen_before"),
        )


    async def initialize(self):
        async with self.db.begin() as conn:
            await conn.run_sync(self.metadata.create_all)


    async def list_engines(self) -> List[Engine]:
        async with self.db.begin() as conn:
            result = await conn.execute(text("SELECT * FROM engines"))
            return [self._load_engine(row) for row in result]

    async def store_engine(self, engine: Engine):
        async with self.db.begin() as conn:
            await conn.execute(insert(self.engines_table).values(**self._store_engine(engine)))

    async def get_engine(self, engine_id: str) -> Engine:
        async with self.db.begin() as conn:
            result = await conn.execute(text("SELECT * FROM engines WHERE engine_id = :engine_id").bindparams(engine_id=engine_id))
            engines = [self._load_engine(row) for row in result]

        if len(engines) != 1:
            raise Exception("No engine async with ID: {}", engine_id)
        return engines[0]

    async def delete_engine(self, engine_id: str):
        async with self.db.begin() as conn:
            await conn.execute(text("DELETE FROM engines WHERE engine_id = :engine_id").bindparams(engine_id=engine_id))

    async def list_games(self) -> List[Game]:
        async with self.db.begin() as conn:
            result = await conn.execute(text("SELECT * FROM games"))

    async def store_game(self, game: Game):
        raise NotImplementedError()

    async def get_game(self, uuid: str) -> Game:
        raise NotImplementedError()

    async def games_for_engine(self, uuid: str) -> List[Game]:
        raise NotImplementedError()

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


async def main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True, future=True)
    storage = SqlStorage(engine)
    await storage.initialize()
    await storage.store_engine(Engine("stockfish", "main", "11", "andrijdavid/stockfish:11"))
    await storage.store_engine(Engine("stockfish", "main", "12", "andrijdavid/stockfish:12"))
    print(await storage.list_engines())
    await storage.delete_engine("stockfish#main#11")
    print(await storage.list_engines())

if __name__ == "__main__":
    asyncio.run(main())
