from typing import List

from sqlalchemy import create_engine, insert, text, MetaData, Table, Column, String

from chessnet.storage import Game, Engine, Move, Storage


class SqlStorage(Storage):
    def __init__(self, db):
        self.db = db
        self.metadata = MetaData()
        self.engines_table = Table(
            "engines",
            self.metadata,
            Column("engine_id", String, primary_key=True),
            Column("family", String),
            Column("variant", String),
            Column("version", String),
            Column("image", String),
        )
        self.metadata.create_all(self.db)


    def list_engines(self) -> List[Engine]:
        with self.db.begin() as conn:
            result = conn.execute(text("SELECT * FROM engines"))
            return [self._load_engine(row) for row in result]

    def store_engine(self, engine: Engine):
        with self.db.begin() as conn:
            conn.execute(insert(self.engines_table).values(**self._store_engine(engine)))

    def get_engine(self, engine_id: str) -> Engine:
        with self.db.begin() as conn:
            result = conn.execute(text("SELECT * FROM engines WHERE engine_id = :engine_id").bindparams(engine_id=engine_id))
            engines = [self._load_engine(row) for row in result]

        if len(engines) != 1:
            raise Exception("No engine with ID: {}", engine_id)
        return engines[0]

    def delete_engine(self, engine_id: str):
        with self.db.begin() as conn:
            conn.execute(text("DELETE FROM engines WHERE engine_id = :engine_id").bindparams(engine_id=engine_id))

    def list_games(self) -> List[Game]:
        raise NotImplementedError()

    def store_game(self, game: Game):
        raise NotImplementedError()

    def get_game(self, uuid: str) -> Game:
        raise NotImplementedError()

    def games_for_engine(self, uuid: str) -> List[Game]:
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


if __name__ == "__main__":
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    storage = SqlStorage(engine)
    storage.store_engine(Engine("stockfish", "main", "11", "andrijdavid/stockfish:11"))
    storage.store_engine(Engine("stockfish", "main", "12", "andrijdavid/stockfish:12"))
    print(storage.list_engines())
    storage.delete_engine("stockfish#main#11")
    print(storage.list_engines())
