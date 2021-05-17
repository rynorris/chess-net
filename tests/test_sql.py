import pytest

from sqlalchemy.ext.asyncio import create_async_engine

from chessnet.storage import Engine, Game, Move, Storage
from chessnet.sql import SqlStorage

stockfish = Engine("stockfish", "main", "11", "andrijdavid/stockfish:11")
alphazero = Engine("alphazero", "main", "1.0.0", "deepmind/alphazero:latest")

game_1 = Game("1", 100, stockfish.id(), alphazero.id(), None)
game_1_outcome = "1-0"
game_1_finished = Game("1", 100, stockfish.id(), alphazero.id(), game_1_outcome)

move_1 = Move("e2e4", 101)
move_2 = Move("e7e5", 102)


@pytest.fixture
async def storage() -> Storage:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True, future=True)
    storage = SqlStorage(engine)
    await storage.initialize()
    return storage


@pytest.mark.asyncio
async def test_engines(storage: Storage) -> None:
    engines = await storage.list_engines()
    assert engines == []

    await storage.store_engine(stockfish)
    engines = await storage.list_engines()
    assert engines == [stockfish]

    await storage.store_engine(alphazero)
    engines = await storage.list_engines()
    assert set(engines) == set([stockfish, alphazero])

    await storage.delete_engine(stockfish.id())
    engines = await storage.list_engines()
    assert set(engines) == set([alphazero])

    engine = await storage.get_engine(alphazero.id())
    assert engine == alphazero


@pytest.mark.asyncio
async def test_games(storage: Storage) -> None:
    await storage.store_engine(stockfish)
    await storage.store_engine(alphazero)

    await storage.store_game(game_1)

    games = await storage.list_games()
    assert games == [game_1]

    await storage.finish_game(game_1.game_id, game_1_outcome)
    game = await storage.get_game(game_1.game_id)
    assert game == game_1_finished


@pytest.mark.asyncio
async def test_moves(storage: Storage) -> None:
    await storage.store_engine(stockfish)
    await storage.store_engine(alphazero)
    await storage.store_game(game_1)

    moves = await storage.moves_in_game(game_1.game_id)
    assert moves == []

    await storage.store_move(game_1.game_id, move_1, "<fen>", stockfish.id())
    await storage.store_move(game_1.game_id, move_2, "<fen>", alphazero.id())

    moves = await storage.moves_in_game(game_1.game_id)
    assert moves == [move_1, move_2]

