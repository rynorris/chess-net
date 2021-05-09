import asyncio
import random

import chess.engine

from chessnet.runner import play_game
from chessnet.storage import Engine, FileStorage


async def main():
    storage = FileStorage("./data.json")

    # storage.store_engine(Engine("a", "stockfish", "master", "11", None, "andrijdavid/stockfish:11"))
    # storage.store_engine(Engine("b", "stockfish", "master", "12", "a", "andrijdavid/stockfish:12"))

    white = random.choice(storage.list_engines())
    black = random.choice(storage.list_engines())

    print(f"Playing {white} against {black}")
    await play_game(white, black)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(chess.engine.EventLoopPolicy())
    asyncio.run(main())
