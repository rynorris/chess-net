import asyncio
import logging
import random
import sys

import chess.engine

from chessnet.fargate import FargateEngineManager, FargateRunner
from chessnet.game import play_game
from chessnet.storage import Engine, FileStorage

log = logging.getLogger(__name__)


async def main():
    storage = FileStorage("./data.pickle")

    # storage.store_engine(Engine("a", "stockfish", "master", "11", None, "andrijdavid/stockfish:11"))
    # storage.store_engine(Engine("b", "stockfish", "master", "12", "a", "andrijdavid/stockfish:12"))
    white = random.choice(storage.list_engines())
    black = random.choice(storage.list_engines())

    log.info("Connecting to docker daemon...")
    manager = FargateEngineManager("chess-net")

    white_runner = FargateRunner(manager, white)
    black_runner = FargateRunner(manager, black)


    log.info(f"Playing {white} against {black}")
    await play_game(white_runner, black_runner)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    asyncio.set_event_loop_policy(chess.engine.EventLoopPolicy())
    asyncio.run(main())
