import asyncio
import logging

import chess
import docker

from chessnet.runner import EngineRunner


log = logging.getLogger(__name__)


async def play_game(white: EngineRunner, black: EngineRunner) -> chess.Outcome:
    board = chess.Board()

    log.info("Starting engines...")
    await asyncio.gather(white.run(), black.run())

    try:
        log.info(board)
        while True:
            log.info("Requesting white move...")
            res = await white.play(board, chess.engine.Limit(time=0.1))
            log.info(f"Got move: {res.move}")

            board.push(res.move)
            log.info(board)

            outcome = board.outcome()
            if outcome is not None:
                log.info("Game over:", outcome.result())
                break

            log.info("Requesting black move...")
            res = await black.play(board, chess.engine.Limit(time=0.1))
            log.info(f"Got move: {res.move}")

            board.push(res.move)
            log.info("\n" + str(board))

            outcome = board.outcome()
            if outcome is not None:
                log.info(f"Game over: {outcome.result()}")
                break
    finally:
        await asyncio.gather(white.shutdown(), black.shutdown())

    return board.outcome()
