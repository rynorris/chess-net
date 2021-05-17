import asyncio
import logging
from typing import Optional

import chess

from chessnet.events import Broker, Events
from chessnet.runner import EngineRunner
from chessnet.storage import Move


log = logging.getLogger(__name__)


async def play_game(broker: Broker, game_id: str, white: EngineRunner, black: EngineRunner) -> Optional[chess.Outcome]:
    board = chess.Board()


    log.info("Starting engines...")
    await asyncio.gather(white.run(), black.run())

    try:
        log.info(board)
        while True:
            log.info("Requesting white move...")
            res = await white.play(board, chess.engine.Limit(time=0.1))
            log.info(f"Got move: {res.move}")

            if res.move is not None:
                broker.publish(game_id, Events.make_move(
                    game_id,
                    Move(res.move.uci(), 0),
                    board.fen(),
                    white.engine().id(),
                ))

                board.push(res.move)
                log.info("\n" + str(board))

            outcome = board.outcome()
            if outcome is not None:
                log.info("Game over:", outcome.result())
                break

            log.info("Requesting black move...")
            res = await black.play(board, chess.engine.Limit(time=0.1))
            log.info(f"Got move: {res.move}")

            if res.move is not None:
                broker.publish(game_id, Events.make_move(
                    game_id,
                    Move(res.move.uci(), 0),
                    board.fen(),
                    white.engine().id(),
                ))
                board.push(res.move)
                log.info("\n" + str(board))

            outcome = board.outcome()
            if outcome is not None:
                log.info(f"Game over: {outcome.result()}")
                break

        reason = "Game finished successfully"
        await asyncio.gather(white.shutdown(reason), black.shutdown(reason))
    except Exception as e:
        reason = f"Game aborted due to error: {type(e).__name__}: {e}"
        await asyncio.gather(white.shutdown(reason), black.shutdown(reason))

    return board.outcome()
