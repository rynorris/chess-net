import chess
import docker

from chessnet.runner import DockerFileRunner, EngineRunner
from chessnet.storage import Engine


async def play_game(white: Engine, black: Engine) -> chess.Outcome:
    board = chess.Board()
    print("Connecting to docker daemon...")
    client = docker.from_env()

    print("Starting white...")
    white_runner = DockerFileRunner(client, white.image, 3333)
    await white_runner.run()

    print("Starting black...")
    black_runner = DockerFileRunner(client, black.image, 3334)
    await black_runner.run()

    try:
        print(board)
        while True:
            print("Requesting white move...")
            res = await white_runner.play(board, chess.engine.Limit(time=0.1))
            print(f"Got move: {res.move}")

            board.push(res.move)
            print(board)

            outcome = board.outcome()
            if outcome is not None:
                print("Game over:", outcome.result())
                break

            print("Requesting black move...")
            res = await black_runner.play(board, chess.engine.Limit(time=0.1))
            print(f"Got move: {res.move}")

            board.push(res.move)
            print(board)

            outcome = board.outcome()
            if outcome is not None:
                print("Game over:", outcome.result())
                break
    finally:
        white_runner.shutdown()
        black_runner.shutdown()

    return board.outcome()
