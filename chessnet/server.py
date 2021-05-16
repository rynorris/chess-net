import asyncio
import logging
from typing import List, Optional
import uuid

from pydantic.dataclasses import dataclass
from quart import Quart
from quart_schema import QuartSchema, validate_response, validate_request
from sqlalchemy.ext.asyncio import create_async_engine

import chessnet.game
from chessnet.fargate import FargateEngineManager, FargateRunner
from chessnet.storage import Engine, Game, Move
from chessnet.sql import SqlStorage

app = Quart("ChessNET")

engine = create_async_engine("sqlite+aiosqlite:///data.sqlite", echo=False, future=True)
fargate = FargateEngineManager("chess-net")
storage = SqlStorage(engine)


@app.before_serving
async def initialize_db():
    await storage.initialize()


@dataclass
class EnginesResponse:
    engines: List[Engine]


@app.route("/engines", methods=["GET"])
@validate_response(EnginesResponse)
async def list_engines():
    return EnginesResponse(await storage.list_engines())


@dataclass
class RegisterEngineRequest:
    family: str
    variant: str
    version: str
    image: str


@app.route("/engines", methods=["POST"])
@validate_request(RegisterEngineRequest)
@validate_response(Engine)
async def register_engine(data: RegisterEngineRequest) -> Engine:
    engine_id = str(uuid.uuid4())
    engine = Engine(
        family=data.family,
        variant=data.variant,
        version=data.version,
        image=data.image,
    )
    await storage.store_engine(engine)
    return engine


@dataclass
class PlayGameRequest:
    white: str
    black: str


@dataclass
class PlayGameResponse:
    game_id: str


@app.route("/play", methods=["POST"])
@validate_request(PlayGameRequest)
@validate_response(PlayGameResponse)
async def play_game(data: PlayGameRequest) -> PlayGameResponse:
    game_id = str(uuid.uuid4())
    white, black = await asyncio.gather(storage.get_engine(data.white), storage.get_engine(data.black))

    async def _play():
        white_runner = FargateRunner(fargate, white)
        black_runner = FargateRunner(fargate, black)
        game = Game(
            game_id=game_id,
            timestamp=0,
            white=data.white,
            black=data.black,
            outcome=None,
        )
        await storage.store_game(game)
        outcome = await chessnet.game.play_game(white_runner, black_runner)
        await storage.finish_game(game_id, outcome.result())

    # Explicitly kick this off asynchronously and just return the ID.
    asyncio.create_task(_play())

    return PlayGameResponse(game_id)


@dataclass
class GamesResponse:
    games: List[Game]


@app.route("/games", methods=["GET"])
@validate_response(GamesResponse)
async def list_games():
    return GamesResponse(await storage.list_games())


@app.route("/games/<game_id>", methods=["GET"])
@validate_response(Game)
async def get_game(game_id):
    return await storage.get_game(game_id)


@dataclass
class MovesResponse:
    moves: List[Move]


@app.route("/games/<game_id>/moves", methods=["GET"])
@validate_response(MovesResponse)
async def get_game_moves(game_id):
    return MovesResponse(await storage.moves_in_game(game_id))


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    app.run()
