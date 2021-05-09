import asyncio
from typing import List, Optional
import uuid

from pydantic.dataclasses import dataclass
from quart import Quart
from quart_schema import QuartSchema, validate_response, validate_request

import chessnet.game
from chessnet.storage import Engine, FileStorage, Game

app = Quart("ChessNET")

storage = FileStorage("./data.pickle")


@dataclass
class EnginesResponse:
    engines: List[Engine]


@app.route("/engines", methods=["GET"])
@validate_response(EnginesResponse)
async def list_engines():
    return EnginesResponse(storage.list_engines())


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
        parent=data.parent,
    )
    storage.store_engine(engine)
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
    white_engine = storage.get_engine(data.white)
    black_engine = storage.get_engine(data.black)
    asyncio.create_task(chessnet.game.play_game(white_engine, black_engine))
    return PlayGameResponse(game_id)


if __name__ == "__main__":
    app.run()
