import asyncio
import logging
from typing import List, Optional
import uuid
import sys

from pydantic.dataclasses import dataclass
from quart import Quart
from quart_schema import QuartSchema, validate_response, validate_request
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

import chessnet.game
from chessnet.events import Broker, Event, Events, EventType
from chessnet.fargate import FargateEngineManager, FargateRunner
from chessnet.storage import Engine, Game, Move
from chessnet.sql import SqlStorage

app = Quart("ChessNET")

engine = create_async_engine("sqlite+aiosqlite:///data.sqlite", echo=False, future=True)
fargate = FargateEngineManager("chess-net")
storage = SqlStorage(engine)
broker = Broker()


async def store_event(event: Event) -> None:
    if event.typ == EventType.START_GAME:
        await storage.store_game(event.game)
    elif event.typ == EventType.END_GAME:
        await storage.finish_game(event.game_id, event.outcome)
    elif event.typ == EventType.MAKE_MOVE:
        await storage.store_move(event.game_id, event.move, event.fen_before, event.engine_id)


broker.subscribe("*", [EventType.START_GAME, EventType.END_GAME, EventType.MAKE_MOVE], store_event)


@app.before_serving
async def initialize_db() -> None:
    await storage.initialize()


@dataclass
class EnginesResponse:
    engines: List[Engine]


@app.route("/engines", methods=["GET"])
@validate_response(EnginesResponse)  # type: ignore
async def list_engines() -> EnginesResponse:
    return EnginesResponse(await storage.list_engines())


@dataclass(frozen=True)
class RegisterEngineRequest:
    family: str
    variant: str
    version: str
    image: str


@app.route("/engines", methods=["POST"])
@validate_request(RegisterEngineRequest)  # type: ignore
@validate_response(Engine)  # type: ignore
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
@validate_request(PlayGameRequest)  # type: ignore
@validate_response(PlayGameResponse)  # type: ignore
async def play_game(data: PlayGameRequest) -> PlayGameResponse:
    game_id = str(uuid.uuid4())
    white, black = await asyncio.gather(storage.get_engine(data.white), storage.get_engine(data.black))

    async def _play() -> None:
        #import docker
        #from chessnet.runner import DockerFileRunner
        #client = docker.from_env()
        #white_runner = DockerFileRunner(client, white, 3333)
        #black_runner = DockerFileRunner(client, black, 3334)
        white_runner = FargateRunner(fargate, white)
        black_runner = FargateRunner(fargate, black)
        game = Game(
            game_id=game_id,
            timestamp=0,
            white=data.white,
            black=data.black,
            outcome=None,
        )
        broker.publish(game_id, Events.start_game(game))
        outcome = await chessnet.game.play_game(broker, game_id, white_runner, black_runner)
        if outcome is None:
            raise Exception("No outcome from game")
        broker.publish(game_id, Events.end_game(game_id, outcome.result()))

    # Explicitly kick this off asynchronously and just return the ID.
    asyncio.create_task(_play())

    return PlayGameResponse(game_id)


@dataclass
class GamesResponse:
    games: List[Game]


@app.route("/games", methods=["GET"])
@validate_response(GamesResponse)  # type: ignore
async def list_games() -> GamesResponse:
    return GamesResponse(await storage.list_games())


@app.route("/games/<game_id>", methods=["GET"])
@validate_response(Game)  # type: ignore
async def get_game(game_id: str) -> Game:
    return await storage.get_game(game_id)


@dataclass
class MovesResponse:
    moves: List[Move]


@app.route("/games/<game_id>/moves", methods=["GET"])
@validate_response(MovesResponse)  # type: ignore
async def get_game_moves(game_id: str) -> MovesResponse:
    return MovesResponse(await storage.moves_in_game(game_id))

