import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import functools
import logging
import os
import time
from typing import cast, Any, Dict, Optional, TypeVar

import boto3
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ecs.client import ECSClient
from mypy_boto3_ecs.type_defs import AttachmentTypeDef
import chess

from chessnet.storage import Engine
from chessnet.runner import EngineRunner, ProtocolAdapter, TransportAdapter


AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

CHESS_ENGINE_SUBNET = "subnet-014608c03e73462f2"
CHESS_ENGINE_SECURITY_GROUP = "sg-09312f6d53bcafa4f"


log = logging.getLogger(__name__)


ReturnType = TypeVar("ReturnType")

def run_in_executor(f: Callable[..., ReturnType]) -> Callable[..., Awaitable[ReturnType]]:
    @functools.wraps(f)
    async def _async_f(*args: Any, **kwargs: Any) -> ReturnType:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: f(*args, **kwargs))
    return _async_f


@dataclass
class RunningEngine:
    task_arn: str
    ip_addr: str
    port: int


class FargateRunner(EngineRunner):
    def __init__(self, manager: FargateEngineManager, engine: Engine):
        self.manager = manager
        self._engine = engine
        self.protocol: Optional[chess.engine.UciProtocol] = None
        self.running_engine: Optional[RunningEngine] = None

    async def run(self) -> None:
        log.info("Starting container...")
        self.running_engine = await self.manager.run_engine(self._engine)
        try:
            log.info("Establishing connection...")
            _, adapter = await asyncio.get_running_loop().create_connection(
                    lambda: ProtocolAdapter(chess.engine.UciProtocol()),
                    host=self.running_engine.ip_addr,
                    port=self.running_engine.port)
            self.protocol = cast(ProtocolAdapter, adapter).protocol

            log.info("Initializing engine...")
            await self.protocol.initialize()
        except Exception as e:
            await self.shutdown(f"Error during initialization: {type(e).__name__}: {e}")
            raise

    async def play(self, board: chess.Board, limit: chess.engine.Limit) -> chess.engine.PlayResult:
        if self.protocol is None:
            raise Exception("Engine is not running")
        return await self.protocol.play(board, limit)

    async def shutdown(self, reason: str) -> None:
        await self.manager.stop_engine(self.running_engine, reason)

    def engine(self) -> Engine:
        return self._engine


class FargateEngineManager():
    TASK_DEF_VERSION = 2

    def __init__(self, cluster: str):
        self.client: ECSClient = boto3.client(
            'ecs',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self.ec2_client: EC2Client = boto3.client(
            'ec2',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self.cluster = cluster

    @run_in_executor
    def run_engine(self, engine: Engine) -> RunningEngine:
        task_def = self._get_or_create_task_definition(engine)
        task = self.client.run_task(**self._run_task_configuration(task_def))["tasks"][0]
        running_engine = self._wait_for_ready(task["taskArn"])
        return running_engine

    @run_in_executor
    def stop_engine(self, running_engine: RunningEngine, reason: str) -> None:
        self.client.stop_task(
            cluster=self.cluster,
            task=running_engine.task_arn,
            reason=reason,
        )

    def _wait_for_ready(self, task_arn: str) -> RunningEngine:
        sleeps = [0, 5, 5, 10, 10, 30, 30, 60, 60]
        for sleep in sleeps:
            time.sleep(sleep)

            response = self.client.describe_tasks(cluster=self.cluster, tasks=[task_arn])
            task = response["tasks"][0]

            status = task["lastStatus"]
            if status != "RUNNING":
                continue
            elif status == "STOPPED":
                raise Exception("Task stopped while waiting for it to start: " + task["stoppedReason"])

            eni = task["attachments"][0]
            if eni["status"] != "ATTACHED":
                continue

            eni_id = self._eni_detail(eni, "networkInterfaceId")
            if eni_id is None:
                raise Exception("Network interface ID is missing")
                continue

            eni_description = self.ec2_client.describe_network_interfaces(
                NetworkInterfaceIds=[eni_id],
            )["NetworkInterfaces"][0]

            ip_addr = eni_description["Association"]["PublicIp"]

            return RunningEngine(task_arn=task_arn, ip_addr=ip_addr, port=3333)

        raise Exception("Task took too long to start")

    def _get_or_create_task_definition(self, engine: Engine) -> str:
        task_name = self._safe_name(engine.id())
        try:
            description = self.client.describe_task_definition(taskDefinition=task_name, include=["TAGS"])
            tags = description["tags"]
            if self._version_tag() not in tags:
                # Old version of task definition, reregister.
                task_def =  self.client.register_task_definition(**self._task_definition(engine))["taskDefinition"]
            else:
                # Up to date.
                task_def = description["taskDefinition"]
        except Exception as e:
            task_def =  self.client.register_task_definition(**self._task_definition(engine))["taskDefinition"]

        log.info(task_def)
        return task_def["family"]

    def _safe_name(self, name: str) -> str:
        return name.replace("#", "_")

    def _eni_detail(self, eni: AttachmentTypeDef, name: str) -> Any:
        values = [d["value"] for d in eni["details"] if d["name"] == name]
        if len(values) == 0:
            return None
        return values[0]

    def _task_definition(self, engine: Engine) -> Dict[str, Any]:
        return {
            "family": self._safe_name(engine.id()),
            "networkMode": "awsvpc",
            "containerDefinitions": [
                {
                    "name": self._safe_name(engine.id()),
                    "image": engine.image,
                    "cpu": 2048,
                    "memory": 4096,
                    "portMappings": [
                        { "containerPort": 3333 },
                    ],
                    "essential": True,
                },
            ],
            "requiresCompatibilities": ["FARGATE"],
            "cpu": "2048",
            "memory": "4096",
            "tags": [self._version_tag()],
        }

    def _run_task_configuration(self, task_def: str) -> Dict[str, Any]:
        return {
            "cluster": self.cluster,
            "taskDefinition": task_def,
            "capacityProviderStrategy": [
                {
                    "capacityProvider": "FARGATE_SPOT",
                },
            ],
            "count": 1,
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": [CHESS_ENGINE_SUBNET],
                    "securityGroups": [CHESS_ENGINE_SECURITY_GROUP],
                    "assignPublicIp": "ENABLED",
                },
            },
        }

    def _version_tag(self) -> Dict[str, str]:
        return {
            "key": "task_definition_version",
            "value": f"v{self.TASK_DEF_VERSION}",
        }

