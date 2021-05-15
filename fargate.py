import boto3
import os

from chessnet.runner import EngineRunner
from chessnet.storage import FileStorage


AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

CHESS_ENGINE_SUBNET = "subnet-014608c03e73462f2"
CHESS_ENGINE_SECURITY_GROUP = "sg-09312f6d53bcafa4f"


class FargateRunner(EngineRunner):
    def __init__(self, engine):
        self.engine = engine


class FargateContainerManager():
    TASK_DEF_VERSION = 2

    def __init__(self, cluster):
        self.client = boto3.client(
            'ecs',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self.cluster = cluster

    def run_engine(self, engine):
        task_def = self.get_or_create_task_definition(engine)
        task = self.client.run_task(**self._run_task_configuration(task_def))
        return task

    def get_or_create_task_definition(self, engine):
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

        print(task_def)
        return task_def["family"]

    def _safe_name(self, name):
        return name.replace("#", "_")

    def _task_definition(self, engine):
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

    def _run_task_configuration(self, task_def):
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

    def _version_tag(self):
        return {
            "key": "task_definition_version",
            "value": f"v{self.TASK_DEF_VERSION}",
        }


if __name__ == "__main__":
    storage = FileStorage("./data.pickle")
    manager = FargateContainerManager("chess-net")
    #print(manager.get_or_create_task_definition(storage.get_engine("stockfish#main#11")))
    print(manager.run_engine(storage.get_engine("stockfish#main#11")))

