import abc
import json
import logging

from redis import Redis
from utils import EnhancedJSONEncoder


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: dict) -> None:
        self.redis_adapter.set(
            "start_from_ts", json.dumps(state, cls=EnhancedJSONEncoder)
        )

    def retrieve_state(self) -> dict:
        raw_data = self.redis_adapter.get("start_from_ts")
        if raw_data is None:
            logging.info("No state file provided. Continue with in-memory state")
            return {}
        return json.loads(raw_data)
