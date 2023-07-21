from abc import ABC
from typing import Optional


class Message(ABC):
    def __init__(self, raw_response: dict):
        self._id = raw_response["id"]
        self._origin = raw_response["origin"]
        self._topic = raw_response["topic"]
        self._name = raw_response["name"]
        self._data = raw_response["data"]

    @property
    def id(self) -> str:
        return self._id

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> Optional[any]:
        return self._data


class Report(Message):
    pass


class Request(Message):
    pass


class Response(Message):
    def __init__(self, raw_response: dict):
        super().__init__(raw_response)
        self._status = int(raw_response["status"])
        self._error = raw_response.get("error")

    @property
    def status(self) -> int:
        return self._status

    @property
    def error(self) -> Optional[str]:
        return self._error