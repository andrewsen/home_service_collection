import json
import threading
import uuid
from abc import ABC
from typing import Optional, Callable

import redis

from service.ServiceBase.message import Report, Response, Request

_CONTROLLER_CHANEL = 'service:controller'


class ServiceBase(ABC):
    """
    Abstract class containing communication logic with the smart home controller
    """
    def __init__(self):
        self._service_channel = None
        self._redis_host = 'localhost'
        self._redis_port = 6379
        self._redis_client: Optional[redis.Redis] = None
        self._request_handlers = {}
        self._report_handlers = {}
        self._service_name = 'ServiceBase'
        self._awaiting_requests = {}

    def registered(self, status: Response) -> None:
        """
        Registration status handler. Is called when the controller registers the service

        :param status: registration status
        """
        pass

    def shutdown(self) -> None:
        """
        Shutdown handler. Is called when the controller sends shutdown request.
        After executing this handler, the service will stop request handling and shutdown
        """
        pass

    def load_config(self, config_file: Optional[str]=None) -> None:
        """
        Loads config.yaml and initialize the service.
        If config_file parameter is not specified, tries to find config.yaml in the root service directory

        :param config_file: optional path to YAML config of the service
        """
        raise Exception("Not implemented")

    def configure(self, config: dict) -> None:
        """
        Configures the service. Currently, accepts parameters: name, host, port; where:
        name: str, name of the service, under which it will be registered by the controller
        host: str, IP of the controller
        port: int, port to connect to the controller

        :param config: dict with configuration parameters: {
            "name": str,
            "host": str,
            "port": int
        }
        """
        if 'name' in config:
            self._service_name = config['name']
        if 'host' in config:
            self._redis_host = config['host']
        if 'port' in config:
            self._redis_port = config['port']
        self._service_channel = f'service:{self._service_name}'

    def run(self) -> threading.Thread:
        """
        Registers and starts the service in the separate thread
        :return: Service thread
        """
        self._redis_client = redis.Redis(host=self._redis_host, port=self._redis_port)
        redis_subscriber = self._redis_client.pubsub()
        redis_subscriber.subscribe(**{
            self._service_channel: self._message_handler,
        })

        self._register_handler('service', 'shutdown', False, lambda _: self.shutdown())

        thread = redis_subscriber.run_in_thread(sleep_time=0.001)

        self._register_service()

        return thread

    def report(self, topic: str, name: str, payload: any) -> None:
        """
        Sends report to the specified topic. Example:
        report(topic="monitoring", name="temperature", data=22.5)

        :param topic: Topic where the report is sent
        :param name: Name of the report
        :param payload: Arbitrary data to send
        """
        report = {
            'type': 'report',
            'topic': topic,
            'name': name,
            'origin': self._service_name,
            'data': payload,
        }
        self._redis_client.publish(_CONTROLLER_CHANEL, json.dumps(report))

    def request(self, topic: str, name: str, handler: Callable[[Response], None], payload: any=None) -> None:
        """
        Sends request tto the specified topic and expects a response. Example:
        report(topic="monitoring", name="get_temperature", handler=handle_temperature)

        :param topic: Topic where the request is sent
        :param name: Name of the request
        :param handler: Request handler
        :param payload: Arbitrary payload to send
        :return:
        """
        request = {
            'id': uuid.uuid4(),
            'type': 'report',
            'topic': topic,
            'name': name,
            'origin': self._service_name,
        }
        if payload is not None:
            request['data'] = payload
        self._awaiting_requests[request['id']] = handler
        self._redis_client.publish(_CONTROLLER_CHANEL, json.dumps(request))

    def _message_handler(self, redis_message: dict) -> None:
        if redis_message['type'] == 'message':
            message = json.loads(redis_message['data'].decode())
            if message['type'] == 'request':
                request = Request(message)
                response = self._handle_request(request)
                if response is not None:
                    self._redis_client.publish(self._service_channel, json.dumps(response))
            elif message['type'] == 'report':
                report = Report(message)
                self._handle_report(report)
            elif message['type'] == 'response':
                response = Response(message)
                handler = self._awaiting_requests.get(response.id)
                if handler is not None:
                    handler(response)
                    del self._awaiting_requests[response.id]

    def _handle_report(self, report: Report) -> None:
        handler = self._find_handler(report.topic, report.name, self._report_handlers)
        if handler is not None:
            handler(report)

    def _handle_request(self, request: Request) -> Optional[dict]:
        handler = self._find_handler(request.topic, request.name, self._request_handlers)
        if handler is None:
            return None
        payload = handler(request)
        response = {
                'id': request.id,
                'type': 'response',
                'topic': request.topic,
                'name': request.name,
                'origin': self._service_name,
        }
        if payload is not None:
            response['data'] = payload
        return response

    def _find_handler(self, topic: str, name: str, handlers: dict) -> Optional[Callable]:
        if topic not in handlers:
            return None
        return handlers[topic].get(name)

    def _register_handler(self, topic: str, name: str, is_request: bool, handler: Callable) -> None:
        dest = self._request_handlers if is_request else self._report_handlers
        if topic in dest:
            dest[topic][name] = handler
        else:
            dest[topic] = { name: handler }

    def _register_service(self) -> None:
        registration_data = {
            'name': self._service_name,
            'channel': self._service_channel,
            'requests': [{'topic': t, 'names': self._request_handlers[t].keys() }
                         for t in self._request_handlers.keys()],
            'reports': [{'topic': t, 'names': self._report_handlers[t].keys() }
                         for t in self._report_handlers.keys()]
        }
        self.request('system', 'register', self.registered, registration_data)


def _register_handler(topic: str, name: str, is_request: bool) -> Callable:
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        service = func.__self__
        if service is not None and issubclass(service, ServiceBase):
            service._register_handler(topic, name, is_request, func)
        return wrapper
    return decorator

def on_request(topic: str, name: str) -> Callable:
    return _register_handler(topic, name, True)

def on_report(topic: str, name: str) -> Callable:
    return _register_handler(topic, name, False)