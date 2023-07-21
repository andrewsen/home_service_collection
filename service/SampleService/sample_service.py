from service.ServiceBase.message import Response, Request, Report
from service.ServiceBase.service_base import ServiceBase, on_request, on_report


class SampleService(ServiceBase):
    def __init__(self):
        super().__init__()
        self.configure({
          'name': SampleService,
        })

    def registered(self, status: Response) -> None:
        print('Service registered')
        self.report('system', 'status', 'I am alive')
        self.request('system', 'get_random_number', self._random_handler)

    def _random_handler(self, response: Response):
        print(f'Random number is {response["value"]}')

    @on_request('sample', 'echo')
    def on_echo(self, request: Request):
        print(f'Echoing {request.data["message"]}')
        return {
            'echo': request.data['message']
        }

    @on_report('system', 'heartbeat')
    def on_heartbeat(self, report: Report):
        print(f'Heartbeat from {report.origin}')

    def shutdown(self) -> None:
        print('Service shutting down')
        self.report('chat', 'I am dying')


if __name__ == '__main__':
    service = SampleService()
    service.run()
