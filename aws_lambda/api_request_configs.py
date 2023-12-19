from api_pull.utils import PetfinderApiRequest, FredApiRequest


class PetfinderApiRequests:
    _instance = None
    _pf_request_1 = PetfinderApiRequest(name='all_dogs',
                                        category='animal',
                                        parameters={
                                            'type': 'dog'
                                        })
    _pf_request_2 = PetfinderApiRequest(name='all_cats',
                                        category='animal',
                                        parameters={
                                            'type': 'cat'
                                        })
    _requests = [_pf_request_1, _pf_request_2]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PetfinderApiRequests, cls).__new__(cls)
        return cls._instance

    def update_api_requests(cls, metadata):
        for request in cls._requests:
            request_name = request.name
            request_last_updated = metadata[request_name]['last_updated']
            request.add_parameter(parameter_name='after',
                                  parameter_value=request_last_updated)

    @classmethod
    def get_petfinder_api_requests(cls, metadata: dict):
        for request in requests:
            request_name = request.name
            request_last_updated = metadata[request_name]['last_updated']
            request.add_parameter(parameter_name=)

        requests = [cls._pf_request_1, cls._pf_request_2]
        return requests

    @staticmethod
    def get_fred_api_requests(real_time_start):

        fred_request_1


class FredApiRequests:
    series_ids = ['GDP', 'RSXFS', 'UNRATE', 'CPALTT01USM657N', 'DFF']

    @staticmethod
    def get_api_requests():
        fred_request_1 = FredApiRequest(series_id='')
