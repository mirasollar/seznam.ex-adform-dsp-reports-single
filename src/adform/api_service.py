import time
import logging
from typing import Dict, List, Optional, Tuple, Generator
from keboola.http_client import HttpClient
from requests.exceptions import RetryError

BASE_URL = 'https://api.adform.com'
LOGIN_URL = 'https://id.adform.com/sts/connect/token'

# endpoints
END_BUYER_STATS = 'v1/buyer/stats/data'
END_BUYER_STATS_OPERATION = "v1/buyer/stats/operations/"

DEFAULT_PAGING_LIMIT = 100000
MAX_RETRIES = 10

# wait between polls (s)
DEFAULT_WAIT_INTERVAL = 2


class AdformClientError(Exception):
    pass


class AdformServerError(Exception):
    pass


class AdformClient(HttpClient):
    """
    Basic HTTP client taking care of core HTTP communication with the API service.

    It extends the kbc.client_base.HttpClientBase class, setting up the specifics for Adform service and adding
    methods for handling pagination.

    """

    def __init__(self, token):
        super().__init__(BASE_URL,
                         max_retries=MAX_RETRIES,
                         backoff_factor=0.3,
                         status_forcelist=(429, 500, 502, 504),
                         auth_header={"Authorization": f'Bearer {str(token)}'})

    def login_using_client_credentials(self,
                                       client_id: str,
                                       client_secret: str,
                                       scope: str = 'https://api.adform.com/scope/buyer.stats'):
        params = dict(grant_type='client_credentials', client_id=client_id, client_secret=client_secret, scope=scope)
        secrets: Dict = self.post(endpoint_path=LOGIN_URL, is_absolute_path=True, data=params)  # noqa
        access_token = str(secrets.get('access_token'))
        self._auth_header = {"Authorization": f'Bearer {access_token}'}

    def _submit_stats_report(self,
                             request_filter: Dict,
                             dimensions: List,
                             metrics: List[Dict],
                             paging: Optional[Dict] = None) -> Tuple[str, str]:
        body = dict(dimensions=dimensions, filter=request_filter, metrics=metrics)
        if paging:
            body['paging'] = paging
        try:
            response = self.post_raw(endpoint_path=END_BUYER_STATS, json=body)
        except RetryError as e:
            raise AdformServerError(f"Client is unable to fetch data from server, "
                                    f"please check your AdForm API quota limits in case of error #429"
                                    f" the maximum allowed number of requests has been reached, error: {e}") from e
        if response.status_code > 299:
            raise AdformClientError(
                f"Failed to submit report. Operation failed with code {response.status_code}. Reason: {response.text}")
        logging.debug(f"submit response : {response}")
        operation_id = str(response.headers['Operation-Location'].rsplit('/', 1)[1])
        report_location_id = str(response.headers['Location'].rsplit('/', 1)[1])
        return operation_id, report_location_id

    def _wait_until_operation_finished(self, operation_id: str) -> Dict:
        continue_polling = True
        res = {}
        invalid_status = False
        start = time.time()
        while continue_polling:
            time.sleep(DEFAULT_WAIT_INTERVAL)
            req_url = "".join([self.base_url, END_BUYER_STATS_OPERATION, operation_id])
            res: Dict = self.get(req_url)  # noqa
            logging.debug(req_url)
            if "status" not in res:
                invalid_status = True
            elif res['status'] in ['succeeded', 'failed']:
                continue_polling = False
            if "status" not in res and time.time() - start > 60:
                continue_polling = False

        if invalid_status:
            raise AdformClientError("Result could not be parsed. The result should return a status, but only"
                                    f"returned {res}")
        if res['status'] == 'failed':
            raise AdformClientError(f'Report job ID "{operation_id} failed to process, please try again later."')

        return res

    def _get_report_result(self, location_id: str) -> Dict:
        endpoint = '/'.join([END_BUYER_STATS, location_id])
        return self.get(endpoint)  # noqa

    def get_report_data_paginated(self,
                                  request_filter: Dict,
                                  dimensions: List,
                                  metrics: List[Dict]) -> Generator:
        """
        Args:
            request_filter: Dict containing date range and client ids
                            e.g { "date": {"from": "2019-12-11T08:38:24.6963524Z","to": "2019-12-11T08:38:24.6963524Z"},
                                 "client": {"id": [12, 13, 14]}}
            dimensions: List containing valid dimensions
                        e.g. ["date","client","campaign"]
            metrics: List of Dicts containing valid metrics and their specs.
                     e.g.  [{"metric": "impressions","specs": {"adUniqueness": "campaignUnique"}}]

        Returns:
            res : generator - paginated results

        """
        has_more = True
        offset = 0
        while has_more:
            paging = {"offset": offset, "limit": DEFAULT_PAGING_LIMIT}
            operation_id, report_location_id = self._submit_stats_report(request_filter, dimensions, metrics, paging)
            logging.debug(f"operation_id  : {operation_id}")
            self._wait_until_operation_finished(operation_id)
            res = self._get_report_result(report_location_id)
            if len(res.get('reportData')['rows']) > 0:
                offset = len(res.get('reportData')['rows']) + offset
            else:
                has_more = False
            yield res
