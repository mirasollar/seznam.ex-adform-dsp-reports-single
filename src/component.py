import csv
import logging
import os
import dateparser
import warnings
from datetime import datetime
from typing import Dict, Tuple, List, Optional

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from adform.api_service import AdformClient
from adform.api_service import AdformClientError, AdformServerError

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)

# configuration variables
KEY_API_TOKEN = '#api_secret'
KEY_API_CLIENT_ID = 'api_client_id'

KEY_RESULT_FILE = 'result_file_name'
KEY_FILTER = 'filter'
KEY_DATE_RANGE = 'date_range'
KEY_DATE_FROM = 'from_date'
KEY_DATE_TO = 'to_date'
KEY_CLIENT_IDS = 'client_ids'

KEY_DIMENSIONS = 'dimensions'

KEY_METRICS = 'metrics'
KEY_METRIC_NAME = 'metric'
KEY_METRIC_SPEC = 'specs_metadata'
KEY_METRIC_SPEC_KEY = 'key'
KEY_METRIC_SPEC_VALUE = 'value'

REQUIRED_PARAMETERS = [KEY_FILTER, KEY_DIMENSIONS, KEY_METRICS, KEY_RESULT_FILE]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

    def run(self) -> None:
        params = self.configuration.parameters

        client = self.init_client()

        logging.info('Building report request..')
        dimensions = params.get(KEY_DIMENSIONS)
        logging.info('Building metrics..')
        metric_definitions = build_metrics(params.get(KEY_METRICS))
        filters = params[KEY_FILTER]
        date_range = filters[KEY_DATE_RANGE]

        logging.info('Getting report period..')
        start_date, end_date = get_date_period_converted(date_range[KEY_DATE_FROM], date_range[KEY_DATE_TO])

        logging.info('Constructing filter..')
        filter_def = build_filter_def(start_date, end_date, filters.get(KEY_CLIENT_IDS))
        logging.info(f'Submitting report with parameters: filter: {params[KEY_FILTER]}, '
                     f'dimensions={dimensions}, metrics:{metric_definitions}')
        logging.info('Collecting report result..')

        result_file_name = params[KEY_RESULT_FILE]
        incremental = params.get('incremental_output', True)
        table_def = self.create_out_table_definition(result_file_name, primary_key=dimensions, incremental=incremental)
        try:
            for res in client.get_report_data(filter_def, dimensions, metric_definitions):
                logging.info('Storing results')
                self.store_results(res, table_def.full_path)
        except AdformClientError as client_exception:
            raise UserException(client_exception) from client_exception
        except AdformServerError as server_exception:
            raise UserException(server_exception) from server_exception
        self.write_manifest(table_def)

        logging.info('Extraction finished successfully!')

    @staticmethod
    def store_results(report_result: Dict, file_path: str) -> None:
        mode = 'a' if os.path.exists(file_path) else 'w+'
        columns = report_result['reportData']['columnHeaders']
        with open(file_path, mode, newline='', encoding='utf-8') as out:
            writer = csv.writer(out)
            if mode == 'w+':
                writer.writerow(columns)
            writer.writerows(report_result['reportData']['rows'])

    @staticmethod
    def init_client_with_api_token(api_token: str, api_client_id: str) -> AdformClient:
        try:
            client = AdformClient('')
            client.login_using_client_credentials(api_client_id, api_token)
        except Exception as ex:
            raise UserException(f'Login failed, please check your credentials! {str(ex)}') from ex

        return client

    @staticmethod
    def init_client_with_access_token(access_token: str) -> AdformClient:
        try:
            client = AdformClient(access_token)
        except Exception as ex:
            raise UserException(f'Login failed, please check your credentials! {str(ex)}') from ex

        return client

    def init_client(self) -> AdformClient:
        params = self.configuration.parameters
        api_token = params.get(KEY_API_TOKEN)
        api_client_id = params.get(KEY_API_CLIENT_ID)
        if api_token and api_client_id:
            return self.init_client_with_api_token(api_token, api_client_id)
        auth = self.configuration.oauth_credentials.data
        access_token = auth.get('access_token')
        return self.init_client_with_access_token(access_token)


def build_metrics(metrics_cfg: List[Dict]) -> List[Dict]:
    metric_definitions = []
    for m in metrics_cfg:
        metric_def = {"metric": m[KEY_METRIC_NAME],
                      "specs": build_specs(m[KEY_METRIC_SPEC])}
        metric_definitions.append(metric_def)
    return metric_definitions


def build_specs(spec_metadata: List[Dict]) -> Dict:
    return {s[KEY_METRIC_SPEC_KEY]: s[KEY_METRIC_SPEC_VALUE] for s in spec_metadata}


def build_filter_def(start_date: datetime, end_date: datetime, client_ids: Optional[List]) -> Dict:
    filter_def = {'date': {"from": start_date.strftime('%Y-%m-%d'), "to": end_date.strftime('%Y-%m-%d')}}
    if client_ids:
        filter_def['client'] = {"id": client_ids}
    return filter_def


def get_date_period_converted(period_from: str, period_to: str) -> Tuple[datetime, datetime]:
    """
    Returns given period parameters in datetime format, or next step in back-fill mode
    along with generated last state for next iteration.

    :param period_from: str YYYY-MM-DD or relative string supported by date parser e.g. 5 days ago
    :param period_to: str YYYY-MM-DD or relative string supported by date parser e.g. 5 days ago

    :return: start_date: datetime, end_date: datetime
    """

    start_date_form = dateparser.parse(period_from)
    end_date_form = dateparser.parse(period_to)
    if not start_date_form or not end_date_form:
        raise UserException("Error with dates, make sure both start and end date are defined properly")
    day_diff = (end_date_form - start_date_form).days
    if day_diff < 0:
        raise UserException("start_date cannot exceed end_date.")

    return start_date_form, end_date_form


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
