import logging
import logging_gelf.handlers
import logging_gelf.formatters
import os
import json
import datetime  # noqa
import dateparser
import requests

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from google_my_business import GoogleMyBusiness


# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'
KEY_ENDPOINTS = 'endpoints'

MANDATORY_PARS = [KEY_ENDPOINTS, KEY_API_TOKEN]


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        params = self.configuration.parameters

        authorization = self.configuration.config_data["authorization"]
        oauth_token = self.get_oauth_token(authorization)

        endpoints = params['endpoints']
        logging.info(f"Component will process following endpoints: {endpoints}")
        if not endpoints:
            raise UserException('Please select an endpoint.')

        # Validating input date parameters
        start_date_str = params['request_range'].get('start_date', '7 days ago')
        end_date_str = params['request_range'].get('end_date', 'today')
        start_date_form = dateparser.parse(start_date_str)
        end_date_form = dateparser.parse(end_date_str)
        if start_date_form > end_date_form:
            raise UserException('Start Date cannot exceed End Date. Please re-enter [Request Range].')
        start_date_str = start_date_form.strftime('%Y-%m-%dT00:00:00.000000Z')
        end_date_str = end_date_form.strftime('%Y-%m-%dT00:00:00.000000Z')
        logging.info('Request Range: {} to {}'.format(start_date_str, end_date_str))

        gmb = GoogleMyBusiness(
            access_token=oauth_token,
            start_timestamp=start_date_str,
            end_timestamp=end_date_str,
            data_folder_path=self.data_folder_path)
        gmb.run(endpoints=endpoints)

        logging.info("Extraction finished")

    @staticmethod
    def get_oauth_token(config):
        data = config['oauth_api']['credentials']
        data_encrypted = json.loads(
            config['oauth_api']['credentials']['#data'])
        client_id = data['appKey']
        client_secret = data['#appSecret']
        refresh_token = data_encrypted['refresh_token']

        url = 'https://www.googleapis.com/oauth2/v4/token'
        header = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
        }

        response = requests.post(
            url=url, headers=header, data=payload)

        if response.status_code != 200:
            raise UserException(f"Unable to refresh access token. "
                                f"Please reset the account authorization: {response.text}")

        data_r = response.json()
        return data_r["access_token"]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
