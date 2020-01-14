'''
Template Component main class.

'''

import logging
import logging_gelf.handlers
import logging_gelf.formatters
import sys
import os
import json
import datetime  # noqa
import dateparser
import requests

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa

from google_my_business import Google_My_Business  # noqa


# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'
KEY_ENDPOINTS = 'endpoints'

MANDATORY_PARS = [KEY_ENDPOINTS, KEY_API_TOKEN]
MANDATORY_IMAGE_PARS = []

# Default Table Output Destination
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_FILE_SOURCE = "/data/in/files/"

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

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        """
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True
        else:
            debug = False

        self.set_default_logger('DEBUG' if debug else 'INFO')
        """
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def get_oauth_token(self, config):
        '''
        Extracting Oauth Token out of Authorization
        '''
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
            logging.error(
                "Unable to refresh access token. Please reset the account authorization.")
            sys.exit(1)

        data_r = response.json()
        token = data_r["access_token"]

        return token

    def run(self):
        '''
        Main execution code
        '''

        # Activate when OAuth in KBC is ready
        # Get Authorization Token
        authorization = self.configuration.get_authorization()
        oauth_token = self.get_oauth_token(authorization)
        # logging.info(oauth_token)

        # Configuration Parameters
        params = self.cfg_params  # noqa
        endpoints = params['endpoints']

        # Validating input date parameters
        request_range = params['request_range']
        start_date_form = dateparser.parse(request_range['start_date'])
        end_date_form = dateparser.parse(request_range['end_date'])
        if start_date_form == '':
            start_date_form = '7 days ago'
        if end_date_form == '':
            end_date_form = 'today'
        day_diff = (end_date_form-start_date_form).days
        if day_diff < 0:
            logging.error(
                'Start Date cannot exceed End Date. Please re-enter [Request Range].')
            sys.exit(1)

        start_date_str = start_date_form.strftime(
            '%Y-%m-%d')+'T00:00:00.000000Z'
        end_date_str = end_date_form.strftime('%Y-%m-%d')+'T00:00:00.000000Z'
        logging.info('Request Range: {} to {}'.format(
            start_date_str, end_date_str))

        # If no endpoints are selected
        if len(endpoints) == 0:
            logging.error('Please select an endpoint.')
            sys.exit(1)

        all_endpoints = []
        for i in endpoints:
            if i['endpoint'] not in all_endpoints:
                all_endpoints.append(i['endpoint'])

        gmb = Google_My_Business(
            access_token=oauth_token,
            start_timestamp=start_date_str,
            end_timestamp=end_date_str)
        gmb.run(endpoints=all_endpoints)

        logging.info("Extraction finished")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()
