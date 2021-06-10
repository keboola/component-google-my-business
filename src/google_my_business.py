import sys
import os
import json  # noqa
import requests
import logging
import logging_gelf.handlers
import logging_gelf.formatters

import parser


if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])

# Request Parameters
requesting = requests.Session()


class Google_My_Business():
    '''
    Google My Business Request Handler
    '''

    def __init__(self, access_token, start_timestamp, end_timestamp):
        self.access_token = access_token
        self.base_url = 'https://mybusiness.googleapis.com/v4'
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        # self.get_output_columns()

    def get_output_columns(self):
        '''
        Getting all the column names from the mapping.json
        '''

        with open('src/mapping.json', 'r') as f:
            self.output_columns = json.load(f)

    def get_request(self, url, headers=None, params=None):
        '''
        Base GET request
        '''

        res = requesting.get(url=url, headers=headers, params=params)

        return res.status_code, res

    def post_request(self, url, headers=None, payload=None):
        '''
        Base POST request
        '''

        res = requesting.post(url=url, headers=headers, json=payload)

        return res.status_code, res

    def list_accounts(self, nextPageToken=None):
        '''
        Fetching all the accounts available in the authorized Google account
        '''

        # Accounts parameters
        out_account_list = []
        account_url = '{}/accounts'.format(self.base_url)

        params = {
            'access_token': self.access_token
            # 'Authorization': 'Bearer {}'.format(self.access_token)
        }

        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Account Lists
        res_status, account_raw = self.get_request(account_url, params=params)
        # logging.info(account_raw.json())
        if res_status != 200:
            logging.error('Error: Issues with fetching the list of accounts associated to the Authorized account.',
                          'Please verify if authorized account has the privileges to access Google My Business Account')
            sys.exit(1)
        account_json = account_raw.json()
        out_account_list = account_json['accounts']

        # Looping for all the accounts
        if 'nextPageToken' in account_json:
            out_account_list = out_account_list + \
                self.list_accounts(nextPageToken=account_json['nextPageToken'])

        return out_account_list

    def list_locations(self, account_id, nextPageToken=None):
        '''
        Fetching all locations associated to the account_id
        '''

        # Location paramters
        out_location_list = []
        location_url = '{}/{}/locations'.format(self.base_url, account_id)
        params = {
            'access_token': self.access_token
        }
        # Pagination
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Loaction Lists
        res_status, location_raw = self.get_request(
            location_url, params=params)
        if res_status != 200:
            logging.error(
                'Something wrong with location request. Please investigate.')
            sys.exit(1)
        location_json = location_raw.json()

        # If the account has no locations under it
        if 'locations' not in location_json:
            out_location_list = []
        else:
            out_location_list = location_json['locations']

        # Looping for all the locations
        if 'nextPageToken' in location_json:
            out_location_list = out_location_list + \
                self.list_locations(
                    nextPageToken=location_json['nextPageToken'])

        return out_location_list

    def list_report_insights(self, account_id, location_id):
        '''
        Fetching all the report insights from assigned location
        '''

        insight_url = '{}/{}/locations:reportInsights'.format(
            self.base_url, account_id)
        header = {
            'Content-type': 'application/json',
            'Authorization': 'Bearer {}'.format(self.access_token)
        }
        payload = {
            'locationNames': [
                location_id
            ],
            'basicRequest': {
                'metricRequests': [
                    {
                        'metric': 'ALL',
                        'options': [
                            'AGGREGATED_DAILY'
                        ]
                    }
                ],
                'timeRange': {
                    'startTime': self.start_timestamp,
                    'endTime': self.end_timestamp
                }
            }
        }

        res_status, insights_raw = self.post_request(
            insight_url, headers=header, payload=payload)

        if res_status != 200:
            logging.error(
                'Something wrong with report insight request. Please investigate'
            )
            sys.exit(1)
        
        # Conditions when the API does not return expected results
        if 'locationMetrics' in insights_raw.json():
            insights_json = insights_raw.json()['locationMetrics']
        else:
            insights_json = []

        return insights_json

    def list_location_related_info(self, location_id, endpoint, nextPageToken=None):
        '''
        Fetching all the information associated to the location
        '''

        out_data = []

        generic_url = '{}/{}/{}'.format(self.base_url, location_id, endpoint)
        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get review for the location
        res_status, data_raw = self.get_request(generic_url, params=params)
        if res_status != 200:
            logging.error(
                'Something wrong with {} request. Please investigate.'.format(endpoint))
            sys.exit(1)
        data_json = data_raw.json()

        if endpoint == 'media':
            out_data = data_json['mediaItems']
        else:
            try:
                out_data = data_json[endpoint]
            except Exception:
                logging.error(data_json)

        # Looping for all the reviews
        if 'nextPageToken' in data_json:
            out_data = out_data + \
                self.list_location_related_info(
                    location_id=location_id,
                    endpoint=endpoint,
                    nextPageToken=data_json['nextPageToken'])

        return out_data

    def run(self, endpoints=[]):
        '''
        Bundle of the request flow
        '''

        all_accounts = self.list_accounts()
        logging.info(f'Accounts found: [{len(all_accounts)}]')

        # Outputting all the accounts found
        logging.info('Outputting Accounts...')
        parser.generic_parser(
            data_in=all_accounts,
            parent_obj_name='accounts',
            primary_key_name='name'
            # output_columns=self.output_columns['accounts']
        )

        # Finding all the accounts available for the authorized account
        for account in all_accounts:
            account_id = account['name']
            # Fetching all the locations available for the entered account
            all_locations = self.list_locations(account_id=account_id)
            logging.info('Locations found in Account [{}] - [{}]'.format(
                account['accountName'], len(all_locations)))
            logging.info('Outputting Locations...')
            parser.generic_parser(
                data_in=all_locations,
                parent_obj_name='locations',
                primary_key_name='name'
                # output_columns=self.output_columns['locations']
            )

            # If there are no locations, terminating the application
            if len(all_locations) == 0:
                logging.warning(f'There are no location info under the authorized account [{account["accountName"]}].')

            # Looping through all the locations
            for location in all_locations:
                location_id = location['name']
                logging.info('Parsing location [{}]...'.format(location_id))

                # Looping through all the requested endpoints
                for endpoint in endpoints:
                    logging.info(
                        'Fetching [{}] - {}...'.format(location_id, endpoint))
                    # insights endpoint has a different request url and method
                    if endpoint != 'reportInsights':
                        data_out = self.list_location_related_info(
                            location_id=location_id,
                            endpoint=endpoint)
                    else:
                        data_out = self.list_report_insights(
                            account_id=account_id,
                            location_id=location_id
                        )

                    # Ensure the output data file contains data, if not output nothing
                    if data_out and endpoint != 'reportInsights':
                        # logging.info('Outputting [{}] - {}...'.format(location_id, endpoint))
                        parser.generic_parser(
                            data_in=data_out,
                            parent_obj_name=endpoint,
                            primary_key_name='name'
                            # output_columns=self.output_columns.get(endpoint)
                        )
                    elif data_out and endpoint == 'reportInsights':
                        parser.insight_parser(
                            data_in=data_out
                        )
                    else:
                        logging.info(
                            'No [{}] found - {}'.format(endpoint, location['locationName']))

        parser.produce_manifest('accounts', ['name'])
        parser.produce_manifest('locations', ['name'])
