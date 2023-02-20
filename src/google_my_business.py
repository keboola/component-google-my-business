import os
import json  # noqa
import requests
import logging
import logging_gelf.handlers
import logging_gelf.formatters
import pandas as pd

from definitions import mapping as output_columns_mapping


class GMBException(Exception):
    pass


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


class GoogleMyBusiness:
    def __init__(self, access_token, start_timestamp, end_timestamp, data_folder_path):
        self.output_columns = None
        self.access_token = access_token
        self.base_url = 'https://mybusiness.googleapis.com/v4'
        self.base_url_v1 = "https://mybusiness.googleapis.com/v1"
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.data_folder_path = data_folder_path

        self.default_table_source = os.path.join(data_folder_path, "in/tables/")
        self.default_table_destination = os.path.join(data_folder_path, "out/tables/")
        self.default_file_source = os.path.join(data_folder_path, "in/files/")
        self.default_file_destination = os.path.join(data_folder_path, "out/files/")

    def get_output_columns(self):
        """
        Getting all the column names from the definitions.py
        """

        with open('src/definitions.py', 'r') as f:
            self.output_columns = json.load(f)

    @staticmethod
    def get_request(url, headers=None, params=None):
        """
        Base GET request
        """

        res = requesting.get(url=url, headers=headers, params=params)

        return res.status_code, res

    @staticmethod
    def post_request(url, headers=None, payload=None):
        """
        Base POST request
        """

        res = requesting.post(url=url, headers=headers, json=payload)

        return res.status_code, res

    def list_accounts(self, nextPageToken=None):
        """
        Fetching all the accounts available in the authorized Google account
        """

        # Accounts parameters
        out_account_list = []
        account_url = '{}/accounts'.format(self.base_url_v1)

        params = {
            'access_token': self.access_token
            # 'Authorization': 'Bearer {}'.format(self.access_token)
        }

        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Account Lists
        res_status, account_raw = self.get_request(account_url, params=params)
        logging.info(account_raw.content)
        if res_status != 200:
            raise GMBException('Error: Issues with fetching the list of accounts associated to the Authorized account.',
                               'Please verify if authorized account has the privileges '
                               'to access Google My Business Account')

        account_json = account_raw.json()
        out_account_list = account_json['accounts']

        # Looping for all the accounts
        if 'nextPageToken' in account_json:
            out_account_list = out_account_list + \
                               self.list_accounts(nextPageToken=account_json['nextPageToken'])

        return out_account_list

    def list_locations(self, account_id, nextPageToken=None):
        """
        Fetching all locations associated to the account_id
        """

        out_location_list = []
        location_url = '{}/{}/locations'.format(self.base_url, account_id)
        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        res_status, location_raw = self.get_request(
            location_url, params=params)
        if res_status != 200:
            raise GMBException(f'Something wrong with location request. Response: {location_raw}')
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
        """
        Fetching all the report insights from assigned location
        """

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
            raise GMBException(f'Something wrong with report insight request. Response: {insights_raw}')

        # Conditions when the API does not return expected results
        if 'locationMetrics' in insights_raw.json():
            insights_json = insights_raw.json()['locationMetrics']
        else:
            insights_json = []

        return insights_json

    def list_location_related_info(self, location_id, endpoint, nextPageToken=None):
        """
        Fetching all the information associated to the location
        """

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
            raise GMBException(f'Something wrong with request. Response: {data_raw}')
        data_json = data_raw.json()

        if endpoint == 'media':
            out_data = data_json['mediaItems'] if data_json.get(
                'mediaItems') else {}
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

    def run(self, endpoints=None):

        if endpoints is None:
            endpoints = []
        all_accounts = self.list_accounts()
        logging.info(f'Accounts found: [{len(all_accounts)}]')

        # Outputting all the accounts found
        logging.info('Outputting Accounts...')
        self.generic_parser(
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
            self.generic_parser(
                data_in=all_locations,
                parent_obj_name='locations',
                primary_key_name='name'
            )

            # If there are no locations, terminating the application
            if len(all_locations) == 0:
                logging.warning(
                    f'There are no location info under the authorized account [{account["accountName"]}].')

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

                        self.generic_parser(
                            data_in=data_out,
                            parent_obj_name=endpoint,
                            primary_key_name='name'
                            # output_columns=self.output_columns.get(endpoint)
                        )
                    elif data_out and endpoint == 'reportInsights':
                        self.insight_parser(
                            data_in=data_out
                        )
                    else:
                        logging.info(
                            'No [{}] found - {}'.format(endpoint, location['locationName']))

        self.produce_manifest('accounts', ['name'])
        self.produce_manifest('locations', ['name'])

    def output_file(self, file_name, data_in, output_columns=None):
        """
        Output dataframe to destination file
        """

        file_output_destination = '{}{}.csv'.format(
            self.default_table_destination, file_name)

        df = pd.DataFrame(data_in)
        output_file_columns = output_columns_mapping.get(file_name)

        # Logic to shrink column names if they are too long
        if not output_file_columns:
            header_columns = []

            for col in df.columns:
                while len(col) > 64:
                    col_split = col.split('_')
                    col = '_'.join(col_split[1:])

                header_columns.append(col)
        else:
            header_columns = output_file_columns

        # Output input datasets with selected columns and dedicated column names
        if not os.path.isfile(file_output_destination):
            with open(file_output_destination, 'w') as f:
                df.to_csv(f, index=False,
                          columns=output_file_columns, header=header_columns)
            f.close()
        else:
            with open(file_output_destination, 'a') as f:
                df.to_csv(f, index=False, header=False,
                          columns=output_file_columns)
            f.close()

    def generic_parser(self, data_in,
                       parent_obj_name=None,  # parent JSON property name to carry over as the filename
                       parent_col_name=None,  # Parent loop column name
                       primary_key_name=None,
                       primary_key_value=None
                       # output_columns=None  # specifying what columns to output
                       ):
        """
        Generic Parser
        1. if type is list, it will output a new table under that JSON obj
        2. if type is dict, it will flatten the obj and output under the same parent obj
        3. if type is string, it will compile all the strings within the same obj and output
        """

        if type(data_in) is list:

            # For cases when the data in the list is not object, but strings
            # example: ['a', 'b', 'c']
            if len(data_in) > 0:
                if type(data_in[0]) is str:
                    return f'{data_in}'

            data_out = []
            for obj in data_in:
                temp_json_obj = {}
                # Setting up parent value
                if primary_key_name:
                    try:
                        primary_key_value = obj[primary_key_name]
                        temp_json_obj[primary_key_name] = primary_key_value
                    except Exception:
                        if primary_key_value:
                            temp_json_obj[primary_key_name] = primary_key_value

                # Output Sequences
                for col in obj:
                    if col == 'priceLists' or col == 'attributes':
                        # Skipping priceLists obj from location which causes issues with parsing
                        # Data is not needed from this atm
                        break

                    # For cases when the data in the list is not object, but strings
                    # example: ['a', 'b', 'c']
                    if type(obj[col]) is list and type(obj[col][0]) is str:
                        temp_json_obj[col] = f'{obj[col]}'

                    else:
                        json_value = self.generic_parser(
                            data_in=obj[col],
                            parent_obj_name='{}_{}'.format(parent_obj_name, col),
                            parent_col_name=col,
                            primary_key_name=primary_key_name,
                            primary_key_value=primary_key_value
                            # output_columns=output_columns
                        )
                        if json_value:
                            for row in json_value:
                                temp_json_obj[row] = json_value[row]
                data_out.append(temp_json_obj)

            if data_out:
                self.output_file(file_name=parent_obj_name, data_in=data_out)

            return None

        elif type(data_in) is dict:
            temp_json_obj = {}
            for obj in data_in:
                # new_col_name = f'{obj}'
                new_col_name = f'{parent_col_name}_{obj}'
                temp_json_obj[new_col_name] = data_in[obj]
            return temp_json_obj

        elif type(data_in) is str:
            temp_json_obj = {
                parent_obj_name: data_in
                # parent_col_name: data_in
            }
            return temp_json_obj

    def produce_manifest(self, file_name, primary_key):
        """
        Dummy function for returning manifest
        """

        file = '{}{}.csv.manifest'.format(self.default_table_destination, file_name)

        manifest = {
            'incremental': True,
            'primary_key': primary_key
        }

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest, file_out)
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)

    def insight_parser(self, data_in):
        """
        Parser dedicated for fetching location metrics
        """

        data_out = []
        primary_key = [
            'location',
            'metric',
            'date'
        ]

        for location in data_in:
            for metric in location['metricValues']:
                # There are cases where the metric name is missing from the API result
                if 'metric' in metric:
                    for metric_value in metric['dimensionalValues']:
                        if 'value' in metric_value:
                            value = metric_value['value']
                        else:
                            value = '0'

                        temp_json = {
                            'location': location['locationName'],
                            'metric': metric['metric'],
                            'date': metric_value['timeDimension']['timeRange']['startTime'],
                            'value': value
                        }
                        data_out.append(temp_json)

        self.output_file('location_insights', data_out)
        self.produce_manifest('location_insights', primary_key)
