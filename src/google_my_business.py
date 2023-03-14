import os
import json
import requests
import logging
from datetime import datetime

from keboola.csvwriter import ElasticDictWriter

from definitions import mapping

AVAILABLE_DAILY_METRICS = ["BUSINESS_IMPRESSIONS_DESKTOP_MAPS", "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
                           "BUSINESS_IMPRESSIONS_MOBILE_MAPS", "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
                           "BUSINESS_CONVERSATIONS", "BUSINESS_DIRECTION_REQUESTS", "CALL_CLICKS",
                           "WEBSITE_CLICKS", "BUSINESS_BOOKINGS", "BUSINESS_FOOD_ORDERS", "BUSINESS_FOOD_MENU_CLICKS"
                           ]


def get_date_from_string(date_string):
    """
    Extracts the year, month, and day from a string in the format "YYYY-MM-DDTHH:MM:SS.ffffffZ"
    and returns them as a tuple.
    """
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_object = datetime.strptime(date_string, date_format)
    year = date_object.year
    month = date_object.month
    day = date_object.day
    return year, month, day


def flatten_dict(d, max_key_length=64):
    flat_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            sub_dict = flatten_dict(value, max_key_length)
            for sub_key, sub_value in sub_dict.items():
                full_key = f"{key}_{sub_key}"
                if len(full_key) > max_key_length:
                    # Truncate key if it's too long
                    truncated_key = full_key[:max_key_length]
                    flat_dict[truncated_key] = sub_value
                else:
                    flat_dict[full_key] = sub_value
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    sub_dict = flatten_dict(item, max_key_length)
                    for sub_key, sub_value in sub_dict.items():
                        full_key = f"{key}_{i}_{sub_key}"
                        if len(full_key) > max_key_length:
                            # Truncate key if it's too long
                            truncated_key = full_key[:max_key_length]
                            flat_dict[truncated_key] = sub_value
                        else:
                            flat_dict[full_key] = sub_value
                else:
                    full_key = f"{key}_{i}"
                    if len(full_key) > max_key_length:
                        # Truncate key if it's too long
                        truncated_key = full_key[:max_key_length]
                        flat_dict[truncated_key] = item
                    else:
                        flat_dict[full_key] = item
        else:
            if len(key) > max_key_length:
                # Truncate key if it's too long
                truncated_key = key[:max_key_length]
                flat_dict[truncated_key] = value
            else:
                flat_dict[key] = value
    return flat_dict


class GMBException(Exception):
    pass


class GoogleMyBusiness:
    def __init__(self, access_token, start_timestamp, end_timestamp, data_folder_path, default_columns=None):
        if default_columns is None:
            default_columns = []
        self.output_columns = None
        self.access_token = access_token
        self.base_url = 'https://mybusiness.googleapis.com/v4'
        self.base_url_v1 = "https://mybusiness.googleapis.com/v1"
        self.base_url_profile_performance = "https://businessprofileperformance.googleapis.com/v1"
        self.base_url_quanda = "https://mybusinessqanda.googleapis.com/v1"

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.data_folder_path = data_folder_path

        self.default_table_source = os.path.join(data_folder_path, "in/tables/")
        self.default_table_destination = os.path.join(data_folder_path, "out/tables/")
        self.default_file_source = os.path.join(data_folder_path, "in/files/")
        self.default_file_destination = os.path.join(data_folder_path, "out/files/")

        self.session = requests.Session()
        self.reviews = []
        self.questions = []
        self.media = []
        self.daily_metrics = {}

        self.tables_columns = default_columns if default_columns else {}

    def run(self, endpoints=None):

        if endpoints is None:
            endpoints = []
        all_accounts = self.list_accounts()
        logging.info(f'Accounts found: [{len(all_accounts)}]')

        # Outputting all the accounts found
        logging.info('Outputting Accounts...')
        self.output_file(
            data_in=all_accounts,
            file_name='accounts'
        )

        # Finding all the accounts available for the authorized account
        for account in all_accounts:
            account_id = account['name']
            # Fetching all the locations available for the entered account
            all_locations = self.list_locations(account_id=account_id)
            logging.info('Locations found in Account [{}] - [{}]'.format(
                account['accountName'], len(all_locations)))
            logging.info('Outputting Locations...')
            self.output_file(
                data_in=all_locations,
                file_name='locations'
            )

            # If there are no locations, terminating the application
            if len(all_locations) == 0:
                raise GMBException(f'There is no location info under the authorized '
                                   f'account [{account["accountName"]}].')

            # Looping through all the locations and endpoints
            if 'dailyMetrics' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_id = location_path.replace("locations/", "")
                    location_title = location['title']
                    logging.info(f"Processing endpoint dailyMetrics for {location_title}.")
                    self.daily_metrics[location_id] = self.list_daily_metrics(location_id=location_path)
                self.daily_metrics_parser(data_in=self.daily_metrics)
            self.daily_metrics = {}

            if 'reviews' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    logging.info(f"Processing reviews for {location_title}.")
                    reviews = self.list_reviews(account_id=account_id, location_id=location_path)
                    for review in reviews:
                        self.reviews.append(review)
                self.output_file(data_in=self.reviews, file_name="reviews")
            self.reviews = []

            if 'media' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    logging.info(f"Processing media for {location_title}.")
                    media = self.list_media(location_id=location_path, account_id=account_id)
                    for medium in media:
                        self.questions.append(medium)
                self.output_file(data_in=self.media, file_name="media")
            self.questions = []

            if 'questions' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    logging.info(f"Processing questions for {location_title}.")
                    questions = self.list_questions(location_id=location_path)
                    for question in questions:
                        self.questions.append(question)
                self.output_file(file_name="questions", data_in=self.questions)
            self.questions = []

    def get_request(self, url, headers=None, params=None):
        """
        Base GET request
        """

        res = self.session.get(url=url, headers=headers, params=params)

        return res.status_code, res

    def list_accounts(self, nextPageToken=None):
        """
        Fetching all the accounts available in the authorized Google account
        """
        account_url = '{}/accounts'.format(self.base_url_v1)

        params = {
            'access_token': self.access_token
            # 'Authorization': 'Bearer {}'.format(self.access_token)
        }

        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Account Lists
        res_status, account_raw = self.get_request(account_url, params=params)
        if res_status != 200:
            raise GMBException(f'The component cannot fetch list of GMB accounts, error: {account_raw.text}')

        account_json = account_raw.json()
        print(account_json)
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

        location_url = '{}/{}/locations'.format(self.base_url_v1, account_id)
        params = {
            'access_token': self.access_token,
            'readMask': 'name,languageCode,storeCode,title,phoneNumbers,categories,storefrontAddress,websiteUri,'
                        'regularHours,specialHours,serviceArea,latlng,openInfo,metadata,profile,relationshipData'
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        res_status, location_raw = self.get_request(
            location_url, params=params)
        if res_status != 200:
            raise GMBException(f'Something wrong with location request. Response: {location_raw.text}')
        location_json = location_raw.json()

        # If the account has no locations under it
        if 'locations' not in location_json:
            out_location_list = []
        else:
            out_location_list = location_json['locations']

        # Looping for all the locations
        if 'nextPageToken' in location_json:
            out_location_list = out_location_list + \
                                self.list_locations(account_id=account_id,
                                                    nextPageToken=location_json['nextPageToken'])

        return out_location_list

    def list_daily_metrics(self, location_id):
        """
        Fetching all the report insights from assigned location.
        https://developers.google.com/my-business/reference/performance/rest/v1/
        locations/getDailyMetricsTimeSeries#DailyRange
        """
        start_year, start_month, start_day = get_date_from_string(self.start_timestamp)
        end_year, end_month, end_day = get_date_from_string(self.end_timestamp)

        parsed_values = {}
        for metric in AVAILABLE_DAILY_METRICS:
            logging.info(f"Fetching metric: {metric}")
            insight_url = self.base_url_profile_performance + f"/{location_id}:getDailyMetricsTimeSeries"
            params = {
                "dailyMetric": metric,
                "dailyRange.startDate.year": start_year,
                "dailyRange.startDate.month": start_month,
                "dailyRange.startDate.day": start_day,
                "dailyRange.endDate.year": end_year,
                "dailyRange.endDate.month": end_month,
                "dailyRange.endDate.day": end_day,
            }

            header = {
                'Content-type': 'application/json',
                'Authorization': 'Bearer {}'.format(self.access_token)
            }

            res_status, insights_raw = self.get_request(url=insight_url, headers=header, params=params)

            if res_status != 200:
                raise GMBException(f'Something wrong with report insight request. Response: {insights_raw.text}')

            response = insights_raw.json()
            if 'timeSeries' in response:
                time_series = response['timeSeries']['datedValues']

                for dated_value in time_series:
                    date = f"{dated_value['date']['year']}-{dated_value['date']['month']:02d}-" \
                           f"{dated_value['date']['day']:02d}"
                    value = int(dated_value.get('value', '0'))
                    if date not in parsed_values:
                        parsed_values[date] = {}
                    parsed_values[date][metric] = value

            else:
                logging.info(f"Metric {metric} did not return any time series.")
        return parsed_values

    def list_reviews(self, account_id, location_id, nextPageToken=None):
        responses = []

        url = self.base_url + "/" + account_id + "/" + location_id + "/reviews"
        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get review for the location
        res_status, data_raw = self.get_request(url, params=params)
        if res_status != 200:
            raise GMBException(f'Something wrong with request. Response: {data_raw.text}')
        data_json = data_raw.json()
        if 'reviews' in data_json:
            responses.extend(data_json['reviews'])

            if 'nextPageToken' in data_json:
                responses.extend(self.list_reviews(
                    account_id=account_id,
                    location_id=location_id,
                    nextPageToken=data_json['nextPageToken']))

            return responses
        else:
            GMBException(f'Reviews not found in response: {data_raw.text}')

    def list_questions(self, location_id, nextPageToken=None):
        responses = []

        url = self.base_url_quanda + "/" + location_id + "/questions"

        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get review for the location
        res_status, data_raw = self.get_request(url, params=params)
        if res_status != 200:
            raise GMBException(f'Something wrong with request. Response: {data_raw.text}')
        data_json = data_raw.json()
        if data_json:
            responses.extend(data_json['questions'])
            if 'nextPageToken' in data_json:
                responses.extend(self.list_questions(
                    location_id=location_id,
                    nextPageToken=data_json['nextPageToken']))
        else:
            logging.info(f"There are no media for {location_id}")

        return responses

    def list_media(self, location_id, account_id, nextPageToken=None):
        responses = []

        url = self.base_url + "/" + account_id + "/" + location_id + "/media"

        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get review for the location
        res_status, data_raw = self.get_request(url, params=params)
        if res_status != 200:
            raise GMBException(f'Something wrong with request. Response: {data_raw.text}')
        if res_status == 503:
            raise GMBException("Media service is unavailable at the moment.")

        data_json = data_raw.json()
        if data_json:
            responses.extend(data_json['mediaItems'])
            if 'nextPageToken' in data_json:
                responses.extend(self.list_media(
                    location_id=location_id,
                    account_id=account_id,
                    nextPageToken=data_json['nextPageToken']))
        else:
            logging.info(f"There are no media for {location_id}")

        return responses

    def output_file(self, file_name, data_in):
        """
        Saves data to csv file and produces manifest.
        """
        file_output_destination = '{}{}.csv'.format(
            self.default_table_destination, file_name)

        if self.tables_columns.get(file_name, {}):
            fieldnames = self.tables_columns.get(file_name)
        else:
            fieldnames = []

        if data_in:
            with ElasticDictWriter(file_output_destination, fieldnames) as wr:
                wr.writeheader()
                for row in data_in:
                    wr.writerow(flatten_dict(row))

            self.produce_manifest(file_name=file_name, primary_key=mapping[file_name])
            self.tables_columns[file_name] = wr.fieldnames
        else:
            logging.warning(f"File {file_name} is empty. Results will not be stored.")

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

    def daily_metrics_parser(self, data_in):
        """
        Parser dedicated for fetching location metrics
        """

        data_out = []
        for location_id, date_data in data_in.items():
            for date, metrics in date_data.items():
                for metric, value in metrics.items():
                    data_out.append({
                        "location_id": location_id,
                        "date": date,
                        "metric": metric,
                        "value": value
                    })

        self.output_file('daily_metrics', data_out)
