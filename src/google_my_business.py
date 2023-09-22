import os
import json
import requests
import logging
from datetime import datetime
import uuid
import backoff

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


def backoff_custom():
    delays = [15, 30, 45]
    for delay in delays:
        yield delay


class GoogleMyBusiness:
    def __init__(self, access_token, data_folder_path, default_columns=None, start_timestamp=None, end_timestamp=None,
                 accounts=None, incremental=True):
        if default_columns is None:
            default_columns = []
        self.output_columns = None
        self.access_token = access_token
        self.incremental = incremental
        self.base_url = 'https://mybusiness.googleapis.com/v4'
        self.base_url_v1 = "https://mybusiness.googleapis.com/v1"
        self.base_url_profile_performance = "https://businessprofileperformance.googleapis.com/v1"
        self.base_url_quanda = "https://mybusinessqanda.googleapis.com/v1"

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.data_folder_path = data_folder_path

        self.temp_table_destination = os.path.join(data_folder_path, "temp/")
        self.default_table_destination = os.path.join(data_folder_path, "out/tables/")

        self.session = requests.Session()
        self.reviews = []
        self.questions = []
        self.media = []
        self.daily_metrics = {}

        self.tables_columns = default_columns if default_columns else {}
        self.selected_accounts = accounts if accounts else []
        self.account_list = []

    def test_connection(self):
        try:
            logging.info("hello")
            self.list_accounts()
        except Exception as e:
            raise GMBException(e)

    @staticmethod
    def select_entries(list1, list_all):
        relevant_entries = []
        for item in list_all:
            if item['name'] in list1:
                relevant_entries.append(item)
        return relevant_entries

    def process(self, endpoints=None):

        self.list_accounts()
        if self.selected_accounts:
            self.account_list = self.select_entries(self.selected_accounts, self.account_list)

        logging.info(f'Component will process following accounts: {self.account_list}')

        # Outputting all the accounts found
        logging.info('Outputting Accounts...')
        self.create_temp_files(
            data_in=self.account_list,
            file_name='accounts'
        )

        # Finding all the accounts available for the authorized account
        for account in self.account_list:
            account_id = account['name']
            # Fetching all the locations available for the entered account
            all_locations = self.list_locations(account_id=account_id)
            logging.info('Locations found in Account [{}] - [{}]'.format(
                account['accountName'], len(all_locations)))
            logging.info('Outputting Locations...')
            self.create_temp_files(
                data_in=all_locations,
                file_name='locations'
            )

            if len(all_locations) == 0:
                logging.error(f'There is no location info under the authorized '
                              f'account [{account["accountName"]}].')
                continue

            if 'dailyMetrics' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    location_id = location_path.replace("locations/", "")
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
                self.create_temp_files(data_in=self.reviews, file_name="reviews")
            self.reviews = []

            if 'media' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    logging.info(f"Processing media for {location_title}.")
                    media = self.list_media(location_id=location_path, account_id=account_id)
                    for medium in media:
                        self.media.append(medium)
                self.create_temp_files(data_in=self.media, file_name="media")
            self.media = []

            if 'questions' in endpoints:
                for location in all_locations:
                    location_path = location['name']
                    location_title = location['title']
                    logging.info(f"Processing questions for {location_title}.")
                    questions = self.list_questions(location_id=location_path)
                    for question in questions:
                        self.questions.append(question)
                self.create_temp_files(file_name="questions", data_in=self.questions)
            self.questions = []

        self.save_resulting_files()

    @backoff.on_exception(backoff_custom, Exception, max_tries=3)
    def get_request(self, url, headers=None, params=None):
        res = self.session.get(url=url, headers=headers, params=params)
        if res.status_code == 429:
            # Raise an exception to trigger the retry logic
            raise Exception("Rate limit exceeded. Retrying...")
        elif res.status_code in [400, 403, 500]:
            # Ignore error 403 and return the status code and response object
            return res.status_code, res
        elif res.status_code != 200:
            raise Exception(f"Request failed with status code {res.status_code}")
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
        self.account_list = account_json['accounts']

        print(self.account_list)
        exit()

        # Looping for all the accounts
        if 'nextPageToken' in account_json:
            self.account_list = self.account_list + \
                                self.list_accounts(nextPageToken=account_json['nextPageToken'])

    @backoff.on_exception(backoff.expo, GMBException, max_tries=5)
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
                if res_status == 403:
                    logging.error(f"Cannot fetch daily metrics for location with id {location_id}, response: "
                                  f"{insights_raw.text}")
                    return {}
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
            logging.warning(f'Reviews for location with id {location_id} not found in response: {data_raw.text}')
            return []

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
            if res_status == 400:
                if data_raw.json()["error"]["details"][0]["reason"] == "UNVERIFIED_LOCATION":
                    logging.warning(f"Location with id {location_id} is unverified. Cannot fetch questions.")
            else:
                logging.warning(f"Cannot fetch questions for location with id {location_id}. Received response: "
                                f"{data_raw.text}")
            return []

        data_json = data_raw.json()
        if data_json.get("questions", None):
            responses.extend(data_json["questions"])
            if 'nextPageToken' in data_json:
                responses.extend(self.list_questions(
                    location_id=location_id,
                    nextPageToken=data_json['nextPageToken']))
        else:
            logging.info(f"There are no questions for {location_id}")

        return responses

    @backoff.on_exception(backoff.expo, GMBException, max_tries=20)
    def list_media(self, location_id, account_id, nextPageToken=None):
        responses = []

        url = self.base_url + "/" + account_id + "/" + location_id + "/media"

        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

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

    def create_temp_files(self, file_name, data_in):
        """
        Saves data to json files.
        """
        file_output_destination = os.path.join(self.temp_table_destination, file_name)
        if not os.path.exists(file_output_destination):
            os.makedirs(file_output_destination)

        if data_in:
            for row in data_in:
                filename = os.path.join(file_output_destination, str(uuid.uuid4())+".json")
                with open(filename, 'w') as outfile:
                    json.dump(flatten_dict(row), outfile)
        else:
            logging.warning(f"File {file_name} is empty. Results will not be stored.")

    def produce_manifest(self, file_name, primary_key):
        """
        Dummy function for returning manifest
        """

        file = '{}{}.csv.manifest'.format(self.default_table_destination, file_name)

        manifest = {
            'incremental': self.incremental,
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

        self.create_temp_files('daily_metrics', data_out)

    def save_resulting_files(self):
        """Produces manifest and saves column names to statefile"""
        filenames = [f.name for f in os.scandir(self.temp_table_destination) if f.is_dir()]

        for file_name in filenames:

            if self.tables_columns.get(file_name, {}):
                fieldnames = self.tables_columns.get(file_name)
            else:
                fieldnames = []

            temp_dir = os.path.join(self.temp_table_destination, file_name)
            temp_files = self.list_json_files(temp_dir)

            if temp_files:
                tgt_path = os.path.join(self.default_table_destination, file_name+".csv")
                with ElasticDictWriter(tgt_path, fieldnames) as wr:
                    wr.writeheader()
                    for file in temp_files:
                        with open(file, 'r') as f:
                            data = json.load(f)
                        wr.writerow(data)

                self.produce_manifest(file_name=file_name, primary_key=mapping[file_name])
                self.tables_columns[file_name] = wr.fieldnames

    @staticmethod
    def list_json_files(target_dir):
        json_files = []
        for filename in os.listdir(target_dir):
            if filename.endswith('.json'):
                json_files.append(os.path.join(target_dir, filename))
        return json_files
