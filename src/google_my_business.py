import sys
import os
import json
import requests
import logging
import logging_gelf.handlers
import logging_gelf.formatters


if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])

# Get Authorization
cfg = docker.Config('/data/')
oauth = cfg.get_authorization()
credentials = oauth["oauth_api"]["credentials"]["#data"]
credentials_json = json.loads(credentials)
oauth_token = credentials_json["access_token"]
app_key = oauth["oauth_api"]["credentials"]["appKey"]
app_secret = oauth["oauth_api"]["credentials"]["#appSecret"]

# Request Parameters
requesting = requests.Session()


class Google_My_Business():
    '''
    Google My Business Request Handler
    '''

    def __init__(self):
        self.access_token = oauth_token
        self.base_url = 'https://mybusiness.googleapis.com/v4'

    def get_request(url, headers=None, params=None):
        '''
        Base GET request
        '''

        res = requesting.get(url=url, headers=headers, params=params)

        return res.status_code, res.json()

    def list_accounts(self, nextPageToken=None):
        '''
        Fetching all the accounts available in the authorized Google account
        '''

        # Accounts parameters
        out_account_list = []
        account_url = '{}/accounts'.format(self.base_url)
        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Account Lists
        res_status, account_json = self.get_request(account_url, params=params)
        if res_status != 200:
            logging.error('Error: Issues with fetching the list of accounts associated to the Authorized account.',
                          'Please verify if authorized account has the privileges to access Google My Business Account')
            sys.exit(1)
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
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get Loaction Lists
        res_status, location_json = self.get_request(
            location_url, params=params)
        if res_status != 200:
            logging.error(
                'Something wrong with location request. Please investigate.')
            sys.exit(1)
        out_location_list = location_json['locations']

        # Looping for all the locations
        if 'nextPageToken' in location_json:
            out_location_list = out_location_list + \
                self.list_locations(
                    nextPageToken=location_json['nextPageToken'])

        return out_location_list

    def list_location_related_info(self, account_id, location_id, endpoint, nextPageToken=None):
        '''
        Fetching all the reviews associated to the location
        '''

        out_data = []
        generic_url = '{}/{}/{}/{}'.format(
            self.base_url, account_id, location_id, endpoint)
        params = {
            'access_token': self.access_token
        }
        if nextPageToken:
            params['pageToken'] = nextPageToken

        # Get review for the location
        res_status, data_json = self.get_request(generic_url, params=params)
        if res_status != 200:
            logging.error(
                'Something wrong with review request. Please investigate.')
            sys.exit(1)
        out_data = data_json[endpoint]

        # Lopping for all the reviews
        if 'nextPageToken' in data_json:
            out_data = out_data + \
                self.list_location_related_info(
                    nextPageToken=data_json['nextPageToken'])

        return out_data

    def run(self):
        '''
        Bundle of the request flow
        '''

        all_accounts = self.list_accounts()
        # Finding all the accounts available for the authorized account
        for account in all_accounts:
            account_id = account['name']
            # Fetching all the locations available for the entered account
            all_locations = self.list_locations(account_id=account_id)
            for location in all_locations:
                location_id = location['name']

                # Reviews
                all_reviews = self.list_location_related_info(
                    account_id=account_id, location_id=location_id, endpoint='reviews')
                # Media
                all_media = self.list_location_related_info(
                    account_id=account_id, location_id=location_id, endpoint='media')
                # Local Posts
                all_local_posts = self.list_location_related_info(
                    account_id=account_id, location_id=location_id, endpoint='localPosts')
                # Questions
                all_questions = self.list_location_related_info(
                    account_id=account_id, location_id=location_id, endpoint='questions')
