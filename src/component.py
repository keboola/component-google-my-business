import logging
import os
import json
import dateparser
import requests
import shutil

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException

from google_my_business import GoogleMyBusiness, GMBException

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'
KEY_ENDPOINTS = 'endpoints'
KEY_ACCOUNTS = 'accounts'
KEY_GROUP_DESTINATION = 'destination'
KEY_LOAD_TYPE = 'load_type'

MANDATORY_PARS = [KEY_ENDPOINTS, KEY_API_TOKEN]


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

        endpoints = params[KEY_ENDPOINTS]
        logging.info(f"Component will process following endpoints: {endpoints}")
        if not endpoints:
            raise UserException('Please select an endpoint.')

        # Validating input date parameters
        start_date_str = params['request_range'].get('start_date', '7 days ago')
        end_date_str = params['request_range'].get('end_date', 'today')
        start_date_form, end_date_form = dateparser.parse(start_date_str), dateparser.parse(end_date_str)
        if start_date_form > end_date_form:
            raise UserException('Start Date cannot exceed End Date. Please re-enter [Request Range].')
        start_date_str = start_date_form.strftime('%Y-%m-%dT00:00:00.000000Z')
        end_date_str = end_date_form.strftime('%Y-%m-%dT00:00:00.000000Z')

        logging.info('Request Range: {} to {}'.format(start_date_str, end_date_str))
        accounts = params.get(KEY_ACCOUNTS, {})
        if not accounts:
            raise UserException("The authorized account has to have a linked My Google Business account with "
                                "management rights and proper account selected in the component's Accounts parameter.")

        destination_params = params.get(KEY_GROUP_DESTINATION, {})
        incremental = destination_params.get(KEY_LOAD_TYPE) != 'full_load' if destination_params else False

        statefile = self.get_state_file()
        default_columns = statefile or []
        if statefile:
            logging.info(f"Columns loaded from statefile: {default_columns}")

        self.create_temp_folder()

        gmb = GoogleMyBusiness(
            access_token=oauth_token,
            start_timestamp=start_date_str,
            end_timestamp=end_date_str,
            data_folder_path=self.data_folder_path,
            default_columns=default_columns,
            accounts=accounts,
            incremental=incremental
        )
        try:
            gmb.process(endpoints=endpoints)
        except GMBException as e:
            raise UserException(e)

        self.write_state_file(gmb.tables_columns)
        self.delete_temp_folder()

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

    def create_temp_folder(self):
        temp_path = os.path.join(self.data_folder_path, "temp")
        if not os.path.exists(temp_path):
            logging.info("creating temp folder")
            os.makedirs(temp_path)

    def delete_temp_folder(self):
        temp_path = os.path.join(self.data_folder_path, "temp")
        try:
            shutil.rmtree(temp_path)
        except OSError as e:
            logging.error(f"Error deleting {temp_path}: {e}")

    @sync_action('listAccounts')
    def list_accounts(self):
        authorization = self.configuration.config_data["authorization"]
        oauth_token = self.get_oauth_token(authorization)

        gmb = GoogleMyBusiness(
            access_token=oauth_token,
            data_folder_path=self.data_folder_path)
        try:
            gmb.process(endpoints=["accounts"])
        except GMBException:
            raise UserException("Failed to retrieved Google My Business accounts for which the authorized user has "
                                "management rights.")

        accounts = []
        if gmb.account_list:
            for account in gmb.account_list:
                if account.get("name", None) and account.get("accountName", None):
                    accounts.append(
                        {
                            "label": account.get("accountName"),
                            "value": account.get("name")
                        }
                    )
            return accounts
        else:
            raise UserException("Authorized account does not have any Google My Business accounts with "
                                "management rights.")


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
