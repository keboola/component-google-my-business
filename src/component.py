'''
Template Component main class.

'''

import logging
import logging_gelf.handlers
import logging_gelf.formatters
import sys
import os
import datetime # noqa

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef # noqa
from kbc.result import ResultWriter # noqa


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


    def get_tables(self, tables, mapping):
        """
        Evaluate input and output table names.
        Only taking the first one into consideration!
        mapping: input_mapping, output_mappings
        """
        # input file
        table_list = []
        for table in tables:
            name = table["full_path"]
            if mapping == "input_mapping":
                destination = table["destination"]
            elif mapping == "output_mapping" :
                destination = table["source"]
            table_list.append(destination)

        return table_list


    def run(self):
        '''
        Main execution code
        '''
        # Get proper list of tables
        in_tables = self.configuration.get_input_tables()
        out_tables = self.configuration.get_expected_output_tables()
        in_table_names = self.get_tables(in_tables, 'input_mapping')
        out_table_names = self.get_tables(out_tables, 'output_mapping')
        logging.info("IN tables mapped: "+str(in_table_names))
        logging.info("OUT tables mapped: "+str(out_table_names))

        params = self.cfg_params  # noqa

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
