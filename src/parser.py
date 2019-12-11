import sys
import os
import json
import logging
import logging_gelf.handlers
import logging_gelf.formatters
import pandas as pd


# Default Table Output Destination
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_FILE_SOURCE = "/data/in/files/"


if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])


def parse_locations(data_in):
    '''
    Parser for locations
    '''
    data_out = []

    for data in data_in:
        temp_data = {
            'name': data['name'],
            'languageCode': data['languageCode'],
            'storeCode': data['storeCode'],
            'locationName': data['locationName'],
            'primaryPhone': data['primaryPhone'],
            'address_region': data['address']['regionCode'],
            'postalCode': data['address']['postalCode'],
            'administrativeArea': data['address']['administrativeArea'],
            'organization': data['address']['organization'],
            'primaryCategoryName': data['primaryCategory']['displayName'],
            'primaryCategoryId': data['primaryCategory']['categoryId'],
            'websiteUrl': data['websiteUrl'],
        }


def output_file(file_name, data_in):
    '''
    Output dataframe to destination file
    '''

    file_output_destination = '{}{}.csv'.format(
        DEFAULT_TABLE_DESTINATION, file_name)
    df = pd.DataFrame(data_in)
    if not os.path.isfile(file_output_destination):
        with open(file_output_destination, 'a') as f:
            df.to_csv(f, index=False)
        f.close()
    else:
        with open(file_output_destination, 'a') as f:
            df.to_csv(f, index=False, header=False)
        f.close()


def generic_parser(data_in,
                   parent_obj_name=None,  # parent JSON property name to carry over as the filename
                   parent_col_name=None,  # Parent loop column name
                   primary_key_name=None,
                   primary_key_value=None):
    '''
    Generic Parser
    1. if type is list, it will output a new table under that JSON obj
    2. if type is dict, it will flatten the obj and output under the same parent obj
    3. if type is string, it will compile all the strings within the same obj and output
    '''

    if type(data_in) is list:
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
                json_value = generic_parser(
                    data_in=obj[col],
                    parent_obj_name='{}_{}'.format(parent_obj_name, col),
                    parent_col_name=col,
                    primary_key_name=primary_key_name,
                    primary_key_value=primary_key_value
                )
                if json_value:
                    for row in json_value:
                        temp_json_obj[row] = json_value[row]
            data_out.append(temp_json_obj)

        if data_out:
            output_file(file_name=parent_obj_name, data_in=data_out)

        return None

    elif type(data_in) is dict:
        temp_json_obj = {}
        for obj in data_in:
            new_col_name = '{}_{}'.format(parent_obj_name, obj)
            temp_json_obj[new_col_name] = data_in[obj]
        return temp_json_obj

    elif type(data_in) is str:
        temp_json_obj = {
            parent_obj_name: data_in
        }
        return temp_json_obj
