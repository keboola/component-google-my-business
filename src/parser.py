import sys  # noqa
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

with open('src/mapping.json', 'r') as f:
    output_columns_mapping = json.load(f)


if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])


def output_file(file_name, data_in, output_columns=None):
    '''
    Output dataframe to destination file
    '''

    file_output_destination = '{}{}.csv'.format(
        DEFAULT_TABLE_DESTINATION, file_name)
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
        with open(file_output_destination, 'a') as f:
            df.to_csv(f, index=False,
                      columns=output_file_columns, header=header_columns)
        f.close()
    else:
        with open(file_output_destination, 'a') as f:
            df.to_csv(f, index=False, header=False,
                      columns=output_file_columns)
        f.close()


def generic_parser(data_in,
                   parent_obj_name=None,  # parent JSON property name to carry over as the filename
                   parent_col_name=None,  # Parent loop column name
                   primary_key_name=None,
                   primary_key_value=None
                   # output_columns=None  # specifying what columns to output
                   ):
    '''
    Generic Parser
    1. if type is list, it will output a new table under that JSON obj
    2. if type is dict, it will flatten the obj and output under the same parent obj
    3. if type is string, it will compile all the strings within the same obj and output
    '''

    if type(data_in) is list:

        # For cases when the data in the list is not object, but strings
        # example: ['a', 'b', 'c']
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
                    json_value = generic_parser(
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

            output_file(file_name=parent_obj_name, data_in=data_out)

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


def produce_manifest(file_name, primary_key):
    '''
    Dummy function for returning manifest
    '''

    file = '{}{}.csv.manifest'.format(DEFAULT_TABLE_DESTINATION, file_name)

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


def insight_parser(data_in):
    '''
    Parser dedicated for fetching location metrics
    '''

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

    output_file('location_insights', data_out)
    produce_manifest('location_insights', primary_key)
