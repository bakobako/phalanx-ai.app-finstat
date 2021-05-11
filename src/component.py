'''
Template Component main class.

'''
import csv
import logging
from datetime import datetime
import pandas as pd
import os
from collections import OrderedDict

from finstat.client import FinstatClient

from keboola.component.base import ComponentBase, UserException

# configuration variables
KEY_API_KEY = "api_key"
KEY_PRIVATE_KEY = "#private_key"
KEY_REQUEST_TYPE = "request_type"
KEY_ICO_FIELD = "ico_field"

API_LIMIT = 5000

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_PRIVATE_KEY, KEY_REQUEST_TYPE]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters

        api_key = params.get(KEY_API_KEY)
        private_key = params.get(KEY_PRIVATE_KEY)
        request_type = params.get(KEY_REQUEST_TYPE)
        ico_field = params.get(KEY_ICO_FIELD)

        timestamp = self.get_current_timestamp()

        input_file = self.get_input_tables_definitions()[0].full_path

        input_icos = self.get_input_icos(input_file, ico_field)

        output_file_path = self.tables_out_path
        out_file_name = "".join(["finstat_output",'.csv'])
        output_file_path = os.path.join(output_file_path, out_file_name)

        finstat_client = FinstatClient(api_key, private_key, request_type)

        results = []
        for input_ico in input_icos:
            result = finstat_client.get_ico_data(input_ico)
            if result:
                results.append(result)

        for i, response in enumerate(results):
            results[i] = self.flatten_json(response, "__")

        table = self.create_out_table_definition(out_file_name, incremental=True, primary_key=["Ico"])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        if len(results) > 0:
            results_df = pd.DataFrame.from_records(results)
            results_df["update_date"] = timestamp
            results_df.to_csv(output_file_path, index=False)
        else:
            logging.error(f"Error : No output. "
                          f"Your API request type or keys might be incorrect or"
                          f" all ICO inputs are invalid")
            exit(1)

        # Save table manifest (output.csv.manifest) from the tabledefinition
        table.columns = list(results_df.columns)
        self.write_tabledef_manifest(table)

    def get_input_icos(self, input_file, ico_field):
        icos = []
        reader = csv.DictReader(open(input_file))
        for row in reader:
            icos.append(row[ico_field])

        icos = self.check_api_limit(icos)
        return icos

    def check_api_limit(self, ico_list):
        if len(ico_list) > API_LIMIT:
            logging.warning(f"You are requesting more than {API_LIMIT} of ICOs to be extracted,"
                            f" only the first {API_LIMIT} will be extracted")
            return ico_list[0:API_LIMIT]
        return ico_list

    def flatten_json(self, json_dict, delim):
        """Flattens a JSON dictionary so it can be stored in a single table row

                Parameters:
                json_dict (dict): Holds the json data
                delim (string): The delimiter to be used to create flattened keys

                Returns:
                flattened_dict (dict): Holds the flattened dictionary
        """
        flattened_dict = {}
        for i in json_dict.keys():
            if isinstance(json_dict[i], dict) or isinstance(json_dict[i], OrderedDict):
                get = self.flatten_json(json_dict[i], delim)
                for j in get.keys():
                    flattened_dict[i + delim + j] = get[j]
            else:
                flattened_dict[i] = json_dict[i]

        return flattened_dict

    @staticmethod
    def get_current_timestamp():
        return datetime.now().isoformat()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
