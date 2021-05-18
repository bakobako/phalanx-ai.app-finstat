'''
Template Component main class.

'''
import csv
import logging
from datetime import datetime
import json
from finstat.client import FinstatClient

from keboola.component.base import ComponentBase, UserException

# configuration variables
KEY_API_KEY = "api_key"
KEY_PRIVATE_KEY = "#private_key"
KEY_REQUEST_TYPE = "request_type"
KEY_ICO_FIELD = "ico_field"

KEY_TIMESTAMP = "timestamp"
API_LIMIT = 5000

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_PRIVATE_KEY, KEY_REQUEST_TYPE, KEY_ICO_FIELD]
REQUIRED_IMAGE_PARS = []

DETAIL = ["Ico", "RegisterNumberText", "Dic", "IcDPH", "Name", "Street", "StreetNumber", "ZipCode", "City", "District",
          "Region", "Country", "Activity", "Created", "Cancelled", "SuspendedAsPerson", "Url", "Warning", "WarningUrl",
          "PaymentOrderWarning", "PaymentOrderUrl", "OrChange", "OrChangeUrl", "Revenue", "SkNaceCode", "SkNaceText",
          "SkNaceDivision", "SkNaceGroup", "LegalFormCode", "LegalFormText", "RpvsInsert", "RpvsUrl", "ProfitActual",
          "RevenueActual", "JudgementFinstatLink", "SalesCategory", "HasKaR", "KarUrl", "HasDebt", "DebtUrl",
          "JudgementIndicators"]

EXTENDED = ["Ico", "Dic", "IcDPH", "Name", "Street", "StreetNumber", "ZipCode", "City", "Activity", "District",
            "Region", "Country", "Created", "Cancelled", "SuspendedAsPerson", "Url", "RegisterNumberText",
            "IcDphAdditional",
            "SkNaceCode", "SkNaceText", "SkNaceDivision", "SkNaceGroup", "Phones", "Emails", "Warning", "WarningUrl",
            "Debts",
            "StateReceivables", "CommercialReceivables", "PaymentOrderWarning", "PaymentOrderUrl", "PaymentOrders",
            "OrChange", "OrChangeUrl", "EmployeeCode", "EmployeeText", "LegalFormCode", "LegalFormText", "RpvsInsert",
            "RpvsUrl",
            "OwnershipTypeCode", "OwnershipTypeText", "CreditScoreValue", "ProfitActual", "ProfitPrev",
            "RevenueActual", "RevenuePrev", "ActualYear", "CreditScoreState", "ForeignResources", "GrossMargin", "ROA",
            "WarningLiquidation", "SelfEmployed", "WarningKaR", "Offices", "Subjects", "StructuredName", "HasKaR",
            "KarUrl", "HasDebt",
            "DebtUrl", "HasDisposal", "DisposalUrl", "ContactSources", "BasicCapital", "JudgementIndicators",
            "JudgementFinstatLink", "JudgementCounts", "JudgementLastPublishedDate", "Ratios", "SalesCategory"]


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)
        self.request_cols = []

    def run(self):
        params = self.configuration.parameters

        api_key = params.get(KEY_API_KEY)
        private_key = params.get(KEY_PRIVATE_KEY)
        request_type = params.get(KEY_REQUEST_TYPE)
        ico_field = params.get(KEY_ICO_FIELD)

        if request_type == "detail":
            self.request_cols = DETAIL
        elif request_type == "extended":
            self.request_cols = EXTENDED

        timestamp = self.get_current_timestamp()
        input_file = self.get_input_tables_definitions()[0].full_path

        input_icos = self.get_input_icos(input_file, ico_field)
        finstat_client = FinstatClient(api_key, private_key, request_type)

        results, bad_icos = self.get_results(finstat_client, input_icos, timestamp)

        self.request_cols.append(KEY_TIMESTAMP)
        self.write_results(results, request_type, self.request_cols)
        self.write_results(bad_icos, request_type, ["Ico"], append_to_name="_bad_icos", incremental=False)

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

    @staticmethod
    def get_current_timestamp():
        return datetime.now().isoformat()

    def get_results(self, finstat_client, input_icos, timestamp):
        results = []
        bad_icos = []
        for input_ico in input_icos:
            result = finstat_client.get_ico_data(input_ico)
            if result:
                result = self.normalize_result_keys(result, timestamp)
                results.append(result)
            else:
                bad_icos.append({"Ico": input_ico})
        return results, bad_icos

    def write_results(self, results, request_type, columns, incremental=True, append_to_name=""):
        out_file_name = "".join(["finstat_", request_type, append_to_name, ".csv"])
        tdf = self.create_out_table_definition(out_file_name, incremental=incremental, primary_key=["Ico"])
        tdf.columns = columns
        with open(tdf.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=tdf.columns)
            for result in results:
                writer.writerow(result)
        self.write_tabledef_manifest(tdf)

    def normalize_result_keys(self, result, timestamp):
        new_result = {}
        for col in self.request_cols:
            if col in result.keys():
                new_result[col] = result[col]
            else:
                new_result[col] = ""
        new_result[KEY_TIMESTAMP] = timestamp
        return new_result


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
