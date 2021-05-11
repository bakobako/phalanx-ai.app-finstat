import logging
import urllib.parse
import requests
import hashlib
import xmltodict
from enum import Enum

FINSTAT_URL = "https://finstat.sk/api/"


class FinstatClient:
    class RequestType(Enum):
        DETAIL = "detail"
        EXTENDED = "extended"
        ULTIMATE = "ultimate"

    def __init__(self, api_key, private_key, request_type):
        self.api_key = api_key
        self.private_key = private_key
        self.request_type = request_type

    def get_ico_data(self, ico):
        logging.info(f"Getting Finstat data for ico : {ico}")
        hash_key = self.get_hash_key(ico)
        params = self.construct_http_params(ico, hash_key)
        response, response_text = self.get_json_response(params, FINSTAT_URL, self.request_type)

        return response

    def get_hash_key(self, ico):
        """Creates a hash key that is needed for the api

                The API defines how this key should be constructed in the API documentation

                    Parameters:
                    api_key (string): The API key of the API user
                    private_key (string): The private key of the API user
                    ico (string): The ICO code of the

                    Returns:
                    hash_key (string): Holds the hashed key
            """
        hash_key_string = "SomeSalt+" + self.api_key + "+" + self.private_key + "++" + ico + "+ended"
        hash_key = self.encrypt_string(hash_key_string)
        return hash_key

    @staticmethod
    def encrypt_string(hash_string):
        """Encrypts a string with sha256

                Parameters:
                hash_string (string): Holds the string to be hashed

                Returns:
                sha_signature (string): Holds the hashed string
        """
        sha_signature = \
            hashlib.sha256(hash_string.encode()).hexdigest()
        return sha_signature

    def construct_http_params(self, ico, hash_key):
        params = {'ico': ico,
                  "apiKey": self.api_key,
                  "Hash": hash_key}
        return params

    def get_json_response(self, params, url, request_type):
        """Uses the API to get a single response

            The XML response of the API is converted to JSON.
            If ICO is invalid it returns False

                Parameters:
                params (dict): Holds the parameters of the API call

                Returns:
                json_response (dict): Holds the JSON response
        """
        # sending get request and saving the response as response object
        url = urllib.parse.urljoin(url, request_type)
        response = requests.get(url=url, params=params)

        if response.status_code == 200:
            # If successful return the result
            response_key = request_type.capitalize() + "Result"
            json_response = dict(xmltodict.parse(response.text)[response_key])
            return json_response, response.text
        else:
            logging.info(f"Error : ico {params['ico']} is not a valid ico in the Finstat database")
            return False, response.text
