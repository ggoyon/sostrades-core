'''
Copyright 2023 Capgemini

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from sostrades_core.execution_engine.data_connector.abstract_data_connector import AbstractDataConnector
import json 
import pandas as pd 
from os.path import dirname, join, exists



class JSONDatasetsConnector(AbstractDataConnector):
    """
    Specific data connector for dataset in json format
    """

    NAME = 'JSON_datasets'

    CONNECTOR_TYPE = 'connector_type'
    CONNECTOR_DATA = 'connector_data'
    CONNECTOR_REQUEST = 'connector_request'

    def __init__(self, data_connection_info=None):
        """
        Constructor for JSON data connector

        :param data_connection_info: contains necessary data for connection
        :type data_connection_info: dict
        """
        self.filename = None
        self.filepath = dirname(__file__)
        self.json_content = {}
        super().__init__(data_connection_info=data_connection_info)

    def _extract_connection_info(self,data_connection_info):

        self.filename = data_connection_info['filename']


    def load_data(self, dataset_name):
        """
        Method to load a dataset from JSON and filla data_dict

        :param: dataset_name, name of the dataset
        :type: string

        """
        #TODO: optimise opening and reading by creating a dedictated abstractDatasetConnector
        json_data = {}
        # Read JSON
        db_path = join(self.filepath,self.filename)
        if exists(db_path):
            with open(db_path, "r") as file:
                json_data = json.load(file)
        else:
            raise Exception(f'The connector json file is not found at {db_path}')
        
        if dataset_name in json_data:
            return json_data[dataset_name]
        else:
            raise Exception(f'The dataset {dataset_name} is not found in the {self.filename}')
        

    
    def write_data(self, ):
        """
        Method to load a data 
        """

        raise Exception("method not implemented")

    def set_connector_request(self, requested_datasets):

        raise Exception("method not implemented")
