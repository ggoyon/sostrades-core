'''
Copyright 2024 Capgemini

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
import logging
from typing import Any, List
from sostrades_core.datasets.dataset_info import DatasetInfo
from sostrades_core.datasets.dataset import Dataset
from sostrades_core.datasets.datasets_connectors.datasets_connector_manager import (
    DatasetsConnectorManager,
)
from sostrades_core.execution_engine.proxy_discipline import ProxyDiscipline


class DatasetsManager:
    """
    Manages connections to datasets
    """
    VALUE = ProxyDiscipline.VALUE
    DATASET_INFO = 'dataset_info'

    def __init__(self, logger:logging.Logger):
        self.datasets = {}
        self.__logger = logger

    def fetch_data_from_datasets(self, datasets_info: List[DatasetInfo],
                                 data_dict: dict[str:str]) -> dict[str:dict[str:Any]]:
        """
        get data from datasets and fill data_dict

        :param datasets_info: list of datasets associated to a namespace
        :type datasets_info: List[DatasetInfo]

        :param data_dict: dict of data to be fetch in datasets with their types
        :type data_dict: dict[name str: type str]

        :return: data_dict of data names and retrieved values plus a DATASET_INFO field with DatasetInfo object
        """
        self.__logger.debug(f"Fetching data {data_dict.keys()} from datasets {datasets_info}")
        data_retrieved = {}

        for dataset_info in datasets_info:
            # Get the dataset, creates it if not exists
            dataset = self.get_dataset(dataset_info=dataset_info)

            # Retrieve values
            dataset_values = dataset.get_values(data_dict=data_dict)
            # Update internal dictionnary adding provenance (DatasetInfo object) for tracking parameter changes
            dataset_data = {key: {self.VALUE: value,
                                  self.DATASET_INFO: dataset_info} for key, value in dataset_values.items()}
            data_retrieved.update(dataset_data)
        return data_retrieved

    def get_dataset(self, dataset_info: DatasetInfo) -> Dataset:
        """
        Gets a dataset, creates it if it does not exist

        :param dataset_info: Dataset info
        :type dataset_info: DatasetInfo

        :return: Dataset
        """
        if dataset_info not in self.datasets:
            self.datasets[dataset_info] = self.__create_dataset(dataset_info=dataset_info)
        return self.datasets[dataset_info]

    def __create_dataset(self, dataset_info: DatasetInfo) -> Dataset:
        """
        Private method
        Get the connector associated to the dataset and create a Dataset object

        :param dataset_info: Dataset info
        :type dataset_info: DatasetInfo
        """
        # Gets connector
        connector = DatasetsConnectorManager.get_connector(connector_identifier=dataset_info.connector_id)

        return Dataset(dataset_info=dataset_info, connector=connector)
