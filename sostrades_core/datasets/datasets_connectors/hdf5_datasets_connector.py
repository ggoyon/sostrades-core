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
import h5py
from typing import Any

from sostrades_core.datasets.dataset_info.abstract_dataset_info import AbstractDatasetInfo
from sostrades_core.datasets.dataset_info.dataset_info_v0 import DatasetInfoV0
from sostrades_core.datasets.datasets_connectors.abstract_datasets_connector import (
    AbstractDatasetsConnector,
    DatasetGenericException,
    DatasetNotFoundException,
)


class HDF5DatasetsConnector(AbstractDatasetsConnector):
    """
    Specific dataset connector for dataset in HDF5 format
    """

    def __init__(self, connector_id: str, file_path: str):
        """
        Constructor for HDF5 data connector

        :param file_path: file_path for this dataset connector
        :type file_path: str
        """
        super().__init__()
        self.__file_path = file_path
        self.__logger = logging.getLogger(__name__)
        self.__logger.debug("Initializing HDF5 connector")
        self.connector_id = connector_id

    def _get_values(self, dataset_identifier: AbstractDatasetInfo, data_to_get: dict[str:str]) -> dict[str:Any]:
        """
        Method to retrieve data from HDF5 and fill a data_dict

        :param dataset_identifier: identifier of the dataset
        :type dataset_identifier: DatasetInfo

        :param data_to_get: data to retrieve, dict of names and types
        :type data_to_get: dict[str:str]
        """
        self.__logger.debug(f"Getting values {data_to_get.keys()} for dataset {dataset_identifier.dataset_id} for connector {self}")

        with h5py.File(self.__file_path, 'r') as hdf_file:
            if dataset_identifier.dataset_id not in hdf_file:
                raise DatasetNotFoundException(dataset_identifier.dataset_id)

            dataset_group = hdf_file[dataset_identifier.dataset_id]
            filtered_values = {key: dataset_group[key][()] for key in data_to_get if key in dataset_group}

        self.__logger.debug(f"Values obtained {list(filtered_values.keys())} for dataset {dataset_identifier.dataset_id} for connector {self}")
        return filtered_values

    def _write_values(self, dataset_identifier: AbstractDatasetInfo, values_to_write: dict[str:Any], data_types_dict: dict[str:str]) -> dict[str: Any]:
        """
        Method to write data

        :param dataset_identifier: dataset identifier for connector
        :type dataset_identifier: DatasetInfo
        :param values_to_write: dict of data to write {name: value}
        :type values_to_write: Dict[str:Any]
        :param data_types_dict: dict of data type {name: type}
        :type data_types_dict: dict[str:str]
        """
        self.__logger.debug(f"Writing values in dataset {dataset_identifier.dataset_id} for connector {self}")

        with h5py.File(self.__file_path, 'a') as hdf_file:
            if dataset_identifier.dataset_id not in hdf_file:
                raise DatasetNotFoundException(dataset_identifier.dataset_id)

            dataset_group = hdf_file[dataset_identifier.dataset_id]
            for key, value in values_to_write.items():
                if key in dataset_group:
                    del dataset_group[key]
                dataset_group.create_dataset(key, data=value)

        return values_to_write

    def _get_values_all(self, dataset_identifier: AbstractDatasetInfo, data_types_dict: dict[str:str]) -> dict[str:Any]:
        """
        Get all values from a dataset for HDF5

        :param dataset_identifier: dataset identifier for connector
        :type dataset_identifier: DatasetInfo
        :param data_types_dict: dict of data type {name: type}
        :type data_types_dict: dict[str:str]
        """
        self.__logger.debug(f"Getting all values for dataset {dataset_identifier.dataset_id} for connector {self}")

        with h5py.File(self.__file_path, 'r') as hdf_file:
            if dataset_identifier.dataset_id not in hdf_file:
                raise DatasetNotFoundException(dataset_identifier.dataset_id)

            dataset_group = hdf_file[dataset_identifier.dataset_id]
            all_values = {key: dataset_group[key][()] for key in dataset_group}

        return all_values

    def get_datasets_available(self) -> list[AbstractDatasetInfo]:
        """
        Get all available datasets for a specific API
        """
        self.__logger.debug(f"Getting all datasets for connector {self}")

        with h5py.File(self.__file_path, 'r') as hdf_file:
            dataset_ids = list(hdf_file.keys())

        return [DatasetInfoV0(self.connector_id, dataset_id) for dataset_id in dataset_ids]

    def _write_dataset(
        self,
        dataset_identifier: AbstractDatasetInfo,
        values_to_write: dict[str:Any],
        data_types_dict: dict[str:str],
        create_if_not_exists: bool = True,
        override: bool = False,
    ) -> dict[str: Any]:
        """
        Write a dataset from HDF5

        :param dataset_identifier: dataset identifier for connector
        :type dataset_identifier: DatasetInfo
        :param values_to_write: dict of data to write {name: value}
        :type values_to_write: dict[str:Any]
        :param data_types_dict: dict of data types {name: type}
        :type data_types_dict: dict[str:str]
        :param create_if_not_exists: create the dataset if it does not exists (raises otherwise)
        :type create_if_not_exists: bool
        :param override: override dataset if it exists (raises otherwise)
        :type override: bool
        """
        self.__logger.debug(
            f"Writing dataset {dataset_identifier.dataset_id} for connector {self} (override={override}, create_if_not_exists={create_if_not_exists})"
        )

        with h5py.File(self.__file_path, 'a') as hdf_file:
            if dataset_identifier.dataset_id in hdf_file:
                if not override:
                    raise DatasetGenericException(f"Dataset {dataset_identifier.dataset_id} would be overridden")
                del hdf_file[dataset_identifier.dataset_id]

            dataset_group = hdf_file.create_group(dataset_identifier.dataset_id)
            for key, value in values_to_write.items():
                dataset_group.create_dataset(key, data=value)

        return values_to_write
