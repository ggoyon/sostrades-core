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
from typing import Any
import h5py

from sostrades_core.datasets.dataset_info.abstract_dataset_info import AbstractDatasetInfo
from sostrades_core.datasets.datasets_connectors.abstract_datasets_connector import (
    AbstractDatasetsConnector,
    DatasetGenericException,
    DatasetNotFoundException,
)
from sostrades_core.datasets.datasets_serializers.datasets_serializer_factory import (
    DatasetSerializerType,
    DatasetsSerializerFactory,
)


class HDF5DatasetsConnector(AbstractDatasetsConnector):
    """
    Specific dataset connector for dataset in hdf5 format
    """

    def __init__(self, connector_id: str, file_path: str, serializer_type: DatasetSerializerType = DatasetSerializerType.JSON):
        """
        Constructor for HDF5 data connector

        :param file_path: file_path for this dataset connector
        :type file_path: str
        :param serializer_type: type of serializer to deserialize data from connector
        :type serializer_type: DatasetSerializerType (JSON for jsonDatasetSerializer)
        """
        super().__init__()
        self.__logger = logging.getLogger(__name__)
        self.__logger.debug("Initializing HDF5 connector")
        self._datasets_serializer = DatasetsSerializerFactory.get_serializer(serializer_type)
        self.connector_id = connector_id
        self.file_path = file_path

    def _get_values(self, dataset_identifier: AbstractDatasetInfo, data_to_get: dict[str:str]) -> dict[str:Any]:
        """
        Method to retrieve data from HDF5 and fill a data_dict

        :param dataset_identifier: identifier of the dataset
        :type dataset_identifier: AbstractDatasetInfo

        :param data_to_get: data to retrieve, dict of names and types
        :type data_to_get: dict[str:str]
        """
        self.__logger.debug(f"Getting values {data_to_get.keys()} for dataset {dataset_identifier.dataset_id} for connector {self}")
        result_data = {}
        with h5py.File(self.file_path, 'r') as hdf:
            if dataset_identifier.dataset_id not in hdf:
                raise DatasetNotFoundException(dataset_identifier.dataset_id)
            dataset_group = hdf[dataset_identifier.dataset_id]
            for key, data_type in data_to_get.items():
                if key in dataset_group:
                    result_data[key] = self._datasets_serializer.convert_from_dataset_data(key, dataset_group[key][()], {key: data_type})
        self.__logger.debug(f"Values obtained {list(result_data.keys())} for dataset {dataset_identifier.dataset_id} for connector {self}")
        return result_data

    def get_datasets_available(self) -> list[AbstractDatasetInfo]:
        """
        Get all available datasets for a specific API
        """
        self.__logger.debug(f"Getting all datasets for connector {self}")
        datasets = []
        with h5py.File(self.file_path, 'r') as hdf:
            for dataset_id in hdf.keys():
                datasets.append(AbstractDatasetInfo(self.connector_id, dataset_id))
        return datasets

    def _write_values(self, dataset_identifier: AbstractDatasetInfo, values_to_write: dict[str:Any], data_types_dict: dict[str:str]) -> dict[str: Any]:
        """
        Method to write data

        :param dataset_identifier: dataset identifier for connector
        :type dataset_identifier: AbstractDatasetInfo
        :param values_to_write: dict of data to write {name: value}
        :type values_to_write: dict[str:Any]
        :param data_types_dict: dict of data type {name: type}
        :type data_types_dict: dict[str:str]
        """
        self.__logger.debug(f"Writing values in dataset {dataset_identifier.dataset_id} for connector {self}")
        with h5py.File(self.file_path, 'a') as hdf:
            if dataset_identifier.dataset_id not in hdf:
                dataset_group = hdf.create_group(dataset_identifier.dataset_id)
            else:
                dataset_group = hdf[dataset_identifier.dataset_id]
            for key, value in values_to_write.items():
                if key in dataset_group:
                    del dataset_group[key]
                dataset_group.create_dataset(key, data=self._datasets_serializer.convert_to_dataset_data(key, value, data_types_dict))
        return values_to_write

    def _get_values_all(self, dataset_identifier: AbstractDatasetInfo, data_types_dict: dict[str:str]) -> dict[str:Any]:
        """
        Get all values from a dataset for HDF5
        :param dataset_identifier: dataset identifier for connector
        :type dataset_identifier: AbstractDatasetInfo
        :param data_types_dict: dict of data type {name: type}
        :type data_types_dict: dict[str:str]
        """
        self.__logger.debug(f"Getting all values for dataset {dataset_identifier.dataset_id} for connector {self}")
        result_data = {}
        with h5py.File(self.file_path, 'r') as hdf:
            if dataset_identifier.dataset_id not in hdf:
                raise DatasetNotFoundException(dataset_identifier.dataset_id)
            dataset_group = hdf[dataset_identifier.dataset_id]
            for key in dataset_group.keys():
                result_data[key] = self._datasets_serializer.convert_from_dataset_data(key, dataset_group[key][()], data_types_dict)
        return result_data

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
        :type dataset_identifier: AbstractDatasetInfo
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
        with h5py.File(self.file_path, 'a') as hdf:
            if dataset_identifier.dataset_id not in hdf:
                if create_if_not_exists:
                    dataset_group = hdf.create_group(dataset_identifier.dataset_id)
                else:
                    raise DatasetNotFoundException(dataset_identifier.dataset_id)
            else:
                if not override:
                    raise DatasetGenericException(f"Dataset {dataset_identifier.dataset_id} would be overriden")
                dataset_group = hdf[dataset_identifier.dataset_id]
                for key in dataset_group.keys():
                    del dataset_group[key]
            for key, value in values_to_write.items():
                dataset_group.create_dataset(key, data=self._datasets_serializer.convert_to_dataset_data(key, value, data_types_dict))
        return values_to_write
