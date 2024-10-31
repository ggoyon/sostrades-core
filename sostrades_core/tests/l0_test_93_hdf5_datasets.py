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
import os
import unittest

import numpy as np
import pandas as pd
from gemseo.utils.compare_data_manager_tooling import dict_are_equal

from sostrades_core.datasets.dataset_info.dataset_info_v0 import DatasetInfoV0
from sostrades_core.datasets.datasets_connectors.datasets_connector_factory import DatasetConnectorType
from sostrades_core.datasets.datasets_connectors.datasets_connector_manager import DatasetsConnectorManager
from sostrades_core.study_manager.study_manager import StudyManager
from sostrades_core.datasets.dataset_mapping import DatasetsMapping


class TestHDF5Datasets(unittest.TestCase):
    """
    Discipline to test HDF5 datasets
    """

    def setUp(self):
        # Set logging level to debug for datasets
        logging.getLogger("sostrades_core.datasets").setLevel(logging.DEBUG)

        # nested types reference values to be completed with more nested types
        df1 = pd.DataFrame({'years': [2020, 2021, 2022],
                            'type': ['alpha', 'beta', 'gamma']})
        df2 = pd.DataFrame({'years': [2020, 2021, 2022],
                            'price': [20.33, 60.55, 72.67]})
        dict_df = {'df1': df1.copy(), 'df2': df2.copy()}
        dict_dict_df = {'dict': {'df1': df1.copy(), 'df2': df2.copy()}}
        dict_dict_float = {'dict': {'f1': 0.033, 'f2': 333.66}}
        array_string = np.array(['s1', 's2'])
        array_df = np.array([df1.copy(), df2.copy()])
        dspace_dict_lists = {'variable': ['x', 'z', 'y_1', 'y_2'],
                             'value': [[1.], [5., 2.], [1.], [1.]],
                             'lower_bnd': [[0.], [-10., 0.], [-100.], [-100.]],
                             'upper_bnd': [[10.], [10., 10.], [100.], [100.]],
                             'enable_variable': [True, True, True, True],
                             'activated_elem': [[True], [True, True], [True], [True]]}
        dspace_dict_array = {'variable': ['x', 'z', 'y_1', 'y_2'],
                             'value': [np.array([1.]), np.array([5., 2.]), np.array([1.]), np.array([1.])],
                             'lower_bnd': [np.array([0.]), np.array([-10., 0.]), np.array([-100.]), np.array([-100.])],
                             'upper_bnd': [np.array([10.]), np.array([10., 10.]), np.array([100.]), np.array([100.])],
                             'enable_variable': [True, True, True, True],
                             'activated_elem': [[True], [True, True], [True], [True]]}

        dspace_lists = pd.DataFrame(dspace_dict_lists)
        dspace_array = pd.DataFrame(dspace_dict_array)

        self.nested_types_reference_dict = {'X_dict_df': dict_df,
                                            'X_dict_dict_df': dict_dict_df,
                                            'X_dict_dict_float': dict_dict_float,
                                            'X_array_string': array_string,
                                            'X_array_df': array_df,
                                            'X_dspace_lists': dspace_lists,
                                            'X_dspace_array': dspace_array,
                                            }

    def test_hdf5_datasets(self):
        connector_args = {
            "file_path": "./sostrades_core/tests/data/test_hdf5_datasets.h5",
            "create_if_not_exists": True
        }
        DatasetsConnectorManager.register_connector(connector_identifier="hdf5_datasets_connector",
                                                    connector_type=DatasetConnectorType.HDF5,
                                                    **connector_args)
        usecase_file_path = "./sostrades_core/tests/data/usecase_hdf5_dataset.py"
        process_path = os.path.dirname(usecase_file_path)
        study = StudyManager(file_path=usecase_file_path)

        dm = study.execution_engine.dm
        connector_to = DatasetsConnectorManager.get_connector('hdf5_datasets_connector')

        dataset_vars = ["a",
                        "x",
                        "b",
                        "name",
                        "x_dict",
                        "y_array",
                        "z_list",
                        "b_bool",
                        "d"]

        data_types_dict = {_k: dm.get_data(f"usecase_hdf5_dataset.Disc1.{_k}", "type") for _k in dataset_vars}

        try:
            connector_to.copy_dataset_from(connector_from=connector_to,
                                           dataset_identifier=DatasetInfoV0(connector_to,"dataset_hdf5"),
                                           data_types_dict=data_types_dict,
                                           create_if_not_exists=True)

            study.update_data_from_dataset_mapping(
                DatasetsMapping.from_json_file(os.path.join(process_path, "usecase_hdf5_dataset.json")))
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.a"), 1)
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.x"), 4.0)
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.b"), 2)
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.name"), "A1")
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.x_dict"), {"test1": 1, "test2": 2})
            self.assertTrue(np.array_equal(dm.get_value("usecase_hdf5_dataset.Disc1.y_array"), np.array([1.0, 2.0, 3.0])))
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.z_list"), [1.0, 2.0, 3.0])
            self.assertEqual(dm.get_value("usecase_hdf5_dataset.Disc1.b_bool"), False)
            self.assertTrue((dm.get_value("usecase_hdf5_dataset.Disc1.d") == pd.DataFrame({"years": [2023, 2024], "x": [1.0, 10.0]})).all().all())
            os.remove(connector_args["file_path"])
        except Exception as cm:
            os.remove(connector_args["file_path"])
            raise cm


if __name__ == "__main__":
    cls = TestHDF5Datasets()
    cls.setUp()
    cls.test_hdf5_datasets()
