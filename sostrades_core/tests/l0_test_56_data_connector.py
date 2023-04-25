'''
Copyright 2022 Airbus SAS

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
'''
mode: python; py-indent-offset: 4; tab-width: 4; coding: utf-8
'''
import unittest
from os.path import join, dirname
from os import remove
from sostrades_core.tools.tree.serializer import DataSerializer
from sostrades_core.tools.rw.load_dump_dm_data import DirectLoadDump
from time import sleep
from pathlib import Path

from sostrades_core.execution_engine.data_connector.mock_connector import MockConnector
from sostrades_core.execution_engine.data_connector.data_connector_factory import ConnectorFactory
from sostrades_core.execution_engine.proxy_discipline import ProxyDiscipline
from sostrades_core.execution_engine.execution_engine import ExecutionEngine
from sostrades_core.sos_processes.test.test_disc1_data_connector_dremio.usecase import Study
from sostrades_core.execution_engine.sos_wrapp import SoSWrapp


class TestMetadataDiscipline(SoSWrapp):
    """
    Discipline to test desc_in metadata for connector
    """
    dremio_path = '"test_request"'

    data_connection_dict = {ConnectorFactory.CONNECTOR_TYPE: MockConnector.NAME,
                            'hostname': 'test_hostname',
                            'connector_request': dremio_path}

    DESC_IN = {
        'deliveries': {'type': 'float', 'value': None, 'namespace': 'ns_market_deliveries', 'editable': False,
                       ProxyDiscipline.CONNECTOR_DATA: data_connection_dict}
    }

    def run(self):
        self.deliveries = self.get_sosdisc_inputs(
            ['deliveries'], in_dict=True)
        print(self.deliveries)


class TestWriteDataDiscipline(SoSWrapp):
    """
    Discipline to test writting data with connector
    """
    dremio_path = '"test_request"'

    data_connection_dict = {ConnectorFactory.CONNECTOR_TYPE: MockConnector.NAME,
                            'hostname': 'test_hostname',
                            'connector_request': dremio_path,
                            'connector_mode': 'write'}

    DESC_OUT = {
        'deliveries_df': {'type': 'float', 'value': None, 'namespace': 'ns_market_deliveries', 'editable': False,
                          ProxyDiscipline.CONNECTOR_DATA: data_connection_dict}
    }

    def run(self):
        connector_info = self.DESC_OUT['deliveries_df'][ProxyDiscipline.CONNECTOR_DATA]
        # no need to call "write_data" method because it is done in
        # fill_output_with_connecotr in sos_discipline


class TestDataConnector(unittest.TestCase):
    """
    Data connector test class
    """

    def setUp(self):
        '''
        Initialize single disc process
        '''
        self.file_to_del = join(dirname(__file__), 'data', 'dm.pkl')
        self.name = 'EETests'
        self.model_name = 'test_'
        self.ee = ExecutionEngine(self.name)

        self.dremio_path = '"test_request"'
        self.data_connection_dict = {ConnectorFactory.CONNECTOR_TYPE: MockConnector.NAME,
                                     'hostname': 'test_hostname',
                                     'connector_request': self.dremio_path}
        self.test_connector = MockConnector()

        '''
        Initialize process for 2 disc in coupling
        '''
        self.base_path = 'sostrades_core.sos_wrapping.test_discs'
        self.dic_status = {'CONFIGURE': False,
                           'RUNNING': False,
                           'DONE': False}
        self.exec_eng = ExecutionEngine(self.name)

        ns_dict = {'ns_ac': self.name}
        self.exec_eng.ns_manager.add_ns_def(ns_dict)

        mod_path = f'{self.base_path}.disc3_data_connector.Disc3'
        disc1_builder = self.exec_eng.factory.get_builder_from_module(
            'Disc3', mod_path)

        mod_path = f'{self.base_path}.disc2_data_connector.Disc2_data_connector'
        disc2_builder = self.exec_eng.factory.get_builder_from_module(
            'Disc2_data_connector', mod_path)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            [disc1_builder, disc2_builder])

        self.exec_eng.configure()

        self.process = self.exec_eng.root_process

        '''
        Initialize process for 1 disc, data connector variable as output
        '''
        self.base_path2 = 'sostrades_core.sos_wrapping.test_discs'
        self.dic_status2 = {'CONFIGURE': False,
                            'RUNNING': False,
                            'DONE': False}
        self.exec_eng2 = ExecutionEngine(self.name)

        ns_dict2 = {'ns_ac': self.name}
        self.exec_eng2.ns_manager.add_ns_def(ns_dict2)

        mod_path2 = f'{self.base_path}.disc1_data_connector.Disc1'
        disc1_builder2 = self.exec_eng2.factory.get_builder_from_module(
            'Disc1', mod_path2)

        self.exec_eng2.factory.set_builders_to_coupling_builder(
            [disc1_builder2])

        self.exec_eng2.configure()

        self.process2 = self.exec_eng2.root_process

    def tearDown(self):
        if Path(self.file_to_del).is_file():
            remove(self.file_to_del)
            sleep(0.5)

    def test_01_data_connector_factory(self):
        """
        test the data_connector_factory
        """
        data = ConnectorFactory.use_data_connector(
            self.data_connection_dict)

        self.assertEqual(data, 42.0)

    def test_02_meta_data_desc_in(self):
        """
        Case where variable with data_connector is an input and does not come from another model.
        Data connector is needed
        """
        ns_dict = {'ns_market_deliveries': self.name}
        self.ee.ns_manager.add_ns_def(ns_dict)
        mod_path = 'sostrades_core.tests.l0_test_56_data_connector.TestMetadataDiscipline'
        builder = self.ee.factory.get_builder_from_module(
            self.model_name, mod_path)
        self.ee.factory.set_builders_to_coupling_builder(builder)
    
        self.ee.load_study_from_input_dict({})
        self.ee.display_treeview_nodes()
        self.ee.execute()
        disc = self.ee.dm.get_disciplines_with_name(
            f'{self.name}.{self.model_name}')[0]
    
        self.assertEqual(disc.mdo_discipline_wrapp.wrapper.deliveries['deliveries'], 42.0)
    
    def test_03_input_data_connector_with_coupling(self):
        """
        Case where variable with data connector comes from the output of an other model
        Data connector should not be used
        """
    
        # modify DM
        values_dict = {}
        values_dict['EETests.Disc3.a'] = 10.
        values_dict['EETests.Disc3.b'] = 5.
        values_dict['EETests.x'] = 2.
        values_dict['EETests.Disc2_data_connector.constant'] = 4.
        values_dict['EETests.Disc2_data_connector.power'] = 2
    
        self.exec_eng.load_study_from_input_dict(values_dict)
        res = self.exec_eng.execute()
        y = self.exec_eng.dm.get_value('EETests.y')
        self.assertEqual(y, 25.0)
    
    def test_04_output_data_connector(self):
        """
        Case where variable with data connector is an output.
        Data connector need to be used +read if the meta_data data_connector is saved in pickles
        """
    
        # modify DM
        values_dict = {}
        values_dict['EETests.Disc1.a'] = 10.
        values_dict['EETests.Disc1.b'] = 5.
        values_dict['EETests.x'] = 2.
    
        self.exec_eng2.load_study_from_input_dict(values_dict)
        res = self.exec_eng2.execute()
    
        y = self.exec_eng2.dm.get_value('EETests.y')
        self.assertEqual(y, 42.0)
    
        folder_path = join(dirname(__file__), 'data')
        serializer = DataSerializer()
        serializer.put_dict_from_study(
            folder_path, DirectLoadDump(), self.exec_eng2.dm.convert_data_dict_with_full_name())
        sleep(0.1)
        ref_dm_df = serializer.get_dict_from_study(
            folder_path, DirectLoadDump())
        self.assertTrue('EETests.y' in ref_dm_df.keys(), 'no y in file')
        if 'EETests.y' in ref_dm_df.keys():
            data_to_read = ref_dm_df['EETests.y']
            print(data_to_read)
            self.assertTrue(
                ProxyDiscipline.CONNECTOR_DATA in data_to_read.keys(), 'no metadata in file')
    
    def test_05_write_data(self):
        """
        Test to write data with connector
        """
        ns_dict = {'ns_market_deliveries': self.name}
        self.ee.ns_manager.add_ns_def(ns_dict)
        mod_path = 'sostrades_core.tests.l0_test_56_data_connector.TestWriteDataDiscipline'
        builder = self.ee.factory.get_builder_from_module(
            self.model_name, mod_path)
        self.ee.factory.set_builders_to_coupling_builder(builder)
    
        self.ee.load_study_from_input_dict({})
        self.ee.display_treeview_nodes()
        self.ee.execute()
        disc = self.ee.dm.get_disciplines_with_name(
            f'{self.name}.{self.model_name}')[0]
        deliveries_df = disc.get_sosdisc_outputs('deliveries_df')
    
        self.assertTrue(deliveries_df is not None)
    
    def _test_06_process_data_connector_dremio(self):
        """
        Test data connector dremio process
        """
        exec_eng = ExecutionEngine(self.name)
        builder_process = exec_eng.factory.get_builder_from_process(
            'sostrades_core.sos_processes.test', 'test_disc1_data_connector_dremio')

        exec_eng.factory.set_builders_to_coupling_builder(builder_process)

        exec_eng.configure()
    
        study_dremio = Study()
        study_dremio.study_name = self.name
        dict_values_list = study_dremio.setup_usecase()
    
        dict_values = {}
        for dict_val in dict_values_list:
            dict_values.update(dict_val)
    
        exec_eng.load_study_from_input_dict(dict_values)
    
        exec_eng.execute()

    def test_07_mongodb_connector(self):
        """
        Test Disc2 using MongoDB connector with a non existing query
        """
        exec_eng  = ExecutionEngine(self.name)
        mod_list = 'sostrades_core.sos_wrapping.test_discs.disc2.Disc2'
        builder_disc2 = exec_eng.factory.get_builder_from_module(
            'Disc2', mod_list)
        ns_dict = {'ns_ac': f'{self.name}.Disc2'}
        exec_eng.ns_manager.add_ns_def(ns_dict, get_from_database=True)
        exec_eng.factory.set_builders_to_coupling_builder(builder_disc2)
        exec_eng.configure()
        values_dict = {}
        values_dict[f'{self.name}.Disc2.database_id'] = 'Disc2Tes5t'
        with self.assertRaises(Exception):
            exec_eng.load_study_from_input_dict(values_dict)

    def test_08_mongodb_connector(self):
        """
        Test Disc2 using MongoDB connector 
        """
        exec_eng  = ExecutionEngine(self.name)
        mod_list = 'sostrades_core.sos_wrapping.test_discs.disc2.Disc2'
        builder_disc2 = exec_eng.factory.get_builder_from_module(
            'Disc2', mod_list)
        power_in = 2 
        constant_in = 8
        y_in = 3
        ns_dict = {'ns_ac': f'{self.name}.Disc2'}
        exec_eng.ns_manager.add_ns_def(ns_dict, get_from_database=True)
        exec_eng.ns_manager.database_activated = True
        exec_eng.factory.set_builders_to_coupling_builder(builder_disc2)
        exec_eng.configure()
        # set data
        values_dict = {}
        values_dict[f'{self.name}.Disc2.power'] = power_in
        values_dict[f'{self.name}.Disc2.constant'] = constant_in
        values_dict[f'{self.name}.Disc2.y'] = y_in
        exec_eng.load_study_from_input_dict(values_dict)
        # set database information
        values_dict = {}
        values_dict[f'{self.name}.Disc2.database_id'] = 'Disc2Test'
        values_dict[f'{self.name}.Disc2.database_subname'] = 'DiscTest'
        exec_eng.load_study_from_input_dict(values_dict)
        exec_eng.configure()

        dm = exec_eng.dm 
        y = dm.get_value(f'{self.name}.Disc2.y')
        # y_db is value in database
        y_db = 2
        # assert value in dm is the value coming from the database
        assert y == y_db



    def test_09_mongodb_connector_local_variables(self):
        """
        Test Disc2 using MongoDB connector on local variables (power and constant are declared as local variables)
        """
        exec_eng  = ExecutionEngine(self.name)
        mod_list = 'sostrades_core.sos_wrapping.test_discs.disc2.Disc2'
        builder_disc2 = exec_eng.factory.get_builder_from_module(
            'Disc2', mod_list)
        builder_disc2.set_builder_info('local_namespace_database', True)
        power_in = 2 
        constant_in = 8
        y_in = 3
        ns_dict = {'ns_ac': f'{self.name}.Disc2'}
        exec_eng.ns_manager.add_ns_def(ns_dict, get_from_database=True)
        exec_eng.ns_manager.database_activated = True
        exec_eng.factory.set_builders_to_coupling_builder(builder_disc2)
        exec_eng.configure()
        # set data
        values_dict = {}
        #values_dict[f'{self.name}.Disc2.power'] = power_in
        values_dict[f'{self.name}.Disc2.constant'] = constant_in
        values_dict[f'{self.name}.Disc2.y'] = y_in
        exec_eng.load_study_from_input_dict(values_dict)
        # set database information
        values_dict = {}
        values_dict[f'{self.name}.Disc2.database_id'] = 'Disc2Test'
        values_dict[f'{self.name}.Disc2.database_subname'] = 'DiscTest'
        exec_eng.load_study_from_input_dict(values_dict)
        exec_eng.configure()

        dm = exec_eng.dm 
        power = dm.get_value(f'{self.name}.Disc2.power')
        power_db = 3
        assert power == power_db



if '__main__' == __name__:
    
    testcls = TestDataConnector()
    testcls.setUp()
    testcls.test_04_output_data_connector()


