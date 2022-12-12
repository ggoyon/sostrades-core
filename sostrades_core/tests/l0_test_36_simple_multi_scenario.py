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
from time import sleep
from shutil import rmtree
from pathlib import Path
from os.path import join
import pandas as pd
import numpy as np
from logging import Handler

from sostrades_core.execution_engine.execution_engine import ExecutionEngine
from tempfile import gettempdir
from sostrades_core.tools.rw.load_dump_dm_data import DirectLoadDump
from sostrades_core.study_manager.base_study_manager import BaseStudyManager

class UnitTestHandler(Handler):
    """
    Logging handler for UnitTest
    """

    def __init__(self):
        Handler.__init__(self)
        self.msg_list = []

    def emit(self, record):
        self.msg_list.append(record.msg)

class TestSimpleMultiScenario(unittest.TestCase):
    """
    SoSSimpleMultiScenario test class
    """


    def setUp(self):
        '''
        Initialize third data needed for testing
        '''
        self.dirs_to_del = []
        self.namespace = 'MyCase'
        self.study_name = f'{self.namespace}'
        self.repo = 'sostrades_core.sos_processes.test'
        self.base_path = 'sostrades_core.sos_wrapping.test_discs'
        self.exec_eng = ExecutionEngine(self.namespace)
        self.factory = self.exec_eng.factory
        self.root_dir = gettempdir()
        self.my_handler = UnitTestHandler()
        self.exec_eng.logger.addHandler(self.my_handler)

        # reference var values
        self.x1 = 2.
        self.a1 = 3
        self.constant = 3
        self.power = 2
        self.b1 = 4
        self.b2 = 2
        self.z1 = 1.2
        self.z2 = 1.5

    def tearDown(self):

        for dir_to_del in self.dirs_to_del:
            sleep(0.5)
            if Path(dir_to_del).is_dir():
                rmtree(dir_to_del)
        sleep(0.5)

    # NEW EEV4 TESTS
    def test_01_multi_instance_configuration_from_df_without_reference_scenario(self):
        # # simple 2-disc process NOT USING nested scatters
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,
                                                                  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()

        # build the scenarios
        dict_values = {}
        scenario_df = pd.DataFrame({'selected_scenario': [True, False, True],
                                    'scenario_name': ['scenario_1',
                                                      'scenario_W',
                                                      'scenario_2']})
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        dict_values[f'{self.study_name}.multi_scenarios.builder_mode'] = 'multi_instance'
        # dict_values[f'{self.study_name}.Eval.instance_reference'] = False
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        # configure the scenarios
        scenario_list = ['scenario_1', 'scenario_2']

        for scenario in scenario_list:
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.a'] = self.a1
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.x'] = self.x1
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.constant'] = self.constant
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.power'] = self.power

        self.exec_eng.load_study_from_input_dict(dict_values)

        # configure b from a dataframe
        scenario_df = pd.DataFrame({'selected_scenario': [True, False, True],
                                    'scenario_name': ['scenario_1',
                                                      'scenario_W',
                                                      'scenario_2'],
                                    'Disc1.b': [self.b1, 1e6, self.b2],
                                    'z': [self.z1, 1e6, self.z2]})
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_df')['scenario_name'].values.tolist(),  ['scenario_1',
                                                                                      'scenario_W',
                                                                                      'scenario_2'])
        ms_disc = self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios')[0]
        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]

        self.assertEqual(ms_sub_disc_names, ['scenario_1',
                                             'scenario_2'])

        y1 = self.a1 * self.x1 + self.b1
        y2 = self.a1 * self.x1 + self.b2
        o1 = self.constant + self.z1 ** self.power
        o2 = self.constant + self.z2 ** self.power

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

    def test_02_multi_instance_configuration_from_df_with_reference_scenario(self):
        # # simple 2-disc process NOT USING nested scatters
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,
                                                                  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()

        # build the scenarios
        dict_values = {}
        scenario_df = pd.DataFrame({'selected_scenario': [True, False, True],
                                    'scenario_name': ['scenario_1',
                                                      'scenario_W',
                                                      'scenario_2']})
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        dict_values[f'{self.study_name}.multi_scenarios.builder_mode'] = 'multi_instance'
        dict_values[f'{self.study_name}.multi_scenarios.instance_reference'] = True
        dict_values[f'{self.study_name}.multi_scenarios.reference_mode'] = 'linked_mode'
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        # reference var values
        self.x = 2.
        self.a = 3
        self.constant = 3
        self.power = 2
        self.b = 8
        self.z = 12

        # configure the Reference scenario
        # Non-trade variables (to propagate)
        dict_values[f'{self.study_name}.multi_scenarios.ReferenceScenario.a'] = self.a
        dict_values[f'{self.study_name}.multi_scenarios.ReferenceScenario.x'] = self.x
        dict_values[self.study_name + '.multi_scenarios.ReferenceScenario.Disc3.constant'] = self.constant
        dict_values[self.study_name + '.multi_scenarios.ReferenceScenario.Disc3.power'] = self.power
        # Trade variables reference (not to propagate)
        dict_values[self.study_name + '.multi_scenarios.ReferenceScenario.Disc1.b'] = self.b
        dict_values[self.study_name + '.multi_scenarios.ReferenceScenario.z'] = self.z
        self.exec_eng.load_study_from_input_dict(dict_values)

        # Configure b and z of others scenarios (trade variables)
        scenario_df = pd.DataFrame({'selected_scenario': [True, False, True],
                                    'scenario_name': ['scenario_1',
                                                      'scenario_W',
                                                      'scenario_2'],
                                    'Disc1.b': [self.b1, 1e6, self.b2],
                                    'z': [self.z1, 1e6, self.z2]})
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        self.exec_eng.load_study_from_input_dict(dict_values)

        # Check trades variables are ok and that non-trade variables have been propagated to other scenarios
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_1.Disc1.b'), self.b1)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_2.Disc1.b'), self.b2)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_1.z'), self.z1)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_2.z'), self.z2)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_1.a'), self.a)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_2.a'), self.a)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_1.x'), self.x)
        self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.scenario_2.x'), self.x)
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.' +
                                                        scenario + '.Disc3.constant'), self.constant)
            self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.' +
                                                        scenario + '.Disc3.power'), self.power)
        self.exec_eng.execute()

        # Change non-trade variable value from reference and check it has induced a reconfiguration and re-propagation
        new_constant_ref = 23
        dict_values[self.study_name + '.multi_scenarios.ReferenceScenario.Disc3.constant'] = new_constant_ref
        self.exec_eng.load_study_from_input_dict(dict_values)
        for scenario in scenario_list:
            self.assertEqual(self.exec_eng.dm.get_value(self.study_name + '.multi_scenarios.' +
                                                        scenario + '.Disc3.constant'), new_constant_ref)

        self.exec_eng.execute()

        # Now, check that, since we are in LINKED_MODE, that the non-trade variables from non-reference scenarios have
        # 'editable' in False.
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_1.a', 'editable'), False)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_2.a', 'editable'), False)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_1.x', 'editable'), False)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_2.x', 'editable'), False)
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.' +
                                                        scenario + '.Disc3.constant', 'editable'), False)
            self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.' +
                                                        scenario + '.Disc3.power', 'editable'), False)

        self.assertEqual(self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df')['scenario_name'].values.tolist(),
                         ['scenario_1', 'scenario_W', 'scenario_2'])
        ms_disc = self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios')[0]
        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]
        for sc in ['scenario_1','scenario_2']:
            assert sc in ms_sub_disc_names

        # Now, change to REFERENCE_MODE to COPY_MODE and check that the non-trade variables from non-reference scenarios have
        # 'editable' in True.
        dict_values[f'{self.study_name}.multi_scenarios.reference_mode'] = 'copy_mode'
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_1.a', 'editable'), True)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_2.a', 'editable'), True)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_1.x', 'editable'), True)
        self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.scenario_2.x', 'editable'), True)
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.' +
                                                       scenario + '.Disc3.constant', 'editable'), True)
            self.assertEqual(self.exec_eng.dm.get_data(self.study_name + '.multi_scenarios.' +
                                                       scenario + '.Disc3.power', 'editable'), True)


    def test_03_consecutive_configure(self):
        # # simple 2-disc process NOT USING nested scatters
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,
                                                                  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])
        dict_values = {f'{self.study_name}.multi_scenarios.builder_mode': 'multi_instance',
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()
        ms_disc = self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios')[0]
        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]
        self.assertEqual(ms_sub_disc_names, ['scenario_1'])

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', True, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])

        dict_values[f'{self.study_name}.multi_scenarios.scenario_df']= scenario_df

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]
        self.assertEqual(ms_sub_disc_names, ['scenario_1',
                                             'scenario_2'])

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', False, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])

        dict_values[f'{self.study_name}.multi_scenarios.scenario_df']= scenario_df

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]
        self.assertEqual(ms_sub_disc_names, ['scenario_1'])

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', True, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])

        dict_values[self.study_name + '.multi_scenarios.scenario_df']= scenario_df

        self.exec_eng.load_study_from_input_dict(dict_values)
        ms_sub_disc_names = [d.sos_name for d in ms_disc.proxy_disciplines]
        self.assertEqual(ms_sub_disc_names, ['scenario_1',
                                             'scenario_2'])

        # manually configure the scenarios non-varying values (~reference)
        private_val = {}
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.a'] = self.a1
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.x'] = self.x1
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.constant'] = self.constant
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.power'] = self.power
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.z'] = self.z1
        self.exec_eng.load_study_from_input_dict(private_val)
        self.exec_eng.execute()

        self.assertEqual(self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_1.Disc1.b'), self.b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_2.Disc1.b'), self.b2)

        y1, o1 = (self.a1 * self.x1 + self.b1, self.constant + self.z1 ** self.power)
        y2, o2 = (self.a1 * self.x1 + self.b2, self.constant + self.z1 ** self.power)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

    def test_04_dump_and_load_after_execute_with_2_trade_vars(self):
        # # simple 2-disc process NOT USING nested scatters
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()
        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1, self.z1], ['scenario_2', True, self.b2, self.z2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b', 'z'])

        dict_values = {f'{self.study_name}.multi_scenarios.builder_mode': 'multi_instance',
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}

        self.exec_eng.load_study_from_input_dict(dict_values)

        # manually configure the scenarios non-varying values (~reference)
        private_val = {}
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.a'] = self.a1
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.x'] = self.x1
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.constant'] = self.constant
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.power'] = self.power
            private_val[f'{self.study_name}.multi_scenarios.{scenario}.z'] = self.z1
        self.exec_eng.load_study_from_input_dict(private_val)
        self.exec_eng.execute()

        y1, o1 = (self.a1 * self.x1 + self.b1, self.constant + self.z1 ** self.power)
        y2, o2 = (self.a1 * self.x1 + self.b2, self.constant + self.z1 ** self.power)
        dump_dir = join(self.root_dir, self.namespace)

        BaseStudyManager.static_dump_data(
            dump_dir, self.exec_eng, DirectLoadDump())

        exec_eng2 = ExecutionEngine(self.namespace)
        builders = exec_eng2.factory.get_builder_from_process(self.repo, proc_name)
        exec_eng2.factory.set_builders_to_coupling_builder(builders)
        exec_eng2.configure()

        BaseStudyManager.static_load_data(
            dump_dir, exec_eng2, DirectLoadDump())
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

        self.assertEqual(exec_eng2.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(exec_eng2.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(exec_eng2.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(exec_eng2.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

        # Clean the dump folder at the end of the test
        self.dirs_to_del.append(
            join(self.root_dir, self.namespace))

    def test_08_changing_trade_variables_by_adding_df_column(self):
        # # simple 2-disc process NOT USING nested scatters
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()
        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', True, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])

        dict_values = {f'{self.study_name}.multi_scenarios.builder_mode': 'multi_instance',
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}

        self.exec_eng.load_study_from_input_dict(dict_values)
        # manually configure the scenarios non-varying values (~reference)
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.a'] = self.a1
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.x'] = self.x1
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.constant'] = self.constant
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.Disc3.power'] = self.power
            dict_values[f'{self.study_name}.multi_scenarios.{scenario}.z'] = self.z1
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.execute()
        y1, o1 = (self.a1 * self.x1 + self.b1, self.constant + self.z1 ** self.power)
        y2, o2 = (self.a1 * self.x1 + self.b2, self.constant + self.z1 ** self.power)

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1, self.z2], ['scenario_2', True, self.b2, self.z2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b', 'z'])
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df']= scenario_df
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.execute()
        y1, o1 = (self.a1 * self.x1 + self.b1, self.constant + self.z2 ** self.power)
        y2, o2 = (self.a1 * self.x1 + self.b2, self.constant + self.z2 ** self.power)

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.y'), y1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.y'), y2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.o'), o1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.o'), o2)

    def test_09_two_scenarios_with_same_name(self):
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,
                                                                  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', False, 0], ['scenario_1', True, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])
        dict_values = {f'{self.study_name}.multi_scenarios.builder_mode': 'multi_instance',
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}

        error_message = 'Cannot activate several scenarios with the same name (scenario_1).'
        exp_tv = 'Nodes representation for Treeview MyCase\n' \
                 '|_ MyCase\n' \
                 '\t|_ multi_scenarios'


        ## Exception
        # with self.assertRaises(Exception) as cm:
        #     self.exec_eng.load_study_from_input_dict(dict_values)
        # self.assertEqual(str(cm.exception), error_message)

        ## Logging only
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.assertEqual(exp_tv, self.exec_eng.display_treeview_nodes())
        self.assertIn(error_message, self.my_handler.msg_list)

    def test_10_two_scenarios_with_same_name_on_2nd_config(self):
        proc_name = 'test_multi_instance_basic'
        builders = self.exec_eng.factory.get_builder_from_process(self.repo,
                                                                  proc_name)
        self.exec_eng.factory.set_builders_to_coupling_builder(builders)
        self.exec_eng.configure()

        scenario_df = pd.DataFrame(
            [['scenario_1', True, self.b1], ['scenario_2', False, 0], ['scenario_2', True, self.b2]], columns=['scenario_name', 'selected_scenario', 'Disc1.b'])

        dict_values = {f'{self.study_name}.multi_scenarios.builder_mode': 'multi_instance',
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}
        self.exec_eng.load_study_from_input_dict(dict_values)

        scenario_df['scenario_name'].iloc[2] = 'scenario_1'
        self.exec_eng.load_study_from_input_dict(dict_values)

        error_message = 'Cannot activate several scenarios with the same name (scenario_1).'
        exp_tv = 'Nodes representation for Treeview MyCase\n' \
                 '|_ MyCase\n' \
                 '\t|_ multi_scenarios\n' \
                 '\t\t|_ scenario_1\n' \
                 '\t\t\t|_ Disc1\n' \
                 '\t\t\t|_ Disc3\n' \
                 '\t\t|_ scenario_2\n' \
                 '\t\t\t|_ Disc1\n' \
                 '\t\t\t|_ Disc3'

        ## Exception
        # with self.assertRaises(Exception) as cm:
        #     self.exec_eng.load_study_from_input_dict(dict_values)
        # self.assertEqual(str(cm.exception), error_message)

        ## Logging only
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.assertEqual(exp_tv, self.exec_eng.display_treeview_nodes())
        self.assertIn(error_message, self.my_handler.msg_list)

    ## EEV3 TESTS #TODO: cleanup when nested scatter exists
    def _test_01_multi_scenario_of_scatter(self):

        # scatter build map
        ac_map = {'input_name': 'name_list',
                  'input_type': 'string_list',
                  'input_ns': 'ns_scatter_scenario',
                  'output_name': 'ac_name',
                  'scatter_ns': 'ns_ac',
                  'gather_ns': 'ns_scenario',
                  'ns_to_update': ['ns_data_ac']}

        self.exec_eng.scattermap_manager.add_build_map('name_list', ac_map)

        # scenario build map
        scenario_map = {'input_name': 'scenario_list',
                        'input_type': 'string_list',
                        'input_ns': 'ns_scatter_scenario',
                        'output_name': 'scenario_name',
                        'scatter_ns': 'ns_scenario',
                        'gather_ns': 'ns_scatter_scenario',
                        'ns_to_update': ['ns_disc3', 'ns_barrierr', 'ns_out_disc3']}

        self.exec_eng.scattermap_manager.add_build_map(
            'scenario_list', scenario_map)

        # shared namespace
        self.exec_eng.ns_manager.add_ns('ns_barrierr', 'MyCase')
        self.exec_eng.ns_manager.add_ns(
            'ns_scatter_scenario', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_disc3', 'MyCase.multi_scenarios.Disc3')
        self.exec_eng.ns_manager.add_ns(
            'ns_out_disc3', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_data_ac', 'MyCase')

        # instantiate factory # get instantiator from Discipline class
        builder_list = self.factory.get_builder_from_process(repo=self.repo,
                                                             mod_id='test_disc1_scenario')

        mod_list = f'{self.base_path}.disc3_scenario.Disc3'
        disc3_builder = self.exec_eng.factory.get_builder_from_module(
            'Disc3', mod_list)
        builder_list.append(disc3_builder)

        # TODO: handle autogather input order and set to True...
        multi_scenarios = self.exec_eng.factory.create_driver(
            'multi_scenarios', cls_builder=builder_list, map_name='scenario_list')

        self.exec_eng.factory.set_builders_to_coupling_builder(
            multi_scenarios)
        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        dict_values = {
            f'{self.study_name}.multi_scenarios.trade_variables': {'x': 'float'}}

        self.exec_eng.load_study_from_input_dict(dict_values)
        scenario_df = pd.DataFrame(columns=['scenario_name', 'x'])

        self.assertTrue(scenario_df.equals(
            self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df')))

        x1 = 2.
        x2 = 4.
        scenario_df = pd.DataFrame(
            [['scenario_1', x1], ['scenario_2', x2]], columns=['scenario_name', 'x'])
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_df = pd.DataFrame([['scenario_1', x1], ['scenario_2', x2]], columns=[
                                   'scenario_name', 'x'])
        print(scenario_df)
        print(self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df'))
        self.assertTrue(scenario_df.equals(
            self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df')))

        dict_values = {self.study_name +
                       '.multi_scenarios.name_list': ['name_1', 'name_2']}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        private_val = {}
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2

            private_val[self.study_name + '.name_1.a'] = a1
            private_val[self.study_name + '.name_2.a'] = a2
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_1.z'] = 1.2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_2.z'] = 1.5

        self.exec_eng.load_study_from_input_dict(private_val)

        self.exec_eng.execute()

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.x'), x1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.x'), x2)

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_2.y'), a2 * x1 + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.y'), a1 * x2 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_2.y'), a2 * x2 + b2)

    def _test_02_multi_scenario_of_scatter_name1_x_trade_variable(self):

        # scatter build map
        ac_map = {'input_name': 'name_list',
                  'input_type': 'string_list',
                  'input_ns': 'ns_scatter_scenario',
                  'output_name': 'ac_name',
                  'scatter_ns': 'ns_ac',
                  'gather_ns': 'ns_scenario',
                  'ns_to_update': ['ns_data_ac']}

        self.exec_eng.scattermap_manager.add_build_map('name_list', ac_map)

        # scenario build map
        scenario_map = {'input_name': 'scenario_list',
                        'input_type': 'string_list',
                        'input_ns': 'ns_scatter_scenario',
                        'output_name': 'scenario_name',
                        'scatter_ns': 'ns_scenario',
                        'gather_ns': 'ns_scatter_scenario',
                        'ns_to_update': ['ns_disc3', 'ns_barrierr', 'ns_out_disc3']}

        self.exec_eng.scattermap_manager.add_build_map(
            'scenario_list', scenario_map)

        # shared namespace
        self.exec_eng.ns_manager.add_ns('ns_barrierr', 'MyCase')
        self.exec_eng.ns_manager.add_ns(
            'ns_scatter_scenario', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_disc3', 'MyCase.multi_scenarios.Disc3')
        self.exec_eng.ns_manager.add_ns(
            'ns_out_disc3', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_data_ac', 'MyCase')

        # instantiate factory # get instantiator from Discipline class

        builder_list = self.factory.get_builder_from_process(repo=self.repo,
                                                             mod_id='test_disc1_scenario')

        scatter_list = self.exec_eng.factory.create_multi_scatter_builder_from_list(
            'name_list', builder_list=builder_list, autogather=True)

        mod_list = f'{self.base_path}.disc3_scenario.Disc3'
        disc3_builder = self.exec_eng.factory.get_builder_from_module(
            'Disc3', mod_list)
        scatter_list.append(disc3_builder)

        multi_scenarios = self.exec_eng.factory.create_simple_multi_scenario_builder(
            'multi_scenarios', 'scenario_list', scatter_list, autogather=True)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            multi_scenarios)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        dict_values = {
            f'{self.study_name}.multi_scenarios.trade_variables': {'name_1.x': 'float'}}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_df = pd.DataFrame(columns=['scenario_name', 'name_1.x'])
        self.assertTrue(scenario_df.equals(
            self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df')))

        x1 = 2.
        x2 = 4.
        scenario_df = pd.DataFrame(
            [['scenario_1', x1], ['scenario_2', x2]], columns=['scenario_name', 'name_1.x'])
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        self.assertTrue(scenario_df.equals(
            self.exec_eng.dm.get_value('MyCase.multi_scenarios.scenario_df')))

        dict_values[self.study_name +
                    '.multi_scenarios.name_list'] = ['name_1', 'name_2']

        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.display_treeview_nodes(display_variables='var_name')

        private_val = {}
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2
            x2b = 5.0

            private_val[self.study_name + '.name_1.a'] = a1
            private_val[self.study_name + '.name_2.a'] = a2
            private_val[self.study_name + '.name_2.x'] = x2b
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_1.z'] = 1.2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_2.z'] = 1.5

        dict_values.update(private_val)
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        x1 = 2
        x2 = 4

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.x'), x1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.x'), x2)

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_2.y'), a2 * x2b + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.y'), a1 * x2 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_2.y'), a2 * x2b + b2)

    def _test_03_consecutive_configure(self):

        # scatter build map
        ac_map = {'input_name': 'name_list',
                  'input_type': 'string_list',
                  'input_ns': 'ns_scatter_scenario',
                  'output_name': 'ac_name',
                  'scatter_ns': 'ns_ac',
                  'gather_ns': 'ns_scenario',
                  'ns_to_update': ['ns_data_ac']}

        self.exec_eng.scattermap_manager.add_build_map('name_list', ac_map)

        # scenario build map
        scenario_map = {'input_name': 'scenario_list',
                        'input_type': 'string_list',
                        'input_ns': 'ns_scatter_scenario',
                        'output_name': 'scenario_name',
                        'scatter_ns': 'ns_scenario',
                        'gather_ns': 'ns_scatter_scenario',
                        'ns_to_update': ['ns_disc3', 'ns_barrierr', 'ns_out_disc3']}

        self.exec_eng.scattermap_manager.add_build_map(
            'scenario_list', scenario_map)

        # shared namespace
        self.exec_eng.ns_manager.add_ns('ns_barrierr', 'MyCase')
        self.exec_eng.ns_manager.add_ns(
            'ns_scatter_scenario', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_disc3', 'MyCase.multi_scenarios.Disc3')
        self.exec_eng.ns_manager.add_ns(
            'ns_out_disc3', 'MyCase.multi_scenarios')
        self.exec_eng.ns_manager.add_ns(
            'ns_data_ac', 'MyCase')

        # instantiate factory # get instantiator from Discipline class
        builder_list = self.factory.get_builder_from_process(repo=self.repo,
                                                             mod_id='test_disc1_scenario')

        scatter_list = self.exec_eng.factory.create_multi_scatter_builder_from_list(
            'name_list', builder_list=builder_list, autogather=True)

        mod_path = f'{self.base_path}.disc3_scenario.Disc3'
        disc3_builder = self.exec_eng.factory.get_builder_from_module(
            'Disc3', mod_path)
        scatter_list.append(disc3_builder)

        multi_scenarios = self.exec_eng.factory.create_simple_multi_scenario_builder(
            'multi_scenarios', 'scenario_list', scatter_list, autogather=True, gather_node='Post-processing')

        self.exec_eng.factory.set_builders_to_coupling_builder(
            multi_scenarios)
        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        x1 = 2
        x2 = 4
        scenario_df = pd.DataFrame(
            [['scenario_1', x1], ['scenario_2', x2]], columns=['scenario_name', 'x'])

        dict_values = {f'{self.study_name}.multi_scenarios.trade_variables': {'x': 'float'},
                       f'{self.study_name}.multi_scenarios.name_list': ['name_1', 'name_2'],
                       f'{self.study_name}.multi_scenarios.scenario_df': scenario_df}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios'):
            if isinstance(disc, SoSSimpleMultiScenario):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'scenario_1', 'scenario_2'])

        scenario_df = pd.DataFrame(
            [['scenario_1', x1]], columns=['scenario_name', 'x'])
        dict_values = {self.study_name +
                       '.multi_scenarios.scenario_df': scenario_df}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios'):
            if isinstance(disc, SoSSimpleMultiScenario):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'scenario_1'])

        dict_values = {
            self.study_name + '.multi_scenarios.name_list': ['name_1', 'name_2', 'name_3']}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios'):
            if isinstance(disc, SoSSimpleMultiScenario):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'scenario_1'])
        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios.scenario_1.Disc1'):
            if isinstance(disc, SoSDisciplineScatter):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'name_1', 'name_2', 'name_3'])

        scenario_df = pd.DataFrame(
            [['scenario_1', x1], ['scenario_2', x2]], columns=['scenario_name', 'x'])

        dict_values = {self.study_name + '.multi_scenarios.scenario_df': scenario_df,
                       self.study_name + '.multi_scenarios.name_list': ['name_1', 'name_2']}

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios'):
            if isinstance(disc, SoSSimpleMultiScenario):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'scenario_1', 'scenario_2'])
        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios.scenario_1.Disc1'):
            if isinstance(disc, SoSDisciplineScatter):
                self.assertListEqual(list(disc.get_scattered_disciplines().keys()), [
                                     'name_1', 'name_2'])

        private_val = {}
        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2

            private_val[self.study_name + '.name_1.a'] = a1
            private_val[self.study_name + '.name_2.a'] = a2
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            private_val[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_1.z'] = 1.2
        private_val[self.study_name +
                    '.multi_scenarios.scenario_2.z'] = 1.5

        self.exec_eng.load_study_from_input_dict(private_val)

        self.exec_eng.execute()

        for disc in self.exec_eng.dm.get_disciplines_with_name('MyCase.multi_scenarios'):
            if isinstance(disc, SoSSimpleMultiScenario):
                self.assertListEqual(
                    [key for key in list(disc.get_data_io_dict('in').keys()) if key not in disc.NUM_DESC_IN], ['trade_variables',  'scenario_list', 'scenario_df', 'scenario_dict'])
                self.assertDictEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.x_dict'), {'scenario_1': 2, 'scenario_2': 4})
                self.assertListEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.scenario_list'), ['scenario_1', 'scenario_2'])
                self.assertDictEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.scenario_dict'), {'scenario_1': {'x': 2}, 'scenario_2': {'x': 4}})
                self.assertListEqual(
                    list(self.exec_eng.dm.get_disciplines_with_name(
                        f'{self.study_name}')[0].get_sosdisc_outputs().keys()),
                    ['residuals_history'])

            elif isinstance(disc, SoSScatterData):
                self.assertListEqual(
                    [key for key in list(disc.get_data_io_dict('in').keys())if key not in disc.NUM_DESC_IN], ['x_dict', 'scenario_list'])
                self.assertListEqual(
                    list(disc.get_data_io_dict('out').keys()), ['scenario_1.x', 'scenario_2.x'])
                self.assertDictEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.x_dict'), {'scenario_1': 2, 'scenario_2': 4})
                self.assertEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.scenario_1.x'), 2)
                self.assertEqual(self.exec_eng.dm.get_value(
                    f'{self.study_name}.multi_scenarios.scenario_2.x'), 4)

    def _test_04_dump_and_load_after_execute(self):

        builders = self.exec_eng.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        self.exec_eng.factory.set_builders_to_coupling_builder(
            builders)
        self.exec_eng.configure()

        x1 = 2
        x2 = 4
        scenario_df = pd.DataFrame(
            [['scenario_1', x1], ['scenario_2', x2]], columns=['scenario_name', 'x'])

        dict_values = {}
        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'x': 'float'}
        dict_values[f'{self.study_name}.multi_scenarios.name_list'] = [
            'name_1', 'name_2']
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df

        scenario_list = ['scenario_1', 'scenario_2']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2

            dict_values[self.study_name + '.name_1.a'] = a1
            dict_values[self.study_name + '.name_2.a'] = a2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_1.z'] = 1.2
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_2.z'] = 1.5

        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_2.y'), a2 * x1 + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.y'), a1 * x2 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_2.y'), a2 * x2 + b2)

        dump_dir = join(self.root_dir, self.namespace)

        BaseStudyManager.static_dump_data(
            dump_dir, self.exec_eng, DirectLoadDump())

        exec_eng2 = ExecutionEngine(self.namespace)
        builders = exec_eng2.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        exec_eng2.factory.set_builders_to_coupling_builder(builders)

        exec_eng2.configure()

        BaseStudyManager.static_load_data(
            dump_dir, exec_eng2, DirectLoadDump())

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_2.y'), a2 * x1 + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.y'), a1 * x2 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_2.y'), a2 * x2 + b2)
        # Clean the dump folder at the end of the test
        self.dirs_to_del.append(
            join(self.root_dir, self.namespace))

    def _test_05_several_trade_variables(self):

        builders = self.exec_eng.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        self.exec_eng.factory.set_builders_to_coupling_builder(
            builders)
        self.exec_eng.configure()

        x1 = 2
        x2 = 4
        x3 = 0
        x4 = 3
        scenario_df = pd.DataFrame(
            [['scenario_1', x1, x3], ['scenario_2', x1, x4], ['scenario_3', x2, x3]], columns=['scenario_name', 'name_1.x', 'name_2.x'])

        dict_values = {}
        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_1.x': 'float', 'name_2.x': 'float'}
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        dict_values[f'{self.study_name}.multi_scenarios.name_list'] = [
            'name_1', 'name_2']

        scenario_list = ['scenario_1', 'scenario_2',
                         'scenario_3']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2

            dict_values[self.study_name + '.name_1.a'] = a1
            dict_values[self.study_name + '.name_2.a'] = a2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
            dict_values[self.study_name +
                        '.multi_scenarios.' + scenario + '.z'] = 1.2

        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.display_treeview_nodes()

        self.exec_eng.execute()

        scenario_dict = {'scenario_1': {'name_1.x': x1, 'name_2.x': x3},
                         'scenario_2': {'name_1.x': x1, 'name_2.x': x4},
                         'scenario_3': {'name_1.x': x2, 'name_2.x': x3}}

        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_dict'), scenario_dict)

        self.assertDictEqual(self.exec_eng.dm.get_value('MyCase.multi_scenarios.name_1.x_dict'), {
                             'scenario_1': x1, 'scenario_2': x1, 'scenario_3': x2})
        self.assertDictEqual(self.exec_eng.dm.get_value('MyCase.multi_scenarios.name_2.x_dict'), {
                             'scenario_1': x3, 'scenario_2': x4, 'scenario_3': x3})

        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_1.name_2.y'), a2 * x3 + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_1.y'), a1 * x1 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_2.name_2.y'), a2 * x4 + b2)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_3.name_1.y'), a1 * x2 + b1)
        self.assertEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_3.name_2.y'), a2 * x3 + b2)

    def _test_06_trade_on_name_list(self):

        builders = self.exec_eng.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        self.exec_eng.factory.set_builders_to_coupling_builder(
            builders)
        self.exec_eng.configure()

        scenario_df = pd.DataFrame(
            [['scenario_A', ['name_1']], ['scenario_B', ['name_1', 'name_2']], ['scenario_C', ['name_1', 'name_2', 'name_3']]], columns=['scenario_name', 'name_list'])

        dict_values = {}

        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_list': 'string_list'}
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df

        scenario_list = ['scenario_A', 'scenario_B',
                         'scenario_C']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2
            a3 = 10
            b3 = 0
            x = 2

            dict_values[self.study_name + '.name_1.a'] = a1
            dict_values[self.study_name + '.name_2.a'] = a2
            dict_values[self.study_name + '.name_3.a'] = a3

            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
            dict_values[self.study_name +
                        '.multi_scenarios.' + scenario + '.z'] = 1.2

        dict_values[self.study_name +
                    '.multi_scenarios.scenario_A.Disc1.name_1.b'] = b1

        dict_values[self.study_name +
                    '.multi_scenarios.scenario_B.Disc1.name_1.b'] = b1
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_B.Disc1.name_2.b'] = b2

        dict_values[self.study_name +
                    '.multi_scenarios.scenario_C.Disc1.name_1.b'] = b1
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_C.Disc1.name_2.b'] = b2
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_C.Disc1.name_3.b'] = b3

        dict_values[self.study_name + '.name_1.x'] = x
        dict_values[self.study_name + '.name_2.x'] = x
        dict_values[self.study_name + '.name_3.x'] = x

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_dict = {'scenario_A': {'name_list': ['name_1']},
                         'scenario_B': {'name_list': ['name_1', 'name_2']},
                         'scenario_C': {'name_list': ['name_1', 'name_2', 'name_3']}}

        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_dict'), scenario_dict)

        dict_values[f'{self.study_name}.multi_scenarios.name_list_list'] = [
            ['name_1', 'name_2'], ['name_1', 'name_2', 'name_3']]
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_A.Disc1.name_2.b'] = b2
        dict_values[self.study_name +
                    '.multi_scenarios.scenario_B.Disc1.name_3.b'] = b3

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        self.exec_eng.execute()

        y1 = a1 * x + b1
        y2 = a2 * x + b2
        y3 = a3 * x + b3

        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_A.y_dict'), {'name_1': y1})
        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_B.y_dict'), {'name_1': y1, 'name_2': y2})
        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_C.y_dict'), {'name_1': y1, 'name_2': y2, 'name_3': y3})
        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.y_dict'), {'scenario_A.name_1': y1,
                                               'scenario_B.name_1': y1,
                                               'scenario_B.name_2': y2,
                                               'scenario_C.name_1': y1,
                                               'scenario_C.name_2': y2,
                                               'scenario_C.name_3': y3})

    def _test_07_simple_multi_scenarios_without_trade_variables(self):

        builders = self.exec_eng.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        self.exec_eng.factory.set_builders_to_coupling_builder(
            builders)
        self.exec_eng.configure()

        scenario_df = pd.DataFrame(
            [['scenario_A'], ['scenario_B'], ['scenario_C']], columns=['scenario_name'])

        dict_values = {}
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        dict_values[f'{self.study_name}.multi_scenarios.name_list'] = [
            'name_1', 'name_2']

        scenario_list = ['scenario_A', 'scenario_B',
                         'scenario_C']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2
            x = 2

            dict_values[self.study_name + '.name_1.a'] = a1
            dict_values[self.study_name + '.name_2.a'] = a2

            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
            dict_values[self.study_name +
                        '.multi_scenarios.' + scenario + '.z'] = 1.2

            dict_values[self.study_name +
                        f'.multi_scenarios.{scenario}.Disc1.name_1.b'] = b1
            dict_values[self.study_name +
                        f'.multi_scenarios.{scenario}.Disc1.name_2.b'] = b2

        dict_values[self.study_name + '.name_1.x'] = x
        dict_values[self.study_name + '.name_2.x'] = x

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.trade_variables'), {})
        self.assertListEqual(
            self.exec_eng.dm.get_all_namespaces_from_var_name('scenario_dict'), [])

    def _test_08_changing_trade_variables(self):

        builders = self.exec_eng.factory.get_builder_from_process(
            repo=self.repo, mod_id='test_disc1_disc3_simple_multi_scenario')
        self.exec_eng.factory.set_builders_to_coupling_builder(
            builders)
        self.exec_eng.configure()

        dict_values = {}
        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_1.x': 'float', 'name_2.x': 'float'}
        self.exec_eng.load_study_from_input_dict(dict_values)

        scenario_df = self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_df')
        self.assertTrue(scenario_df.equals(pd.DataFrame(
            columns=['scenario_name', 'name_1.x', 'name_2.x'])))

        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_list': 'string_list'}
        self.exec_eng.load_study_from_input_dict(dict_values)

        scenario_df = self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_df')
        self.assertTrue(scenario_df.equals(pd.DataFrame(
            columns=['scenario_name', 'name_list'])))

        scenario_df = pd.DataFrame([['scenario_1', ['name_1', 'name_2']], [
                                   'scenario_2', ['name_3']]], columns=['scenario_name', 'name_list'])
        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_dict = self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_dict')
        self.assertDictEqual(scenario_dict, {'scenario_1': {'name_list': [
                             'name_1', 'name_2']}, 'scenario_2': {'name_list': ['name_3']}})

        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_list': 'string_list', 'name_1.x': 'float', 'name_2.x': 'float'}
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_dict = self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_dict')

        self.assertDictEqual(scenario_dict, {'scenario_1': {'name_list': ['name_1', 'name_2'], 'name_1.x': np.nan, 'name_2.x': np.nan}, 'scenario_2': {
                             'name_list': ['name_3'], 'name_1.x': np.nan, 'name_2.x': np.nan}})

        dict_values[f'{self.study_name}.multi_scenarios.name_list'] = [
            'name_1', 'name_2']
        dict_values[f'{self.study_name}.multi_scenarios.trade_variables'] = {
            'name_1.x': 'float', 'name_2.x': 'float'}
        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_dict = self.exec_eng.dm.get_value(
            f'{self.study_name}.multi_scenarios.scenario_dict')

        self.assertDictEqual(scenario_dict, {'scenario_1': {'name_1.x': np.nan, 'name_2.x': np.nan}, 'scenario_2': {
                             'name_1.x': np.nan, 'name_2.x': np.nan}})

        x1 = 2
        x2 = 4
        x3 = 0
        x4 = 3
        scenario_df = pd.DataFrame(
            [['scenario_1', x1, x3], ['scenario_2', x1, x4], ['scenario_3', x2, x3]], columns=['scenario_name', 'name_1.x', 'name_2.x'])

        dict_values[f'{self.study_name}.multi_scenarios.scenario_df'] = scenario_df
        dict_values[f'{self.study_name}.multi_scenarios.name_list'] = [
            'name_1', 'name_2']

        scenario_list = ['scenario_1', 'scenario_2',
                         'scenario_3']
        for scenario in scenario_list:
            a1 = 3
            b1 = 4
            a2 = 6
            b2 = 2

            dict_values[self.study_name + '.name_1.a'] = a1
            dict_values[self.study_name + '.name_2.a'] = a2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_1.b'] = b1
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc1.name_2.b'] = b2
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.constant'] = 3
            dict_values[self.study_name + '.multi_scenarios.' +
                        scenario + '.Disc3.power'] = 2
            dict_values[self.study_name +
                        '.multi_scenarios.' + scenario + '.z'] = 1.2

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.display_treeview_nodes()

        scenario_dict = {'scenario_1': {'name_1.x': x1, 'name_2.x': x3},
                         'scenario_2': {'name_1.x': x1, 'name_2.x': x4},
                         'scenario_3': {'name_1.x': x2, 'name_2.x': x3}}

        self.assertDictEqual(self.exec_eng.dm.get_value(
            'MyCase.multi_scenarios.scenario_dict'), scenario_dict)


if '__main__' == __name__:
    cls = TestSimpleMultiScenario()
    cls.setUp()
    cls.test_06_trade_on_name_list()
