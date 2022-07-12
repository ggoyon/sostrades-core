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

from sos_trades_core.execution_engine.execution_engine import ExecutionEngine


class TestScatterDiscipline(unittest.TestCase):
    """
    Class to test Scatter discipline configure and cleaning
    """

    def setUp(self):
        '''
        Initialize third data needed for testing
        '''
        self.name = 'Scatter'
        self.ee = ExecutionEngine(self.name)

        # set scatter build map
        mydict = {'input_name': 'AC_list',

                  'input_ns': 'ns_barrierr',
                  'output_name': 'ac_name',
                  'scatter_ns': 'ns_ac',
                  'gather_ns': 'ns_barrierr'
                  }
        self.ee.smaps_manager.add_build_map('AC_list', mydict)

        # set namespace definition
        self.ee.ns_manager.add_ns('ns_barrierr', self.name)

        # get coupling process builder
        sub_proc = 'test_disc1_disc2_coupling'
        cls_list = self.ee.factory.get_builder_from_process(repo='sos_trades_core.sos_processes.test',
                                                            mod_id=sub_proc)

        # create scatter builder with map and coupling process
        scatter_list = self.ee.factory.create_multi_scatter_builder_from_list(
            'AC_list', cls_list, autogather=True)

        # set scatter builder to root process
        self.ee.factory.set_builders_to_coupling_builder(scatter_list)
        self.ee.configure()

        #-- aircraft lists
        self.list_aircraft_1 = ['CH19_Kero', 'CH19_H2']

        #-- the idea here is to check that removing (from CH19_Kero and CH19_H2 list) one
        # of the aircraft give the same result reagrdless of the aircraft position in the list
        # (because variables are own by one of the associated discipline)
        self.list_aircraft_remove_1_1 = ['CH19_H2']
        self.list_aircraft_remove_1_2 = ['CH19_Kero']

        self.list_aircraft_add_1 = ['CH19_Kero', 'A320', 'CH19_H2']

    def build_aircraft_disciplines_list(self, list_aircraft=[]):

        disciplines_list = []

        disciplines_list.append(self.name)
        disciplines_list.append(
            f'{self.name}.Disc1')
        disciplines_list.append(
            f'{self.name}.Disc2')

        for aircraft in list_aircraft:
            disciplines_list.append(
                f'{self.name}.Disc1.{aircraft}')
            disciplines_list.append(
                f'{self.name}.Disc2.{aircraft}')

        disciplines_list.sort()

        return disciplines_list

    def test_01_raw_initialisation(self):

        private_values_multiproduct = {
            f'{self.name}.AC_list': ['A1']}

        self.ee.dm.set_values_from_dict(private_values_multiproduct)

        self.ee.configure()

        print('Treeview for test test_01_raw_initialisation')
        self.ee.display_treeview_nodes()
        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        raw_disciplines = self.build_aircraft_disciplines_list(['A1'])

        disciplines_list.sort()
        self.assertListEqual(raw_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

    def test_02_multiinstance_modification_remove_one_aircraft_1(self):

        private_values_multiproduct = {
            f'{self.name}.AC_list': self.list_aircraft_1}

        self.ee.dm.set_values_from_dict(private_values_multiproduct)

        self.ee.configure()

        print('Treeview for test test_03_multiinstance_modification_remove_one_aircraft_1')
        self.ee.display_treeview_nodes()

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_1_disciplines = self.build_aircraft_disciplines_list(
            self.list_aircraft_1)

        self.assertListEqual(list_aircraft_1_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

        #-- remove on aircraft
        private_values_multiproduct = {
            f'{self.name}.AC_list': self.list_aircraft_remove_1_1}

        self.ee.dm.set_values_from_dict(private_values_multiproduct)

        self.ee.configure()

        print('Treeview for test test_03_multiinstance_modification_remove_one_aircraft_1')
        self.ee.display_treeview_nodes()

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_remove_1_disciplines = self.build_aircraft_disciplines_list(
            self.list_aircraft_remove_1_1)

        self.assertListEqual(list_aircraft_remove_1_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

    def test_03_multiinstance_modification_remove_one_aircraft_2(self):

        private_values_multiproduct = {
            f'{self.name}.AC_list': self.list_aircraft_1}

        self.ee.dm.set_values_from_dict(private_values_multiproduct)

        self.ee.configure()

        print('Treeview for test test_04_multiinstance_modification_remove_one_aircraft_2')
        self.ee.display_treeview_nodes(display_variables='visi')

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_1_disciplines = self.build_aircraft_disciplines_list(
            self.list_aircraft_1)

        self.assertListEqual(list_aircraft_1_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

        #-- remove on aircraft
        private_values_multiproduct = {
            f'{self.name}.AC_list': self.list_aircraft_remove_1_2}

        self.ee.dm.set_values_from_dict(private_values_multiproduct)

        self.ee.configure()

        print('Treeview for test test_04_multiinstance_modification_remove_one_aircraft_2')
        self.ee.display_treeview_nodes(display_variables='visi')

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_remove_1_disciplines = self.build_aircraft_disciplines_list(
            self.list_aircraft_remove_1_2)

        self.assertListEqual(list_aircraft_remove_1_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

    def test_04_multiinstance_modification_remove_all_aircraft(self):

        raw_dm_values = list(self.ee.dm.data_id_map.keys())
        raw_dm_values.sort()

        private_values_multiproduct = {
            f'{self.name}.AC_list': self.list_aircraft_1}

        self.ee.load_study_from_input_dict(private_values_multiproduct)

        self.ee.display_treeview_nodes()

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_1_disciplines = self.build_aircraft_disciplines_list(
            self.list_aircraft_1)

        self.assertListEqual(list_aircraft_1_disciplines, disciplines_list,
                             'Discipline between reference and generated are different')

        #-- remove all aircraft
        private_values_multiproduct = {
            f'{self.name}.AC_list': []}

        self.ee.load_study_from_input_dict(private_values_multiproduct)

        exp_tv_list = [f'Nodes representation for Treeview {self.name}',
                       f'|_ {self.name}',
                       f'\t|_ Disc2',
                       f'\t|_ Disc1']
        exp_tv_str = '\n'.join(exp_tv_list)
        assert exp_tv_str == self.ee.display_treeview_nodes()

        disciplines_list = list(self.ee.dm.disciplines_id_map.keys())
        disciplines_list.sort()

        list_aircraft_remove_all = self.build_aircraft_disciplines_list()

        self.assertListEqual(list_aircraft_remove_all, disciplines_list,
                             'Discipline between reference and generated are different')

        #-- cheack that data manager is also cleared

        last_dm_values = list(self.ee.dm.data_id_map.keys())
        last_dm_values.sort()

        # Except dict out of gather
        last_dm_values.remove('Scatter.y_dict')
        last_dm_values.remove('Scatter.z_dict')
        self.assertListEqual(raw_dm_values, last_dm_values,
                             'After removing discipline, data manager variables list is different than raw list')
