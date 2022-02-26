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

import pandas as pd
import numpy as np
from numpy import array
from pandas.testing import assert_frame_equal

from sos_trades_core.execution_engine.execution_engine import ExecutionEngine


class TestExtendDataframe(unittest.TestCase):
    """
    Extend dataframe type for GEMSEO test class
    """

    def setUp(self):

        self.name = 'EE'

    def test_01_extend_df_sosdiscipline(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_protected', self.name)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc6.Disc6'
        disc6_builder = exec_eng.factory.get_builder_from_module(
            'Disc6', mod_list)

        exec_eng.factory.set_builders_to_coupling_builder(
            disc6_builder)
        exec_eng.configure()

        # additional test to verify that values_in are used
        values_dict = {}
        values_dict['EE.df'] = pd.DataFrame(
            array([[0.5, 0.5]]), columns=['c1', 'c2'])
        values_dict['EE.dict_df'] = {'key_1': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2']),
                                     'key_2': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2'])}
        exec_eng.dm.set_values_from_dict(values_dict)

        exec_eng.display_treeview_nodes()
        exec_eng.execute()

        target = {'EE.df': [0.0, 0.5, 0.5], 'EE.dict_df':
                  [0., 5., 3., 0., 5., 3.], 'EE.h': [0.75, 0.75]}

        res = {}
        for key in target:
            res[key] = exec_eng.dm.get_value(key)
            if target[key] is dict:
                self.assertDictEqual(res[key], target[key])
            elif target[key] is array:
                self.assertListEqual(
                    list(target[key]), list(res[key]))
        #-- check dataframe as SoSTrades output
        disc6 = exec_eng.root_process.sos_disciplines[0]
        df = disc6.get_sosdisc_inputs('df')
        df_target = pd.DataFrame(
            array([[0.5, 0.5]]), columns=['c1', 'c2'], index=[0.])
        self.assertTrue(df.equals(df_target),
                        f'expected and output DF are different:\n{df_target}\n VS\n{df}')

    def test_02_extend_df_soscoupling(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_protected', self.name)
        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc7.Disc7'
        disc7_builder = exec_eng.factory.get_builder_from_module(
            'Disc7', mod_list)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc6.Disc6'
        disc6_builder = exec_eng.factory.get_builder_from_module(
            'Disc6', mod_list)

        exec_eng.factory.set_builders_to_coupling_builder(
            [disc6_builder, disc7_builder])
        exec_eng.configure()
        # additional test to verify that values_in are used
        values_dict = {}
        values_dict['EE.h'] = [8., 9.]
        values_dict['EE.df'] = pd.DataFrame(
            array([[5., 3.]]), columns=['c1', 'c2'])
        values_dict['EE.dict_df'] = {'key_1': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2']),
                                     'key_2': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2'])}
        exec_eng.dm.set_values_from_dict(values_dict)

        exec_eng.execute()

        target = {'EE.h': array([0.70710678,
                                 0.70710678]),
                  'EE.dict_df': array([0., 0.70710678, 0.70710678, 0., 0.70710678, 0.70710678]),
                  'EE.df': array([0., 0.707107, 0.707107])}
        #-- check output keys
        res = {}
        for key in target:
            res[key] = exec_eng.dm.get_value(key)
            if target[key] is dict:
                self.assertDictEqual(res[key], target[key])
            elif target[key] is array:
                self.assertListEqual(
                    list(target[key]), list(res[key]))

        disc7 = exec_eng.root_process.sos_disciplines[1]

        tolerance = exec_eng.dm.get_value('EE.tolerance')

        df = disc7.get_sosdisc_outputs('df')
        df_target = pd.DataFrame(
            array([[np.sqrt(2.0) / 2.0, np.sqrt(2.0) / 2.0]]), columns=['c1', 'c2'])
        assert_frame_equal(df, df_target, check_exact=False, rtol=1e-5)

        max_mda_iter = exec_eng.dm.get_value('EE.max_mda_iter')
        residual_history = exec_eng.root_process.sub_mda_list[0].residual_history

        # Check residual history
        self.assertLessEqual(len(residual_history), max_mda_iter)
        self.assertLessEqual(residual_history[-1][0], tolerance)

    def test_03_check_df_excluded_columns(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_protected', self.name)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc6.Disc6'
        disc6_builder = exec_eng.factory.get_builder_from_module(
            'Disc6', mod_list)

        exec_eng.factory.set_builders_to_coupling_builder(
            disc6_builder)
        exec_eng.configure()

        # additional test to verify that values_in are used
        values_dict = {}
        values_dict['EE.df'] = pd.DataFrame(
            array([[2020, 2020, 0.5, 0.5]]), columns=['years', 'year', 'c1', 'c2'])
        values_dict['EE.dict_df'] = {'key_1': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2']),
                                     'key_2': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2'])}
        exec_eng.dm.set_values_from_dict(values_dict)

        exec_eng.display_treeview_nodes()
        exec_eng.execute()

        #-- check dataframe as SoSTrades output
        disc6 = exec_eng.root_process.sos_disciplines[0]

        # Check that in GEMS we do not have year and years columns
        self.assertListEqual(
            list(disc6.get_inputs_by_name('EE.df')), [0.5, 0.5])

        # Check that in SoSTrades we have the columns back and at the right
        # order
        df = disc6.get_sosdisc_inputs('df')
        df_target = pd.DataFrame(
            array([[2020, 2020, 0.5, 0.5]]), columns=['years', 'year', 'c1', 'c2'], index=[0.])
        self.assertTrue(df.equals(df_target),
                        f'expected and output DF are different:\n{df_target}\n VS\n{df}')

    def test_04_check_empty_df(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_test', self.name)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc4_dict_empty_df.Disc4EmptyDf'
        disc4_builder = exec_eng.factory.get_builder_from_module(
            'Disc4', mod_list)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc5_disc_empty_df.Disc5EmptyDf'
        disc5_builder = exec_eng.factory.get_builder_from_module(
            'Disc5', mod_list)
        exec_eng.factory.set_builders_to_coupling_builder(
            [disc4_builder, disc5_builder])

        exec_eng.configure()

        # additional test to verify that values_in are used
        values_dict = {}
        values_dict['EE.h'] = {'dataframe': pd.DataFrame(data={'col1': array([0.5,
                                                                              0.5])})}
        exec_eng.dm.set_values_from_dict(values_dict)

        exec_eng.display_treeview_nodes()
        exec_eng.execute()

        self.assertTrue(exec_eng.dm.get_value('EE.Disc5.is_df_empty'))
        self.assertTrue(exec_eng.dm.get_value(
            'EE.Disc5.is_dict_empty_df_empty'))
        self.assertTrue(exec_eng.dm.get_value(
            'EE.Disc5.is_dict_empty_list_empty'))

    def test_05_multi_index_column_df(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_protected', self.name)
        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc7.Disc7'
        disc7_builder = exec_eng.factory.get_builder_from_module(
            'Disc7', mod_list)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc6.Disc6'
        disc6_builder = exec_eng.factory.get_builder_from_module(
            'Disc6', mod_list)

        exec_eng.factory.set_builders_to_coupling_builder(
            [disc6_builder, disc7_builder])
        exec_eng.configure()

        values_dict = {}

        col = pd.MultiIndex.from_tuples(
            [('c1', ''), ('c2', '')])
        df_multi_index_columns = pd.DataFrame(
            data=[[0.5, 1.0], [0.5, 1.0]], columns=col)

        values_dict['EE.df'] = df_multi_index_columns
        values_dict['EE.dict_df'] = {'key_1': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2']),
                                     'key_2': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2'])}
        exec_eng.load_study_from_input_dict(values_dict)

        disc6 = exec_eng.dm.get_disciplines_with_name('EE.Disc6')[0]

        self.assertTrue(disc6.get_sosdisc_inputs(
            'df').columns.equals(df_multi_index_columns.columns))
        self.assertTrue(disc6.get_sosdisc_inputs(
            'df').equals(df_multi_index_columns))

        df_in_dm = disc6.get_sosdisc_inputs('df')
        df_converted_array = disc6._convert_new_type_into_array(
            {'EE.df': df_in_dm})
        df_reconverted = disc6._convert_array_into_new_type(df_converted_array)
        self.assertTrue(df_reconverted['EE.df'].equals(df_multi_index_columns))
        self.assertTrue(df_reconverted['EE.df'].columns.equals(col))
        self.assertTrue(exec_eng.dm.get_data(
            'EE.df', 'type_metadata')[0]['columns'].equals(col))

        exec_eng.execute()

        target = [0.7071067811865475, 0.7071067811865475]
        self.assertListEqual(list(exec_eng.dm.get_value('EE.h')), target)

    def test_06_multi_index_rows_df(self):

        exec_eng = ExecutionEngine(self.name)

        exec_eng.ns_manager.add_ns('ns_protected', self.name)
        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc7.Disc7'
        disc7_builder = exec_eng.factory.get_builder_from_module(
            'Disc7', mod_list)

        mod_list = 'sos_trades_core.sos_wrapping.test_discs.disc6.Disc6'
        disc6_builder = exec_eng.factory.get_builder_from_module(
            'Disc6', mod_list)

        exec_eng.factory.set_builders_to_coupling_builder(
            [disc6_builder, disc7_builder])
        exec_eng.configure()

        values_dict = {}

        mux = pd.MultiIndex.from_arrays(
            [list('aaabbbccc'), [0, 1, 2, 0, 1, 2, 0, 1, 2]], names=['one', 'two'])
        df = pd.DataFrame({'c1': [0.5] * 9, 'c2': [0.5] * 9}, mux)

        values_dict['EE.df'] = df
        values_dict['EE.dict_df'] = {'key_1': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2']),
                                     'key_2': pd.DataFrame(array([[5., 3.]]), columns=['c1', 'c2'])}
        exec_eng.load_study_from_input_dict(values_dict)

        disc6 = exec_eng.dm.get_disciplines_with_name('EE.Disc6')[0]

        self.assertTrue(disc6.get_sosdisc_inputs(
            'df').index.equals(mux))
        self.assertTrue(disc6.get_sosdisc_inputs(
            'df').equals(df))

        df_in_dm = disc6.get_sosdisc_inputs('df')
        df_converted_array = disc6._convert_new_type_into_array(
            {'EE.df': df_in_dm})
        df_reconverted = disc6._convert_array_into_new_type(df_converted_array)
        self.assertTrue(df_reconverted['EE.df'].equals(df))
        self.assertTrue(df_reconverted['EE.df'].index.equals(mux))
        self.assertTrue(exec_eng.dm.get_data(
            'EE.df', 'type_metadata')[0]['indices'].equals(mux))

        exec_eng.execute()

        target = [0.7071067811865475, 0.7071067811865475]
        self.assertListEqual(list(exec_eng.dm.get_value('EE.h')), target)


if '__main__' == __name__:
    cls = TestExtendDataframe()
    cls.setUp()
    cls.test_06_multi_index_rows_df()
