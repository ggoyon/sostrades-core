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
from numpy import array

'''
mode: python; py-indent-offset: 4; tab-width: 4; coding: utf-8
'''
import unittest
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

from sos_trades_core.execution_engine.execution_engine import ExecutionEngine
from sos_trades_core.sos_wrapping.analysis_discs.grid_search_eval import GridSearchEval
from sos_trades_core.execution_engine.sos_coupling import SoSCoupling


class TestGridSearchEval(unittest.TestCase):
    """
    SoSGridSearchEval test class
    """

    def setUp(self):
        '''
        Initialize third data needed for testing
        '''
        pd.set_option('display.max_columns', 10)
        self.namespace = 'MyCase'
        self.study_name = f'{self.namespace}'
        self.repo = 'sos_trades_core.sos_processes.test'
        self.base_path = 'sos_trades_core.sos_wrapping.test_discs'
        self.exec_eng = ExecutionEngine(self.namespace)
        self.factory = self.exec_eng.factory
        self.grid_search = 'GridSearch'
        self.proc_name = 'test_grid_search'

    def test_01_grid_search_eval(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        print('Study first configure!')

        self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        # self.exec_eng.dm.get_data('MyCase.GridSearch.eval_inputs')[
        #     'possible_values']

        # dict_values = {}
        # self.exec_eng.load_study_from_input_dict(dict_values)

        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.j', ['selected_input']] = True

        eval_outputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_outputs')
        eval_outputs.loc[eval_outputs['full_name'] ==
                         f'{self.grid_search}.Disc1.y', ['selected_output']] = True

        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,

            # DISC1 INPUTS
            f'{self.study_name}.{self.grid_search}.Disc1.name': 'A1',
            f'{self.study_name}.{self.grid_search}.Disc1.a': 20,
            f'{self.study_name}.{self.grid_search}.Disc1.b': 2,
            f'{self.study_name}.{self.grid_search}.Disc1.x': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.d': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.f': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.g': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.h': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.j': 3.,
        }

        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        doe_disc_samples = grid_search_disc_output['samples_inputs_df']
        y_dict = grid_search_disc_output['GridSearch.Disc1.y_dict']
        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {doe_disc_samples}')
        print(f'Study generated the output: y_dict \n {y_dict}')

        dspace = pd.DataFrame({
            'shortest_name': ['x', 'j'],
            'lower_bnd': [5., 20.],
            'upper_bnd': [7., 25.],
            'nb_points': [3, 3],
            'full_name': ['GridSearch.Disc1.x', 'GridSearch.Disc1.j'],
        })

        dict_values = {
            f'{self.study_name}.{self.grid_search}.design_space': dspace,
        }

        self.exec_eng.load_study_from_input_dict(dict_values)
        self.exec_eng.configure()
        self.exec_eng.execute()

        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()

        doe_disc_samples = grid_search_disc_output['samples_inputs_df']
        y_dict = grid_search_disc_output['GridSearch.Disc1.y_dict']

        # CHECK THE GRIDSEARCH OUTPUTS
        doe_disc_samples_ref = pd.DataFrame({'scenario': [
            'scenario_1', 'scenario_2', 'scenario_3'], 'GridSearch.Disc1.x': [5.0, 6.0, 7.0]})
        y_dict_ref = {'scenario_1': 102.0,
                      'scenario_2': 122.0, 'scenario_3': 142.0}
        # assert_frame_equal(doe_disc_samples, doe_disc_samples_ref)
        # assert y_dict_ref == y_dict

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {doe_disc_samples}')
        print(f'Study generated the output: y_dict \n {y_dict}')

        # TEST FOR 6 INPUTS
        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.f', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.g', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.h', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.j', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.d', ['selected_input']] = True

        dict_values = {
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        doe_disc_samples = grid_search_disc_output['samples_inputs_df']
        y_dict = grid_search_disc_output['GridSearch.Disc1.y_dict']

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {doe_disc_samples}')
        print(f'Study generated the output: y_dict \n {y_dict}')

        # CHANGE THE SELECTED INPUTS TO 2
        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = False
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.f', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.g', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.h', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.j', ['selected_input']] = False
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.d', ['selected_input']] = False

        dict_values = {
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.dm.get_value(['MyCase.GridSearch.eval_inputs'][0])

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        self.exec_eng.execute()

        self.exec_eng.dm.get_value(['MyCase.GridSearch.eval_inputs'][0])

    def test_02_grid_search_shortest_name(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        print('Study first configure!')

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        list = ['GridSearch.Disc1.d', 'GridSearch.Disc1.f', 'GridSearch.Disc1.g',
                'GridSearch.Disc1.h', 'GridSearch.Disc1.j', 'GridSearch.Disc1.x',
                'GridSearch.Disc2.d', 'GridSearch.Nana.Disc1.d', 'GridSearch.Nana.Disc2.d']

        shortest_list = grid_search_disc.generate_shortest_name(list)

    def test_03_postproc_check(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        # print('Study first configure!')

        self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        # self.exec_eng.dm.get_data('MyCase.GridSearch.eval_inputs')[
        #     'possible_values']

        # dict_values = {}
        # self.exec_eng.load_study_from_input_dict(dict_values)

        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.f', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.g', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.h', ['selected_input']] = True

        eval_outputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_outputs')
        eval_outputs.loc[eval_outputs['full_name'] ==
                         f'{self.grid_search}.Disc1.y', ['selected_output']] = True

        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,

            # DISC1 INPUTS
            f'{self.study_name}.{self.grid_search}.Disc1.name': 'A1',
            f'{self.study_name}.{self.grid_search}.Disc1.a': 20,
            f'{self.study_name}.{self.grid_search}.Disc1.b': 2,
            f'{self.study_name}.{self.grid_search}.Disc1.x': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.d': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.f': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.g': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.h': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.j': 3.,
        }

        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        doe_disc_samples = grid_search_disc_output['samples_inputs_df']
        y_dict = grid_search_disc_output['GridSearch.Disc1.y_dict']
        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        # yy_dict={'scenario_1':12.0,
        #         'scenario_2':25.0,
        #         'scenario_3':56.0,
        #         'scenario_4':48.0,
        #         'scenario_5':19.0,
        #         'scenario_6':55.0,
        #         'scenario_7':27.0,
        #         'scenario_8':32.0,
        #         'reference':45.0,}

        # new_gsoutputs_dict={'doe_samples_dataframe':doe_disc_samples,
        #                     'GridSearch.Disc1.y_dict':yy_dict}

        # grid_search_disc.store_sos_outputs_values(
        #     new_gsoutputs_dict, update_dm=True
        # )
        # dspace = pd.DataFrame({
        #     'shortest_name': ['f','g', 'h'],
        #     'lower_bnd': [5., 20.,1.],
        #     'upper_bnd': [7., 25.,2.],
        #     'nb_points': [3, 3, 3],
        #     'full_name': ['GridSearch.Disc1.f', 'GridSearch.Disc1.g','GridSearch.Disc1.h'],
        # })

        # dict_values = {
        #     f'{self.study_name}.{self.grid_search}.design_space': dspace,
        # }

        # self.exec_eng.load_study_from_input_dict(dict_values)
        # self.exec_eng.configure()
        # self.exec_eng.execute()

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        filter = grid_search_disc.get_chart_filter_list()
        graph_list = grid_search_disc.get_post_processing_list(filter)
        # for graph in graph_list:
        #     #     pass
        #     graph.to_plotly().show()

    def test_04_grid_search_status(self):
        """ This tests aims at proving the ability of grid search
        discipline to have the proper status after it has run
        """
        self.ns = f'{self.study_name}'

        exec_eng = ExecutionEngine(self.study_name)
        factory = exec_eng.factory

        proc_name = "test_sellar_grid_search"
        grid_search_builder = factory.get_builder_from_process(repo=self.repo,
                                                               mod_id=proc_name)

        exec_eng.factory.set_builders_to_coupling_builder(
            grid_search_builder)

        exec_eng.configure()

        exp_tv_list = [f'Nodes representation for Treeview {self.ns}',
                       '|_ MyCase',
                       f'\t|_ GridSearch',
                       '\t\t|_ Sellar_2',
                       '\t\t|_ Sellar_1',
                       '\t\t|_ Sellar_Problem']
        exp_tv_str = '\n'.join(exp_tv_list)
        exec_eng.display_treeview_nodes(True)
        assert exp_tv_str == exec_eng.display_treeview_nodes()

        # -- set up disciplines

        values_dict = {}
        values_dict[f'{self.ns}.x'] = 1.
        values_dict[f'{self.ns}.y_1'] = 1.
        values_dict[f'{self.ns}.y_2'] = 1.
        values_dict[f'{self.ns}.z'] = array([1., 1.])
        values_dict[f'{self.ns}.GridSearch.Sellar_Problem.local_dv'] = 10
        exec_eng.load_study_from_input_dict(values_dict)

        dspace = pd.DataFrame({'variable': ['x'],
                               'lower_bnd': [20.],
                               'upper_bnd': [25.],
                               'nb_points': [3],
                               })
        dspace = pd.DataFrame(dspace)

        input_selection_x = {'selected_input': [False, True, False, False, False],
                             'full_name': ['GridSearch.Sellar_Problem.local_dv', 'x', 'y_1',
                                           'y_2',
                                           'z'],
                             'shortest_name': ['local_dv', 'x', 'y_1',
                                               'y_2',
                                               'z']}
        input_selection_x = pd.DataFrame(input_selection_x)

        output_selection_obj_y1_y2 = {'selected_output': [False, False, True, True, True],
                                      'full_name': ['c_1', 'c_2', 'obj',
                                                    'y_1', 'y_2'],
                                      'shortest_name': ['c_1', 'c_2', 'obj',
                                                        'y_1', 'y_2']}
        output_selection_obj_y1_y2 = pd.DataFrame(output_selection_obj_y1_y2)

        disc_dict = {}
        # GridSearch inputs

        disc_dict[f'{self.ns}.GridSearch.design_space'] = dspace
        disc_dict[f'{self.ns}.GridSearch.eval_inputs'] = input_selection_x
        disc_dict[f'{self.ns}.GridSearch.eval_outputs'] = output_selection_obj_y1_y2

        exec_eng.load_study_from_input_dict(disc_dict)
        exec_eng.execute()
        grid_search = exec_eng.dm.get_disciplines_with_name('MyCase.GridSearch')[
            0]
        sellar_2 = exec_eng.dm.get_disciplines_with_name(
            'MyCase.GridSearch.Sellar_2')[0]
        sellar_1 = exec_eng.dm.get_disciplines_with_name(
            'MyCase.GridSearch.Sellar_1')[0]
        sellar_problem = exec_eng.dm.get_disciplines_with_name(
            'MyCase.GridSearch.Sellar_Problem')[0]

        # check that all the grid search disciplines has run successfully
        self.assertEqual(grid_search.status, 'DONE')
        self.assertEqual(sellar_1.status, 'DONE')
        self.assertEqual(sellar_2.status, 'DONE')
        self.assertEqual(sellar_problem.status, 'DONE')

        # We trigger an exception during the execute and we check that
        # the failed discipline and grid search status are set to failed while
        # the other are PENDING
        values_dict[f'{self.ns}.debug_mode_sellar'] = True
        exec_eng.load_study_from_input_dict(values_dict)
        try:
            exec_eng.execute()
        except:
            self.assertEqual(grid_search.status, 'FAILED')
            self.assertEqual(sellar_1.status, 'PENDING')
            self.assertEqual(sellar_2.status, 'FAILED')
            self.assertEqual(sellar_problem.status, 'PENDING')

        print("done")

    def test_05_grid_search_multipliers_inputs_for_2_columns(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        print('Study first configure!')

        dict_values = {
            # DISC1 INPUTS
            f'{self.study_name}.{self.grid_search}.Disc1.name': 'A1',
            f'{self.study_name}.{self.grid_search}.Disc1.a': 20,
            f'{self.study_name}.{self.grid_search}.Disc1.b': 2,
            f'{self.study_name}.{self.grid_search}.Disc1.x': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.d': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.f': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.g': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.h': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.j': 3.,
            f'{self.study_name}.{self.grid_search}.Disc1.dd_df': pd.DataFrame({'string_val': ['str', 'str2', 'str3'], 'values1': [100., 200., 300.], 'values2': [50., 100., 150.]})
        }

        self.exec_eng.load_study_from_input_dict(dict_values)

        # self.exec_eng.dm.get_data('MyCase.GridSearch.eval_inputs')[
        #     'possible_values']

        # dict_values = {}
        # self.exec_eng.load_study_from_input_dict(dict_values)

        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.dd_df_values2_multiplier', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.dd_df_values1_multiplier', ['selected_input']] = True

        eval_outputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_outputs')
        eval_outputs.loc[eval_outputs['full_name'] ==
                         f'{self.grid_search}.Disc1.val_sum_dict', ['selected_output']] = True

        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        ds['nb_points'] = 3
        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.design_space': ds,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        dd_df = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.Disc1.dd_df')
        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        samples_inputs_df = grid_search_disc_output['samples_inputs_df']
        samples_outputs_df = grid_search_disc_output['samples_outputs_df']
        val_sum_dict_dict = grid_search_disc_output['GridSearch.Disc1.val_sum_dict_dict']
        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {samples_inputs_df}')
        print(f'Outputs used for graphs: \n {samples_outputs_df}')
        print(f'Study generated the output: y_dict \n {val_sum_dict_dict}')

        print(f'Check scenario {samples_inputs_df.loc[10][0]}')
        val2_multi = samples_inputs_df.loc[10][2]
        val1_multi = samples_inputs_df.loc[10][3]
        result_val1 = samples_outputs_df.loc[10][1]
        result_val2 = samples_outputs_df.loc[10][2]

        dd_df['values1'] = dd_df['values1'] * (val1_multi / 100)
        dd_df['values2'] = dd_df['values2'] * (val2_multi / 100)

        result__computed_val1 = dd_df['values1'].cumsum().values[-1]
        result__computed_val2 = dd_df['values2'].cumsum().values[-1]

        self.assertEqual(result_val1, result__computed_val1)
        self.assertEqual(result_val2, result__computed_val2)

    def test_06_grid_search_multipliers_inputs_for_all_columns(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        print('Study first configure!')

        dict_values = {
            # DISC1 INPUTS
            f'{self.study_name}.{self.grid_search}.Disc1.name': 'A1',
            f'{self.study_name}.{self.grid_search}.Disc1.b': 2,
            f'{self.study_name}.{self.grid_search}.Disc1.dd_df': pd.DataFrame({'string_val': ['str', 'str2', 'str3'], 'values1': [100., 200., 300.], 'values2': [50., 100., 150.]})
        }

        self.exec_eng.load_study_from_input_dict(dict_values)

        # self.exec_eng.dm.get_data('MyCase.GridSearch.eval_inputs')[
        #     'possible_values']

        # dict_values = {}
        # self.exec_eng.load_study_from_input_dict(dict_values)

        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.dd_df_multiplier', ['selected_input']] = True

        eval_outputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_outputs')
        eval_outputs.loc[eval_outputs['full_name'] ==
                         f'{self.grid_search}.Disc1.val_sum_dict', ['selected_output']] = True

        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        ds['nb_points'] = 3
        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.design_space': ds,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        dd_df = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.Disc1.dd_df')
        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        samples_inputs_df = grid_search_disc_output['samples_inputs_df']
        samples_outputs_df = grid_search_disc_output['samples_outputs_df']
        val_sum_dict_dict = grid_search_disc_output['GridSearch.Disc1.val_sum_dict_dict']
        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {samples_inputs_df}')
        print(f'Outputs used for graphs: \n {samples_outputs_df}')
        print(f'Study generated the output: y_dict \n {val_sum_dict_dict}')

        print(f'Check scenario {samples_inputs_df.loc[4][0]}')
        val_multi = samples_inputs_df.loc[4][2]
        result_val1 = samples_outputs_df.loc[4][1]
        result_val2 = samples_outputs_df.loc[4][2]

        dd_df['values1'] = dd_df['values1'] * (val_multi / 100)
        dd_df['values2'] = dd_df['values2'] * (val_multi / 100)

        result__computed_val1 = dd_df['values1'].cumsum().values[-1]
        result__computed_val2 = dd_df['values2'].cumsum().values[-1]

        self.assertEqual(result_val1, result__computed_val1)
        self.assertEqual(result_val2, result__computed_val2)

    def test_07_grid_search_multipliers_inputs_for_dict(self):
        sa_builder = self.exec_eng.factory.get_builder_from_process(
            self.repo, self.proc_name)

        self.exec_eng.factory.set_builders_to_coupling_builder(
            sa_builder)

        self.exec_eng.configure()
        self.exec_eng.display_treeview_nodes()

        print('Study first configure!')

        dict_values = {
            # DISC1 INPUTS
            f'{self.study_name}.{self.grid_search}.Disc1.name': 'A1',
            f'{self.study_name}.{self.grid_search}.Disc1.b': 2,
            f'{self.study_name}.{self.grid_search}.Disc1.di_dict': {'values1': 100., 'values2': 50.}

        }

        self.exec_eng.load_study_from_input_dict(dict_values)

        # self.exec_eng.dm.get_data('MyCase.GridSearch.eval_inputs')[
        #     'possible_values']

        # dict_values = {}
        # self.exec_eng.load_study_from_input_dict(dict_values)

        eval_inputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_inputs')
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.x', ['selected_input']] = True
        eval_inputs.loc[eval_inputs['full_name'] ==
                        f'{self.grid_search}.Disc1.di_dict_multiplier', ['selected_input']] = True

        eval_outputs = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.eval_outputs')
        eval_outputs.loc[eval_outputs['full_name'] ==
                         f'{self.grid_search}.Disc1.val_sum_dict2', ['selected_output']] = True

        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.eval_inputs': eval_inputs,
            f'{self.study_name}.{self.grid_search}.eval_outputs': eval_outputs,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Second configure with design_space creation: \n {ds}')

        ds['nb_points'] = 3
        dict_values = {
            # GRID SEARCH INPUTS
            f'{self.study_name}.{self.grid_search}.design_space': ds,
        }
        self.exec_eng.load_study_from_input_dict(dict_values)

        self.exec_eng.execute()

        grid_search_disc = self.exec_eng.dm.get_disciplines_with_name(
            f'{self.study_name}.{self.grid_search}')[0]

        di_dict = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.Disc1.di_dict')
        grid_search_disc_output = grid_search_disc.get_sosdisc_outputs()
        samples_inputs_df = grid_search_disc_output['samples_inputs_df']
        samples_outputs_df = grid_search_disc_output['samples_outputs_df']
        val_sum_dict_dict = grid_search_disc_output['GridSearch.Disc1.val_sum_dict2_dict']
        ds = self.exec_eng.dm.get_value(
            f'{self.study_name}.{self.grid_search}.design_space')

        print(f'Study executed from the design_space: \n {ds}')
        print(f'Study executed with the samples: \n {samples_inputs_df}')
        print(f'Outputs used for graphs: \n {samples_outputs_df}')
        print(f'Study generated the output: y_dict \n {val_sum_dict_dict}')

        print(f'Check scenario {samples_inputs_df.loc[4][0]}')
        val_multi = samples_inputs_df.loc[4][2]
        result_val1 = samples_outputs_df.loc[4][1]
        result_val2 = samples_outputs_df.loc[4][2]

        result__computed_val1 = di_dict['values1'] * (val_multi / 100)
        result__computed_val2 = di_dict['values2'] * (val_multi / 100)

        self.assertEqual(result_val1, result__computed_val1)
        self.assertEqual(result_val2, result__computed_val2)


if '__main__' == __name__:
    cls = TestGridSearchEval()
    cls.setUp()
    unittest.main()
