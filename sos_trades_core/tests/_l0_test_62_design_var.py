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
import unittest
import numpy as np
import pandas as pd
from os.path import join, dirname
from pandas import DataFrame, read_csv

from sos_trades_core.execution_engine.execution_engine import ExecutionEngine
from sos_trades_core.tests.core.abstract_jacobian_unit_test import AbstractJacobianUnittest


class TestDesignVar(unittest.TestCase):
    """
    DesignVar test class
    """

    def setUp(self):
        self.study_name = 'optim'
        self.ns = f'{self.study_name}'
        self.sc_name = "SellarOptimScenario"
        self.c_name = "SellarCoupling"

        dspace_dict = {'variable': ['x_in', 'z_in', 'y_1_in', 'y_2_in'],
                       'value': [1., [5., 2.], 1., 1.],
                       'lower_bnd': [0., [-10., 0.], -100., -100.],
                       'upper_bnd': [10., [10., 10.], 100., 100.],
                       'enable_variable': [True, True, True, True],
                       'activated_elem': [[True], [True, True], [True], [True]]}

        self.dspace = pd.DataFrame(dspace_dict)
        self.repo = 'sos_trades_core.sos_processes.test'
        self.proc_name = 'test_sellar_opt_w_design_var'

        self.output_descriptor = {}

        self.output_descriptor['x_in'] = {'out_name': 'x', 'type': 'float', 'namespace_in': 'ns_witness',
                                          'namespace_out': 'ns_witness'}
        self.output_descriptor['z_in'] = {'out_name': 'z', 'type': 'array', 'namespace_in': 'ns_witness',
                                          'namespace_out': 'ns_witness'}
        self.output_descriptor['y_1_in'] = {'out_name': 'y_1', 'type': 'float',
                                            'namespace_in': 'ns_witness', 'namespace_out': 'ns_witness'}
        self.output_descriptor['y_2_in'] = {'out_name': 'y_2', 'type': 'float',
                                            'namespace_in': 'ns_witness', 'namespace_out': 'ns_witness'}

    def test_01_check_execute(self):
        print("\n Test 1 : check configure and treeview")
        exec_eng = ExecutionEngine(self.study_name)
        factory = exec_eng.factory

        opt_builder = factory.get_builder_from_process(repo=self.repo,
                                                       mod_id=self.proc_name)

        exec_eng.factory.set_builders_to_coupling_builder(opt_builder)

        exec_eng.configure()

        #-- set up disciplines in Scenario
        disc_dict = {}
        # Optim inputs
        disc_dict[f'{self.ns}.SellarOptimScenario.max_iter'] = 100
        disc_dict[f'{self.ns}.SellarOptimScenario.algo'] = "SLSQP"
        disc_dict[f'{self.ns}.SellarOptimScenario.design_space'] = self.dspace
        disc_dict[f'{self.ns}.SellarOptimScenario.formulation'] = 'MDF'
        disc_dict[f'{self.ns}.SellarOptimScenario.objective_name'] = 'obj'
        disc_dict[f'{self.ns}.SellarOptimScenario.ineq_constraints'] = [
            f'c_1', f'c_2']

        disc_dict[f'{self.ns}.SellarOptimScenario.algo_options'] = {"ftol_rel": 1e-10,
                                                                    "ineq_tolerance": 2e-3,
                                                                    "normalize_design_space": False}
        exec_eng.dm.set_values_from_dict(disc_dict)

        # Sellar inputs
        values_dict = {}
        local_dv = 10.
        values_dict[f'{self.ns}.{self.sc_name}.x'] = np.array([1.])
        values_dict[f'{self.ns}.{self.sc_name}.y_1'] = np.array([1.])
        values_dict[f'{self.ns}.{self.sc_name}.y_2'] = np.array([1.])
        values_dict[f'{self.ns}.{self.sc_name}.z'] = np.array([1., 1.])
        exec_eng.dm.set_values_from_dict(values_dict)
        values_dict[f'{self.ns}.{self.sc_name}.Sellar_Problem.local_dv'] = local_dv

        exec_eng.dm.set_values_from_dict(values_dict)

        exec_eng.configure()

        exec_eng.execute()

        # disc = exec_eng.dm.get_disciplines_with_name(
        #     f'{self.study_name}.{self.c_name}')[0]



'''
# Copyright 2022 Airbus SAS
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# '''
# import unittest
# import numpy as np
# import pandas as pd
# from os.path import join, dirname
# from pandas import DataFrame, read_csv
#
# from sos_trades_core.execution_engine.execution_engine import ExecutionEngine
# from sos_trades_core.tests.core.abstract_jacobian_unit_test import AbstractJacobianUnittest
#
#
# class DesignVarDisc(AbstractJacobianUnittest):
#     # AbstractJacobianUnittest.DUMP_JACOBIAN = True
#     def analytic_grad_entry(self):
#         return [
#             self.test_derivative
#         ]
#
#     def setUp(self):
#         self.name = 'Test'
#         self.ee = ExecutionEngine(self.name)
#         self.model_name = 'DesignVar'
#         ns_dict = {'ns_witness': f'{self.name}',
#                    'ns_public': f'{self.name}',
#                    'ns_energy_mix': f'{self.name}',
#                    'ns_energy_study': f'{self.name}',
#                    'ns_optim': f'{self.name}',
#                    'ns_ccs': f'{self.name}',
#                    'ns_invest': f'{self.name}',
#                    'ns_agriculture': f'{self.name}'}
#
#         self.ee.ns_manager.add_ns_def(ns_dict)
#
#         mod_path = 'climateeconomics.core.design_variables_translation.witness_bspline_invest_distrib.design_var_disc.DesignVarDiscipline'
#         builder = self.ee.factory.get_builder_from_module(
#             self.model_name, mod_path)
#
#         self.ee.factory.set_builders_to_coupling_builder(builder)
#
#         self.ee.configure()
#         self.ee.display_treeview_nodes()
#         energy_mix_array = list(np.linspace(0.0, 100.0, 8))
#         forest_investment_ctrl = list(np.linspace(0.0, 100.0, 8))
#         deforested_surface_ctrl = list(np.linspace(0.0, 20.0, 8))
#         red_to_white_meat_ctrl = list(np.linspace(0.0, 50.0, 8))
#         meat_to_vegetables_ctrl = list(np.linspace(0.0, 60.0, 8))
#         years = np.arange(2020, 2101, 1)
#
#         self.energy_list = ['methane', 'liquid_fuel', 'electricity']
#         self.ccs_list = ['carbon_capture', 'carbon_storage']
#         self.dict_technos = {}
#         self.dict_technos['methane'] = ['FossilGas', 'UpgradingBioGas']
#         self.dict_technos['liquid_fuel'] = ['Refinery', 'FischerTropsch']
#         self.dict_technos['carbon_capture'] = ['Capture1', 'Capture2']
#         self.dict_technos[ 'carbon_storage'] = ['Storage1', 'Storage2']
#         self.dict_technos['electricity'] = ['CoalGen', 'Nuclear', 'SolarPV']
#
#         self.output_descriptor = {}
#
#         self.output_descriptor['forest_investment_ctrl'] = {'out_name': 'forest_investment', 'type': 'dataframe',
#                                                        'key': 'forest_investment', 'namespace_in': 'ns_witness',
#                                                        'namespace_out': 'ns_witness'}
#
#         self.output_descriptor['deforested_surface_ctrl'] = {'out_name': 'deforestation_surface', 'type': 'dataframe',
#                                                         'key': 'deforested_surface', 'namespace_in': 'ns_witness',
#                                                         'namespace_out': 'ns_witness'}
#
#         self.output_descriptor['red_to_white_meat_ctrl'] = {'out_name': 'red_to_white_meat', 'type': 'array',
#                                                        'namespace_in': 'ns_witness', 'namespace_out': 'ns_witness'}
#
#         self.output_descriptor['meat_to_vegetables_ctrl'] = {'out_name': 'meat_to_vegetables', 'type': 'array',
#                                                         'namespace_in': 'ns_witness', 'namespace_out': 'ns_witness'}
#
#         for energy in self.energy_list:
#             energy_wo_dot = energy.replace('.', '_')
#             for technology in self.dict_technos[energy]:
#                 technology_wo_dot = technology.replace('.', '_')
#                 self.output_descriptor[f'{energy}.{technology}.{energy_wo_dot}_{technology_wo_dot}_array_mix'] = {'out_name': 'invest_mix', 'type': 'dataframe', 'key': f'{energy}.{technology}', 'namespace_in': 'ns_energy_mix', 'namespace_out': 'ns_invest'}
#
#         for ccs in self.ccs_list:
#             ccs_wo_dot = ccs.replace('.', '_')
#             for technology in self.dict_technos[ccs]:
#                 technology_wo_dot = technology.replace('.', '_')
#                 self.output_descriptor[f'{ccs}.{technology}.{ccs_wo_dot}_{technology_wo_dot}_array_mix'] = {'out_name': 'invest_mix', 'type': 'dataframe', 'key': f'{ccs}.{technology}', 'namespace_in': 'ns_ccs', 'namespace_out': 'ns_invest'}
#
#         values_dict = {f'{self.name}.forest_investment_ctrl': forest_investment_ctrl,
#                        f'{self.name}.deforested_surface_ctrl': deforested_surface_ctrl,
#                        f'{self.name}.red_to_white_meat_ctrl': red_to_white_meat_ctrl,
#                        f'{self.name}.meat_to_vegetables_ctrl': meat_to_vegetables_ctrl,
#                        f'{self.name}.DesignVar.output_descriptor': self.output_descriptor,
#                        }
#         self.input_names = []
#         ddict = {}
#         for energy in self.energy_list + self.ccs_list:
#             energy_wo_dot = energy.replace('.', '_')
#             for techno in self.dict_technos[energy]:
#                 techno_wo_dot = techno.replace('.', '_')
#                 invest_mix_name = f'{self.name}.{energy}.{techno}.{energy_wo_dot}_{techno_wo_dot}_array_mix'
#                 invest_mix_name_wo = f'{energy}.{techno}.{energy_wo_dot}_{techno_wo_dot}_array_mix'
#
#                 ddict[invest_mix_name_wo] = {'value': energy_mix_array,
#                                              'lower_bnd': 1, 'upper_bnd': 100, 'enable_variable': True, 'activated_elem': [True, True, True, True, True, True, True]}
#                 values_dict[invest_mix_name] = energy_mix_array
#                 self.input_names.append(invest_mix_name)
#
#         ddict['forest_investment_ctrl'] = {'value': forest_investment_ctrl,
#                                            'lower_bnd': 0.0, 'upper_bnd': 100.0, 'enable_variable': True, 'activated_elem': [True, True, True, True, True, True, True]}
#         ddict['deforested_surface_ctrl'] = {'value': deforested_surface_ctrl,
#                                             'lower_bnd': 0.0, 'upper_bnd': 20.0, 'enable_variable': True, 'activated_elem': [True, True, True, True, True, True, True]}
#         ddict['red_to_white_meat_ctrl'] = {'value': red_to_white_meat_ctrl,
#                                            'lower_bnd': 0.0, 'upper_bnd': 50.0, 'enable_variable': True, 'activated_elem': [True, True, True, True, True, True, True]}
#         ddict['meat_to_vegetables_ctrl'] = {'value': meat_to_vegetables_ctrl,
#                                             'lower_bnd': 0.0, 'upper_bnd': 50.0, 'enable_variable': True, 'activated_elem': [True, True, True, True, True, True, True]}
#
#         dspace_df_columns = ['variable', 'value', 'lower_bnd',
#                              'upper_bnd', 'enable_variable', 'activated_elem']
#         dspace_df = pd.DataFrame(columns=dspace_df_columns)
#
#         for key, elem in ddict.items():
#             dict_var = {'variable': key}
#             dict_var.update(elem)
#             dspace_df = dspace_df.append(dict_var, ignore_index=True)
#
#         values_dict.update({f'{self.name}.design_space': dspace_df})
#
#         self.ee.load_study_from_input_dict(values_dict)
#
#     def test_execute(self):
#
#         self.ee.execute()
#
#         disc = self.ee.dm.get_disciplines_with_name(
#             f'{self.name}.{self.model_name}')[0]
#         filterr = disc.get_chart_filter_list()
#         graph_list = disc.get_post_processing_list(filterr)
#         # for graph in graph_list:
#         #    graph.to_plotly().show()
#
#     def test_derivative(self):
#         disc_techno = self.ee.root_process.sos_disciplines[0]
#         #AbstractJacobianUnittest.DUMP_JACOBIAN = True
#         output_names = [f'{self.name}.invest_mix',
#                         f'{self.name}.deforestation_surface',
#                         f'{self.name}.forest_investment',
#                         f'{self.name}.red_to_white_meat',
#                         f'{self.name}.meat_to_vegetables']
#
#         self.check_jacobian(location=dirname(__file__), filename=f'jacobian_design_var_bspline_invest_distrib_full.pkl', discipline=disc_techno, step=1e-15, inputs=self.input_names,
#                             outputs=output_names, derr_approx='complex_step')
