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
from sos_trades_core.study_manager.study_manager import StudyManager
from numpy import array
import pandas as pd


class Study(StudyManager):

    def __init__(self, execution_engine=None):
        super().__init__(__file__, execution_engine=execution_engine)

    def setup_usecase(self):
        ns = f'{self.study_name}'
        sc_name = "SobOptimScenario"
        
        dspace_dict = {'variable': ['z', 'x_1', 'x_2', 'x_3'],
               'value': [
                        [0.05, 45000.0,1.6,5.5,55.0,1000.0], 
                        [0.25, 1.0], 
                        [1.0],
                        [0.5]],
               'lower_bnd': [
                        [0.01,30000.0,1.4,2.5,40.0,500.0], 
                        [0.1,0.75], 
                        [0.75],
                        [0.1]],
               'upper_bnd': [
                        [0.09,60000.0,1.8,8.5,70.0,1500.0], 
                        [0.4,1.25], 
                        [1.25],
                        [1.0]],
               'enable_variable': [True, True, True, True],
               'activated_elem': [[True, True, True, True, True, True], [True, True], [True], [True]]}
        dspace = pd.DataFrame(dspace_dict)
        
        dspace_coupl_dict = {'variable': ['y_14','y_32','y_31','y_24','y_34','y_23','y_21','y_12'],
                       'value': [
                                [50606.9741711,7306.20262124], 
                                [0.50279625], 
                                [6354.32430691],
                                [4.15006276],
                                [1.10754577],
                                [12194.2671934],
                                [50606.9741711],
                                [50606.9742,0.95]
                                ],
                       'lower_bnd':  [
                                [24850.0,-7700.0], 
                                [0.235], 
                                [2960.0],
                                [0.44],
                                [0.44],
                                [3365.0],
                                [24850.0],
                                [24850.0,0.45]
                                ],
                       'upper_bnd':   [
                                [77100.0,45000.0], 
                                [0.795], 
                                [10185.0],
                                [11.13],
                                [1.98],
                                [26400.0],
                                [77250.0],
                                [77250.0,1.5]
                                ],
                       'enable_variable': [True, True, True, True, True, True, True, True],
                       'activated_elem': [[True, True], [True], [True], [True], [True], [True], [True],[True, True]]}
                
        dspace_coupl = pd.DataFrame(dspace_coupl_dict)
        
        
        frames = [dspace,dspace_coupl]
        dspace_all = pd.concat(frames)
        
        
        
        
        disc_dict = {}
        # Optim inputs
        #SobOptimScenario inputs
        disc_dict[f'{ns}.{sc_name}.max_iter'] = 10
        disc_dict[f'{ns}.{sc_name}.algo'] = "SLSQP"
        disc_dict[f'{ns}.{sc_name}.design_space'] = dspace_all
        #disc_dict[f'{ns}.{sc_name}.design_space'] = dspace # does not work!
        disc_dict[f'{ns}.{sc_name}.formulation'] = 'MDF'
        disc_dict[f'{ns}.{sc_name}.objective_name'] = 'y_4'
        disc_dict[f'{ns}.{sc_name}.maximize_objective'] = True
        disc_dict[f'{ns}.{sc_name}.ineq_constraints'] = [f'g_1', f'g_2', f'g_3']
        disc_dict[f'{ns}.{sc_name}.algo_options'] = {"ftol_rel": 1e-10,
                                                                    "ineq_tolerance": 2e-3,
                                                                    "normalize_design_space": True}
        disc_dict[f'{ns}.tolerance'] = 1e-10
        disc_dict[f'{ns}.max_mda_iter'] = 50
        disc_dict[f'{ns}.warm_start'] = True
        disc_dict[f'{ns}.use_lu_fact'] = True
        disc_dict[f'{ns}.{sc_name}.struct.cache_type'] = 'SimpleCache'
        disc_dict[f'{ns}.{sc_name}.aero.cache_type'] = 'SimpleCache'
        disc_dict[f'{ns}.{sc_name}.prop.cache_type'] = 'SimpleCache'
        disc_dict[f'{ns}.{sc_name}.cache_type'] = 'SimpleCache'
        # Disciplines inputs
        disc_dict[f'{ns}.{sc_name}.z'] = [0.05,45000,1.6,5.5,55.,1000]
        disc_dict[f'{ns}.{sc_name}.y_14'] = [50606.9,7306.20]
        disc_dict[f'{ns}.{sc_name}.y_24'] =  [4.15]
        disc_dict[f'{ns}.{sc_name}.y_34'] = [1.10]
        disc_dict[f'{ns}.{sc_name}.x_1'] = [0.25,1.0]
        disc_dict[f'{ns}.{sc_name}.y_21'] =  [50606.9]
        disc_dict[f'{ns}.{sc_name}.y_31'] = [6354.32]
        disc_dict[f'{ns}.{sc_name}.x_2'] = [1.0]
        disc_dict[f'{ns}.{sc_name}.y_12'] =  [50606.9,0.95]
        disc_dict[f'{ns}.{sc_name}.y_32'] = [12194.2]
        disc_dict[f'{ns}.{sc_name}.x_3'] = [0.5]
        disc_dict[f'{ns}.{sc_name}.y_23'] =  [12194.2]
        return [disc_dict]


if '__main__' == __name__:
    uc_cls = Study()
    uc_cls.load_data()
    uc_cls.execution_engine.display_treeview_nodes(display_variables=True)
    uc_cls.run()
