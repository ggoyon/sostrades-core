import pandas as pd
import numpy as np
from os.path import dirname
from itertools import product

from sostrades_core.execution_engine.execution_engine import ExecutionEngine
from sostrades_core.tests.core.abstract_jacobian_unit_test import AbstractJacobianUnittest
from sostrades_core.execution_engine.design_var.design_var_disc import DesignVarDiscipline


class GradiantAssetDiscTestCase(AbstractJacobianUnittest):
    """
    AssetDisc gradients test class
    """
    #AbstractJacobianUnittest.DUMP_JACOBIAN = True
    np.random.seed(42)

    def analytic_grad_entry(self):
        return [
            self.test_01_analytic_gradient_default_dataframe_fill(),
            self.test_02_analytic_gradient_dataframe_fill_one_column_for_key(),
        ]

    def setUp(self):
        """Initialize"""
        self.study_name = 'Test'
        self.ns = f'{self.study_name}'

        dspace_dict = {'variable': ['x_in', 'z_in'],
                       'value': [[1., 1., 3., 2.], [5., 2., 2., 1., 1., 1.]],
                       'lower_bnd': [[0., 0., 0., 0.], [-10., 0., -10., -10., -10., -10.]],
                       'upper_bnd': [[10., 10., 10., 10.], [10., 10., 10., 10., 10., 10.]],
                       'enable_variable': [True, True],
                       'activated_elem': [[True], [True, True]]}
        self.dspace = pd.DataFrame(dspace_dict)

        self.design_var_descriptor = {'x_in': {'out_name': 'x',
                                               'type': 'array',
                                               'out_type': 'array',
                                               'index': np.arange(0, 4, 1),
                                               'index_name': 'test',
                                               'namespace_in': 'ns_public',
                                               'namespace_out': 'ns_public'
                                               },
                                      'z_in': {'out_name': 'z',
                                               'type': 'array',
                                               'out_type': 'array',
                                               'index': np.arange(0, 6, 1),
                                               'index_name': 'index',
                                               'namespace_in': 'ns_public',
                                               'namespace_out': 'ns_public'
                                               }
                                      }

        self.ee = ExecutionEngine(self.study_name)
        factory = self.ee.factory
        design_var_path = 'sostrades_core.execution_engine.design_var.design_var_disc.DesignVarDiscipline'
        design_var_builder = factory.get_builder_from_module('DesignVar', design_var_path)
        self.ee.ns_manager.add_ns_def({'ns_public': self.ns,
                                       'ns_optim': self.ns})
        self.ee.factory.set_builders_to_coupling_builder(design_var_builder)
        self.ee.configure()

        # -- set up disciplines in Scenario
        values_dict = {}

        # design var
        values_dict[
            f'{self.ns}.DesignVar.design_var_descriptor'] = self.design_var_descriptor
        values_dict[
            f'{self.ns}.design_space'] = self.dspace
        values_dict[f'{self.ns}.x_in'] = np.array([1., 1., 3., 2.])
        values_dict[f'{self.ns}.z_in'] = np.array([5., 2., 2., 1., 1., 1.])
        self.values_dict = values_dict

    def tearDown(self):
        pass

    def test_01_analytic_gradient_default_dataframe_fill(self):
        """Test gradient with default design var dataframe description, namely 'one column per key' """
        self.ee.load_study_from_input_dict(self.values_dict)
        self.ee.configure()
        self.ee.execute()

        disc = self.ee.root_process.proxy_disciplines[0].mdo_discipline_wrapp.mdo_discipline

        self.check_jacobian(location=dirname(__file__), filename=f'jacobian_{self.study_name}_default_dataframe_fill.pkl',
                            discipline=disc,
                            step=1e-16,
                            derr_approx='complex_step',
                            threshold=1e-5,
                            local_data=disc.local_data,
                            inputs=[f'{self.ns}.x_in', f'{self.ns}.z_in'],
                            outputs=[f'{self.ns}.x', f'{self.ns}.z']
                            )

    def test_02_analytic_gradient_dataframe_fill_one_column_for_key(self):
        """Test gradient with design var dataframe description 'one column for key, one column for value' """
        self.design_var_descriptor = {'x_in': {'out_name': 'x',
                                               'type': 'array',
                                               'out_type': 'dataframe',
                                               'key': 'value',
                                               'index': np.arange(0, 4, 1),
                                               'index_name': 'years',
                                               'namespace_in': 'ns_public',
                                               'namespace_out': 'ns_public',
                                               DesignVarDiscipline.DATAFRAME_FILL:
                                                   DesignVarDiscipline.DATAFRAME_FILL_POSSIBLE_VALUES[1],
                                               DesignVarDiscipline.COLUMNS_NAMES: ['name', 'sharevalue']
                                               },
                                      'z_in': {'out_name': 'z',
                                               'type': 'array',
                                               'out_type': 'dataframe',
                                               'key': 'value',
                                               'index': np.arange(0, 10, 1),
                                               'index_name': 'years',
                                               'namespace_in': 'ns_public',
                                               'namespace_out': 'ns_public',
                                               DesignVarDiscipline.DATAFRAME_FILL:
                                                   DesignVarDiscipline.DATAFRAME_FILL_POSSIBLE_VALUES[1],
                                               DesignVarDiscipline.COLUMNS_NAMES: ['name', 'sharevalue']
                                               },
                                      }
        self.values_dict[
            f'{self.ns}.DesignVar.design_var_descriptor'] = self.design_var_descriptor
        self.ee.load_study_from_input_dict(self.values_dict)
        self.ee.configure()
        self.ee.execute()

        disc = self.ee.root_process.proxy_disciplines[0].mdo_discipline_wrapp.mdo_discipline

        self.check_jacobian(location=dirname(__file__), filename=f'jacobian_{self.study_name}_dataframe_fill_one_column_for_key.pkl',
                            discipline=disc,
                            step=1e-16,
                            derr_approx='complex_step',
                            threshold=1e-5,
                            local_data=disc.local_data,
                            inputs=[f'{self.ns}.x_in', f'{self.ns}.z_in'],
                            outputs=[f'{self.ns}.x', f'{self.ns}.z']
                            )