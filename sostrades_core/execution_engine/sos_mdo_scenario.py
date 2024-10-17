'''
Copyright 2022 Airbus SAS
Modifications on 2023/04/13-2024/06/07 Copyright 2023 Capgemini

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
from copy import deepcopy
from typing import Any

import numpy as np
import pandas as pd
from gemseo.scenarios.mdo_scenario import MDOScenario

from sostrades_core.execution_engine.sos_discipline import SoSDiscipline


class SoSMDOScenario(MDOScenario):
    """
    Generic implementation of Optimization Scenario
    """
    # Default values of algorithms

    # ontology information
    _ontology_data = {
        'label': 'Scenario Optimization Model',
        'type': 'Official',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fas fa-bezier-curve fa-fw',
        'version': '',
    }

    POST_PROC_MDO_DATA = 'post_processing_mdo_data'

    def __init__(self,
                 disciplines,
                 name,
                 formulation,
                 objective_name,
                 design_space,
                 logger: logging.Logger,
                 reduced_dm=None):
        """
        Constructor
        """
        self.logger = logger
        self.formulation = formulation
        self.objective_name = objective_name
        self.name = name
        super().__init__(disciplines,
                         self.formulation,
                         self.objective_name,
                         design_space,
                         name=self.name)
        self.maximize_objective = None
        self.algo_name = None
        self.algo_options = None
        self.max_iter = None
        self.eval_mode = False
        self.eval_jac = False
        self.dict_desactivated_elem = None
        self.input_design_space = None
        self.reduced_dm = reduced_dm
        self.activated_variables = self.formulation.design_space.variable_names
        self.is_sos_coupling = False
        self.mdo_options = {}

    def _update_input_grammar(self) -> None:
        pass

        # desactivate designspace outputs for post processings
        self.desactivate_optim_out_storage = False

    def _run(self):
        '''

        '''
        self.execution_status.value = self.execution_status.Status.RUNNING

        if self.eval_mode:
            self.run_eval_mode()
        else:
            self.run_scenario()
        outputs = [discipline.get_output_data()
                   for discipline in self._disciplines]
        for data in outputs:
            self.local_data.update(data)

        self.add_design_space_inputs_to_local_data()
        # save or not the output of design space for post processings
        if not self.desactivate_optim_out_storage:
            self.update_design_space_out()
            post_processing_mdo_data = {}
            if not self.eval_mode:
                post_processing_mdo_data = self.update_post_processing_df()
            self.local_data.update({
                [key for key in self.get_output_data_names() if self.POST_PROC_MDO_DATA in key][
                    0]: post_processing_mdo_data})

    def update_post_processing_df(self):
        """Gathers the data for plotting the MDO graphs"""
        dataset = self.to_dataset()
        dataframe = dataset.copy()
        # quick fix to avoind NaN in the resulting dataframe
        # context : empty fields due to several calls to the same design space lead to NaN in dataframes
        # TODO: post proc this dataframe (or directly retrieve values from database) so that NaN values are replaced by already computed values
        dataframe = dataframe.fillna(-1)
        # dataframe = dataframe.rename(columns=rename_func)

        constraints_names = [constraint.name for constraint in
                             self.formulation.optimization_problem.constraints]
        objective_name = self.formulation.optimization_problem.objective.name

        def correct_var_name(varname: str) -> str:
            """removes study name from variable name"""
            corrected_var_name = ".".join(varname.split(".")[1:])
            return corrected_var_name

        post_processing_mdo_data = {
            "objective": np.array(dataframe[dataframe.FUNCTION_GROUP][objective_name].values),
            "variables": {correct_var_name(var): np.array(dataframe[dataframe.DESIGN_GROUP][var].values) for var in
                          self.design_space.variable_names},
            "constraints": {correct_var_name(var): np.array(dataframe[dataframe.FUNCTION_GROUP][var].values) for var in
                            constraints_names}
        }
        return post_processing_mdo_data

    def execute_at_xopt(self):
        '''
        trigger post run if execute at optimum is activated
        '''
        self.logger.info("Post run at xopt")
        self._post_run()

    def _run_algorithm(self):
        '''
        Run the chosen algorithm with algo options and max_iter
        '''
        problem = self.formulation.optimization_problem
        # Clears the database when multiple runs are performed (bi level)
        if self.clear_history_before_run:
            problem.database.clear()
        algo_name = self.algo_name
        max_iter = self.max_iter
        options = self.algo_options
        if options is None:
            options = {}
        if "max_iter" in options:
            self.logger.warning("Double definition of algorithm option " +
                                "max_iter, keeping value: " + str(max_iter))
            options.pop("max_iter")
        lib = self._algo_factory.create(algo_name)
        self.logger.info(options)

        self.preprocess_functions()

        self.optimization_result = lib.execute(problem,
                                               max_iter=max_iter,
                                               **options)
        self.clear_jacobian()
        return self.optimization_result

    def clear_jacobian(self):
        return SoSDiscipline.clear_jacobian(self)  # should rather be double inheritance

    def run_scenario(self):
        '''
        Call to the GEMSEO MDOScenario run and update design_space_out
        Post run is possible if execute_at_xopt is activated
        '''
        MDOScenario._run(self)

        self.execute_at_xopt()

    def run_eval_mode(self):
        '''
        Run evaluate functions with the initial x
        jacobian_functions: The functions computing the Jacobians.
                If ``None``, evaluate all the functions computing Jacobians.
                If empty, do not evaluate functions computing Jacobians.
        '''
        output_functions, _ = self.formulation.optimization_problem.get_functions()
        jacobian_functions = []
        if self.eval_jac:
            jacobian_functions = output_functions

        self.formulation.optimization_problem.evaluate_functions(output_functions=output_functions,
                                                                 jacobian_functions=jacobian_functions)

        # self.io.update_output_data(**local_data)
        # if eval mode design space was not modified
        # self.store_sos_outputs_values(
        #     {'design_space_out': self.formulation.design_space}, update_dm=True)

    def preprocess_functions(self):
        """
        preprocess functions to store functions list
        """

        problem = self.formulation.optimization_problem
        normalize = self.algo_options['normalize_design_space']

        # preprocess functions
        problem.preprocess_functions(is_function_input_normalized=normalize)
        functions = list(problem.constraints.get_originals()) + [problem.objective.original]

        self.functions_before_run = functions

    def set_design_space_for_complex_step(self):
        '''
        Set design space values to complex if the differentiation method is complex_step
        '''

        if self.formulation.optimization_problem.differentiation_method == self.COMPLEX_STEP:
            dspace = deepcopy(self.opt_problem.design_space)
            curr_x = dspace._current_x
            for var in curr_x:
                curr_x[var] = curr_x[var].astype('complex128')
            self.formulation.optimization_problem.design_space = dspace

    def _post_run(self):
        """
        Post-processes the scenario.
        """
        formulation = self.formulation
        problem = formulation.optimization_problem
        design_space = problem.design_space
        normalize = self.algo_options[
            'normalize_design_space']
        # Test if the last evaluation is the optimum
        x_opt = design_space.get_current_value()
        try:
            # get xopt from x_opt
            x_opt_result = problem.solution.x_opt
            self.logger.info(f"Executing at xopt point {x_opt}")
            self.logger.info(f"x_opt from problem solution is {x_opt_result}")
        except:
            self.logger.info(f"Exception {problem.solution}")
            pass
        # Revaluate all functions at optimum
        # To re execute all disciplines and get the right data

        # self.logger.info(
        #    f"problem database {problem.database._Database__dict}")
        try:

            self.evaluate_functions(problem, x_opt)

        except:
            self.logger.warning(
                "Warning: executing the functions in the except after nominal execution of post run failed")

            for func in self.functions_before_run:
                func.evaluate(x_opt)

    def evaluate_functions(self,
                           problem,
                           x_vect=None,  # type: ndarray
                           ):  # type: (...) -> tuple[dict[str,Union[float,ndarray]],dict[str,ndarray]]
        """Compute the objective and the constraints.

        amples.

        Args:
            x_vect: The input vector at which the functions must be evaluated;
                if None, x_0 is used.
            problem: opt problem object


        """
        functions = list(problem.constraints.get_originals()) + [problem.objective.original]
        self.logger.info(f'list of functions to evaluate {functions}')

        for func in functions:
            try:
                func.evaluate(x_vect)
            except ValueError:
                self.logger.error("Failed to evaluate function %s", func.name)
                raise
            except TypeError:
                self.logger.error("Failed to evaluate function %s", func)
                raise

    def add_design_space_inputs_to_local_data(self):
        '''

        Add Design space inputs values to the local_data to store it in the dm

        '''

        problem = self.formulation.optimization_problem

        if problem.solution is not None:
            x = problem.solution.x_opt
        else:
            x = problem.design_space.get_current_value()
        current_idx = 0
        for k, v in problem.design_space._variables.items():
            k_size = v.size
            # WARNING we fill input in local_data that will be deleted by GEMSEO because they are not outputs ...
            # Only solution is to specify design space inputs as outputs of the mdoscenario
            self.local_data.update({k: x[current_idx:current_idx + k_size]})
            current_idx += k_size

    def update_design_space_out(self):
        """
        Method to update design space with opt value
        """
        design_space = deepcopy(self.input_design_space)
        l_variables = design_space['variable']

        for var_name in l_variables:
            var_name = var_name.split('.')[-1]
            full_name_var = self.get_namespace_from_var_name(var_name)
            if full_name_var in self.activated_variables:
                value_x_opt = list([self.formulation.design_space.get_current_value(
                    [full_name_var])])
                if self.dict_desactivated_elem[full_name_var] != {}:
                    # insert a desactivated element
                    for _pos, _val in zip(self.dict_desactivated_elem[full_name_var]['position'],
                                          self.dict_desactivated_elem[full_name_var]['value']):
                        value_x_opt.insert(_pos, _val)

                design_space.loc[design_space['variable'] == var_name, 'value'] = pd.Series(
                    [value_x_opt] * len(design_space))
        self.local_data.update({
            [key for key in self.get_output_data_names() if 'design_space_out' in key][
                0]: design_space})

    def get_namespace_from_var_name(self, var_name):
        subcoupling = self.disciplines[0]
        namespace_list = [full_name for full_name in subcoupling.get_input_data_names() if
                          (var_name == full_name.split('.')[-1] or var_name == full_name)]
        if len(namespace_list) == 1:
            return namespace_list[0]
        else:
            raise Exception(
                f'Cannot find the variable {var_name} in the sub-coupling input grammar of the optim scenario {self.name}')
