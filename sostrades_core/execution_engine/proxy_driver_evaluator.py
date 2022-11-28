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
mode: python; py-indent-offset: 4; tab-width: 8; coding: utf-8
'''

import copy
import pandas as pd
from numpy import NaN


from sostrades_core.api import get_sos_logger
from sostrades_core.execution_engine.sos_wrapp import SoSWrapp
from sostrades_core.execution_engine.proxy_discipline import ProxyDiscipline
from sostrades_core.execution_engine.proxy_coupling import ProxyCoupling
from sostrades_core.execution_engine.proxy_discipline_builder import ProxyDisciplineBuilder
from sostrades_core.execution_engine.mdo_discipline_driver_wrapp import MDODisciplineDriverWrapp
from sostrades_core.execution_engine.disciplines_wrappers.driver_evaluator_wrapper import DriverEvaluatorWrapper
from sostrades_core.execution_engine.disciplines_wrappers.sample_generator_wrapper import SampleGeneratorWrapper
from sostrades_core.execution_engine.builder_tools.tool_builder import ToolBuilder
from gemseo.utils.compare_data_manager_tooling import dict_are_equal


class ProxyDriverEvaluatorException(Exception):
    pass


class ProxyDriverEvaluator(ProxyDisciplineBuilder):
    '''
        SOSEval class which creates a sub process to evaluate
        with different methods (Gradient,FORM,Sensitivity ANalysis, DOE, ...)
    '''

    # ontology information
    _ontology_data = {
        'label': 'Driver Evaluator',
        'type': 'Official',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': '',
        'version': '',
    }

    BUILDER_MODE = DriverEvaluatorWrapper.BUILDER_MODE
    MONO_INSTANCE = DriverEvaluatorWrapper.MONO_INSTANCE
    MULTI_INSTANCE = DriverEvaluatorWrapper.MULTI_INSTANCE
    REGULAR_BUILD = DriverEvaluatorWrapper.REGULAR_BUILD
    BUILDER_MODE_POSSIBLE_VALUES = DriverEvaluatorWrapper.BUILDER_MODE_POSSIBLE_VALUES

    SCENARIO_DF = 'scenario_df'

    SELECTED_SCENARIO = 'selected_scenario'
    SCENARIO_NAME = 'scenario_name'
    SUBCOUPLING_NAME = 'subprocess'
    EVAL_INPUT_TYPE = ['float', 'array', 'int', 'string']

    GENERATED_SAMPLES = SampleGeneratorWrapper.GENERATED_SAMPLES



    def __init__(self, sos_name, ee, cls_builder,
                 driver_wrapper_cls=None,
                 associated_namespaces=None,
                 builder_tool=None):
        """
        Constructor

        Arguments:
            sos_name (string): name of the discipline/node
            ee (ExecutionEngine): execution engine of the current process
            cls_builder (List[SoSBuilder]): list of the sub proxy builders
            driver_wrapper_cls (Class): class constructor of the driver wrapper (user-defined wrapper or SoSTrades wrapper or None)
            map_name (string): name of the map associated to the scatter builder in case of multi-instance build
            associated_namespaces(List[string]): list containing ns ids ['name__value'] for namespaces associated to builder
        """
        # if 'ns_doe' does not exist in ns_manager, we create this new
        # namespace to store output dictionaries associated to eval_outputs
        if 'ns_doe' not in ee.ns_manager.shared_ns_dict.keys():
            ee.ns_manager.add_ns('ns_doe', ee.study_name)

        super().__init__(sos_name, ee, driver_wrapper_cls,
                         associated_namespaces=associated_namespaces)
        if cls_builder is not None:
            self.cls_builder = cls_builder
        else:
            raise Exception(
                'The driver evaluator builder must have a cls_builder to work')

        self.builder_tool = None
        if builder_tool is not None:
            if not isinstance(builder_tool, ToolBuilder):
                self.logger.error(
                    f'The given builder tool {builder_tool} is not a tool builder')
            self.builder_tool_cls = builder_tool
        else:
            self.builder_tool_cls = None

        self.old_builder_mode = None
        self.eval_process_builder = None
        self.eval_in_list = None
        self.eval_out_list = None
        self.selected_outputs = []
        self.selected_inputs = []
        self.eval_out_type = []
        self.eval_out_list_size = []
        self.logger = get_sos_logger(f'{self.ee.logger.name}.DriverEvaluator')

        self.old_samples_df, self.old_scenario_df = ({}, {})

    def get_desc_in_out(self, io_type):
        """
        get the desc_in or desc_out. if a wrapper exists get it from the wrapper, otherwise get it from the proxy class
        """
        # TODO : check if the following logic could be OK,
        # according to what we want to do : DESC_IN of Proxy is updated by SoSWrapp if exists
        # thus no mixed calls to n-1 and n-2

        if self.mdo_discipline_wrapp.wrapper is not None:
            # ProxyDiscipline gets the DESC from the wrapper
            return ProxyDiscipline.get_desc_in_out(self, io_type)
        else:
            # ProxyDisciplineBuilder expects the DESC on the proxies e.g. Coupling
            # TODO: move to coupling ?
            return super().get_desc_in_out(io_type)

    def create_mdo_discipline_wrap(self, name, wrapper, wrapping_mode):
        """
        creation of mdo_discipline_wrapp by the proxy which in this case is a MDODisciplineDriverWrapp that will create
        a SoSMDODisciplineDriver at prepare_execution, i.e. a driver node that knows its subprocesses but manipulates
        them in a different way than a coupling.
        """
        self.mdo_discipline_wrapp = MDODisciplineDriverWrapp(
            name, wrapper, wrapping_mode)

    def configure(self):
        """
        Configure the DriverEvaluator layer
        """
        if self.builder_tool_cls:
            self.configure_tool()
        # configure al processes stored in children
        for disc in self.get_disciplines_to_configure():
            disc.configure()

        # configure current discipline DriverEvaluator
        # if self._data_in == {} or (self.get_disciplines_to_configure() == []
        # and len(self.proxy_disciplines) != 0) or len(self.cls_builder) == 0:
        if self._data_in == {} or self.subprocess_is_configured():
            # Call standard configure methods to set the process discipline
            # tree
            # TODO: use super()
            super().configure()
            self.configure_driver()

        if self.subprocess_is_configured():
            self.update_data_io_with_subprocess_io()
            self.set_children_cache_inputs()

    def update_data_io_with_subprocess_io(self):
        """
        Update the DriverEvaluator _data_in and _data_out with subprocess i/o so that grammar of the driver can be
        exploited for couplings etc.
        """
        self._restart_data_io_to_disc_io()
        # TODO: working because no two different discs share a local ns
        for proxy_disc in self.proxy_disciplines:
            # if not isinstance(proxy_disc, ProxyDisciplineGather):
            subprocess_data_in = proxy_disc.get_data_io_with_full_name(
                self.IO_TYPE_IN, as_namespaced_tuple=True)
            subprocess_data_out = proxy_disc.get_data_io_with_full_name(
                self.IO_TYPE_OUT, as_namespaced_tuple=True)
            self._update_data_io(subprocess_data_in, self.IO_TYPE_IN)
            self._update_data_io(subprocess_data_out, self.IO_TYPE_OUT)

    def configure_driver(self):
        """
        To be overload by drivers with specific configuration actions
        """
        # Extract variables for eval analysis in mono instance mode
        disc_in = self.get_data_in()
        if self.BUILDER_MODE in disc_in:
            if self.get_sosdisc_inputs(self.BUILDER_MODE) == self.MONO_INSTANCE \
                    and 'eval_inputs' in disc_in and len(self.proxy_disciplines) > 0:
                self.set_eval_possible_values()
            # TODO: check if the configuration of the scenarios ~scatter_data is to be done here
            # TODO: ok for successive configurations ? test
            elif self.get_sosdisc_inputs(self.BUILDER_MODE) == self.MULTI_INSTANCE and self.SCENARIO_DF in disc_in:
                self.configure_subprocesses_with_driver_input()

    def configure_subprocesses_with_driver_input(self):
        # TODO: code below might need refactoring after reference_scenario
        # configuration fashion is decided upon
        scenario_df = self.get_sosdisc_inputs(self.SCENARIO_DF)
        # NB assuming that the scenario_df entries are unique otherwise there
        # is some intelligence to be added
        scenario_names = scenario_df[scenario_df[self.SELECTED_SCENARIO]
                                     == True][self.SCENARIO_NAME].values.tolist()
        # check that all the input scenarios have indeed been built
        # (configuration sequence allows the opposite)
        if self.subprocesses_built(scenario_names):
            var_names = [col for col in scenario_df.columns if col not in [
                self.SELECTED_SCENARIO, self.SCENARIO_NAME]]
            # check that there are indeed variable changes input, with respect
            # to reference scenario
            if var_names:
                driver_evaluator_ns = self.ee.ns_manager.get_local_namespace_value(
                    self)
                scenarios_data_dict = {}
                for sc in scenario_names:
                    sc_row = scenario_df[scenario_df[self.SCENARIO_NAME] == sc].iloc[0] # assuming it is unique # TODO: as index?
                    for var in var_names:
                        var_full_name = self.ee.ns_manager.compose_ns([driver_evaluator_ns, sc, var])
                        scenarios_data_dict[var_full_name] = sc_row.loc[var]
                if scenarios_data_dict:
                    # push to dm  # TODO: should also alter associated
                    # disciplines' reconfig. flags for structuring ?
                    self.ee.dm.set_values_from_dict(scenarios_data_dict)

    def subprocesses_built(self, scenario_names):
        #TODO: if scenario_names is None get it?
        proxies_names = [disc.sos_name for disc in self.proxy_disciplines]
        # # assuming self.coupling_per_scenario is true so bock below commented
        # if self.coupling_per_scenario:
        #     builder_names = [b.sos_name for b in self.cls_builder]
        #     expected_proxies_names = []
        #     for sc_name in scenario_names:
        #         for builder_name in builder_names:
        #             expected_proxies_names.append(self.ee.ns_manager.compose_ns([sc_name, builder_name]))
        #     return set(expected_proxies_names) == set(proxies_names)
        # else:
        return set(proxies_names) == set(scenario_names)

    def setup_sos_disciplines(self):
        """
        Dynamic inputs and outputs of the DriverEvaluator
        """
        if self.BUILDER_MODE in self.get_data_in():
            builder_mode = self.get_sosdisc_inputs(self.BUILDER_MODE)
            disc_in = self.get_data_in()
            if builder_mode == self.MULTI_INSTANCE:
                self.build_inst_desc_io_with_scenario_df()
                if self.GENERATED_SAMPLES in disc_in:
                    generated_samples = self.get_sosdisc_inputs(self.GENERATED_SAMPLES)
                    generated_samples_dict = {self.GENERATED_SAMPLES: generated_samples}
                    # scenario_df = self.get_sosdisc_inputs(self.SCENARIO_DF)
                    # scenario_df_dict = {self.SCENARIO_DF: scenario_df}
                    # TODO: checking for sample change via object identity based on SampleGenerator impl. -> check value equality?
                    # NB also doing nothing with an empty dataframe, which means sample needs to be regenerated to renew
                    # scenario_df on 2nd config. The reason of this choice is that using an optional generated_samples
                    # gives problems with structuring variables checks leading to incomplete configuration sometimes
                    # self.logger.info('Checking for the existence of a generated sample ['+str(generated_samples.empty)+']')
                    if not generated_samples.empty and not dict_are_equal(generated_samples_dict, self.old_samples_df):
                        # and (not self.old_scenario_df or dict_are_equal(scenario_df_dict, self.old_scenario_df)):
                        self.old_samples_df = copy.deepcopy(generated_samples_dict) #TODO: overload struct. var. check to spare this deepcopy ?
                        # self.old_scenario_df = copy.deepcopy(scenario_df_dict) #TODO: overload struct. var. check to spare this deepcopy ?
                        # we crush old scenario_df and propose a df with all scenarios imposed by new sample, all de-activated
                        scenario_df = pd.DataFrame(columns=[self.SELECTED_SCENARIO, self.SCENARIO_NAME])
                        scenario_df = pd.concat([scenario_df, generated_samples], axis=1)
                        scenario_df[self.SELECTED_SCENARIO] = False
                        scenario_name = scenario_df[self.SCENARIO_NAME]
                        for i in scenario_name.index.tolist():
                            scenario_name.iloc[i] = 'scenario_'+str(i+1)
                        self.logger.info('Generated sample has changed, updating scenarios to select.')
                        self.dm.set_data(self.get_var_full_name(self.SCENARIO_DF, disc_in),
                                         'value', scenario_df, check_value=False)

            elif builder_mode == self.MONO_INSTANCE:
                # TODO: clean code below with class variables etc.
                dynamic_inputs = {'eval_inputs': {'type': 'dataframe',
                                                  'dataframe_descriptor': {'selected_input': ('bool', None, True),
                                                                           'full_name': ('string', None, False)},
                                                  'dataframe_edition_locked': False,
                                                  'structuring': True,
                                                  'visibility': self.SHARED_VISIBILITY,
                                                  'namespace': 'ns_eval'},
                                  'eval_outputs': {'type': 'dataframe',
                                                   'dataframe_descriptor': {'selected_output': ('bool', None, True),
                                                                            'full_name': ('string', None, False)},
                                                   'dataframe_edition_locked': False,
                                                   'structuring': True, 'visibility': self.SHARED_VISIBILITY,
                                                   'namespace': 'ns_eval'},
                                  'n_processes': {'type': 'int', 'numerical': True, 'default': 1},
                                  'wait_time_between_fork': {'type': 'float', 'numerical': True, 'default': 0.0}
                                  }
                dynamic_outputs = {'samples_inputs_df': {'type': 'dataframe', 'unit': None, 'visibility': self.SHARED_VISIBILITY,
                                                         'namespace': 'ns_eval'}
                                   }

                selected_inputs_has_changed = False
                if 'eval_inputs' in disc_in:
                    # if len(disc_in) != 0:

                    eval_outputs = self.get_sosdisc_inputs('eval_outputs')
                    eval_inputs = self.get_sosdisc_inputs('eval_inputs')

                    # we fetch the inputs and outputs selected by the user
                    selected_outputs = eval_outputs[eval_outputs['selected_output']
                                                    == True]['full_name']
                    selected_inputs = eval_inputs[eval_inputs['selected_input']
                                                  == True]['full_name']
                    if set(selected_inputs.tolist()) != set(self.selected_inputs):
                        selected_inputs_has_changed = True
                        self.selected_inputs = selected_inputs.tolist()
                    self.selected_outputs = selected_outputs.tolist()

                    if len(selected_inputs) > 0 and len(selected_outputs) > 0:
                        # TODO: is it OK that it crashes with empty input ? also, might want an eval without outputs ?
                        # we set the lists which will be used by the evaluation
                        # function of sosEval
                        self.set_eval_in_out_lists(
                            self.selected_inputs, self.selected_outputs)

                        # setting dynamic outputs. One output of type dict per selected
                        # output
                        for out_var in self.eval_out_list:
                            dynamic_outputs.update(
                                {f'{out_var.split(self.ee.study_name + ".", 1)[1]}_dict': {'type': 'dict',
                                                                                           'visibility': 'Shared',
                                                                                           'namespace': 'ns_doe'}})
                        dynamic_inputs.update(self._get_dynamic_inputs_doe(
                            disc_in, selected_inputs_has_changed))
                self.add_inputs(dynamic_inputs)
                self.add_outputs(dynamic_outputs)
            elif builder_mode == self.REGULAR_BUILD:
                pass  # regular build requires no specific dynamic inputs
            elif builder_mode is None:
                pass
            else:
                raise ValueError(
                    f'Wrong builder mode input in {self.sos_name}')
        # after managing the different builds inputs, we do the setup_sos_disciplines of the wrapper in case it is
        # overload, e.g. in the case of a custom driver_wrapper_cls (with DriverEvaluatorWrapper this does nothing)
        # super().setup_sos_disciplines() # TODO: manage custom driver wrapper
        # case

    def prepare_build(self):
        """
        Get the actual drivers of the subprocesses of the DriverEvaluator.
        """
        # TODO: make me work with custom driver
        # TODO: test proper cleaning when changing builder mode
        builder_list = []
        if self.BUILDER_MODE in self.get_data_in():
            builder_mode = self.get_sosdisc_inputs(self.BUILDER_MODE)
            builder_mode_has_changed = builder_mode != self.old_builder_mode
            if builder_mode_has_changed:
                self.clean_children()
                self.clean_sub_builders()
                if self.old_builder_mode == self.MONO_INSTANCE:
                    self.clean_namespaces_with_subprocess()
                    self.eval_process_builder = None
                elif self.old_builder_mode == self.MULTI_INSTANCE:
                    self.builder_tool = None
                self.old_builder_mode = copy.copy(builder_mode)
            if builder_mode == self.MULTI_INSTANCE:
                builder_list = self.prepare_multi_instance_build()
            elif builder_mode == self.MONO_INSTANCE:
                builder_list = self.prepare_mono_instance_build()
            elif builder_mode == self.REGULAR_BUILD:
                builder_list = super().prepare_build()
            elif builder_mode is None:
                pass
            else:
                raise ValueError(
                    f'Wrong builder mode input in {self.sos_name}')
        return builder_list

    def prepare_execution(self):
        """
        Preparation of the GEMSEO process, including GEMSEO objects instantiation
        """
        # prepare_execution of proxy_disciplines as in coupling
        # TODO: move to builder ?
        for disc in self.proxy_disciplines:
            disc.prepare_execution()
        # TODO : cache mgmt of children necessary ? here or in
        # SoSMDODisciplineDriver ?
        super().prepare_execution()

        self.reset_subdisciplines_of_wrapper()

    def reset_subdisciplines_of_wrapper(self):

        self.mdo_discipline_wrapp.reset_subdisciplines(self)

    def set_wrapper_attributes(self, wrapper):
        """
        set the attribute ".attributes" of wrapper which is used to provide the wrapper with information that is
        figured out at configuration time but needed at runtime. The DriverEvaluator in particular needs to provide
        its wrapper with a reference to the subprocess GEMSEO objets so they can be manipulated at runtime.
        """
        # TODO: needs to accommodate the eval attributes in the mono instance
        # case
        # io full name maps set by ProxyDiscipline
        super().set_wrapper_attributes(wrapper)

        wrapper.attributes.update({'sub_mdo_disciplines': [
                                  proxy.mdo_discipline_wrapp.mdo_discipline for proxy in self.proxy_disciplines
                                  if proxy.mdo_discipline_wrapp is not None]})  # discs and couplings but not scatters

        if self.BUILDER_MODE in self.get_data_in() and self.get_sosdisc_inputs(self.BUILDER_MODE) == self.MONO_INSTANCE:
            eval_attributes = {'eval_in_list': self.eval_in_list,
                               'eval_out_list': self.eval_out_list,
                               'reference_scenario': self.get_x0(),
                               'activated_elems_dspace_df': [[True, True]
                                                             if self.ee.dm.get_data(var, 'type') == 'array' else [True]
                                                             for var in self.eval_in_list],  # TODO: Array dimensions greater than 2??? TEST
                               'study_name': self.ee.study_name,
                               'reduced_dm': self.ee.dm.reduced_dm,  # for conversions
                               'selected_inputs': self.selected_inputs,
                               'selected_outputs': self.selected_outputs,
                               }
            wrapper.attributes.update(eval_attributes)

    def is_configured(self):
        """
        Return False if discipline is not configured or structuring variables have changed or children are not all configured
        """
        return super().is_configured() and self.subprocess_is_configured()

    def subprocess_is_configured(self):
        """
        Return True if the subprocess is configured or the builder is empty.
        """
        # Explanation:
        # 1. self._data_in == {} : if the discipline as no input key it should have and so need to be configured
        # 2. Added condition compared to SoSDiscipline(as sub_discipline or associated sub_process builder)
        # 2.1 (self.get_disciplines_to_configure() == [] and len(self.proxy_disciplines) != 0) : sub_discipline(s) exist(s) but all configured
        # 2.2 len(self.cls_builder) == 0 No yet provided builder but we however need to configure (as in 2.1 when we have sub_disciplines which no need to be configured)
        # Remark1: condition "(   and len(self.proxy_disciplines) != 0) or len(self.cls_builder) == 0" added for proc build
        # Remark2: /!\ REMOVED the len(self.proxy_disciplines) == 0 condition
        # to accommodate the DriverEvaluator that holds te build until inputs
        # are available
        return self.get_disciplines_to_configure() == []  # or len(self.cls_builder) == 0

    def prepare_multi_instance_build(self):
        """
        Call the tool to build the subprocesses in multi-instance builder mode.
        """
        self.build_tool()
        # Tool is building disciplines for the driver on behalf of the
        # driver name
        # no further disciplines needed to be builded by the evaluator
        return []

    def build_inst_desc_io_with_scenario_df(self):
        '''
        Complete inst_desc_in with scenario_df
        '''
        if self.builder_tool:
            dynamic_inputs = {self.SCENARIO_DF: {
                self.TYPE: 'dataframe',
                self.DEFAULT: pd.DataFrame(columns=[self.SELECTED_SCENARIO, self.SCENARIO_NAME]),
                self.DATAFRAME_DESCRIPTOR: {self.SELECTED_SCENARIO: ('bool', None, True),
                                            self.SCENARIO_NAME: ('string', None, True)},
                self.DATAFRAME_EDITION_LOCKED: False,
                self.EDITABLE: True,
                self.STRUCTURING: True}}  # TODO: manage variable columns for (non-very-simple) multiscenario cases

            dynamic_inputs.update({self.GENERATED_SAMPLES: {'type': 'dataframe',
                                                            'dataframe_edition_locked': True,
                                                            'structuring': True,
                                                            'unit': None,
                                                            # 'visibility': SoSWrapp.SHARED_VISIBILITY,
                                                            # 'namespace': 'ns_sampling',
                                                            'default': pd.DataFrame(), # TODO: [think] set optional ?
                                                            # self.OPTIONAL: True,
                                                            # self.USER_LEVEL: 3
                                                            }})
            self.add_inputs(dynamic_inputs)

    def configure_tool(self):
        '''
        Instantiate the tool if it does not and prepare it with data that he needs (the tool know what he needs)
        '''
        if self.builder_tool is None:
            self.builder_tool = self.builder_tool_cls.instantiate()
            self.builder_tool.associate_tool_to_driver(
                self, cls_builder=self.cls_builder, associated_namespaces=self.associated_namespaces)
        self.builder_tool.prepare_tool()

    def build_tool(self):

        self.builder_tool.build()

    # MONO INSTANCE PROCESS

    def _get_disc_shared_ns_value(self):
        # TODO: better factorization, rename?
        return self.ee.ns_manager.disc_ns_dict[self]['others_ns']['ns_eval'].get_value()

    def _set_eval_process_builder(self):
        '''
        Create the eval process builder, in a coupling if necessary, which will allow mono-instance builds.
        '''
        if len(self.cls_builder) == 0:  # added condition for proc build
            disc_builder = None
        elif len(self.cls_builder) == 1:
            # Note no distinction is made whether the builder is executable or not; old implementation used to put
            # scatter builds under a coupling automatically too. # TODO: check
            # if necessary for gather implementation.
            disc_builder = self.cls_builder[0]
        else:
            # If eval process is a list of builders then we build a coupling
            # containing the eval process
            disc_builder = self.ee.factory.create_builder_coupling(
                self.SUBCOUPLING_NAME)
            self.update_namespaces_with_subprocess()
            disc_builder.set_builder_info('cls_builder', self.cls_builder)
        self.eval_process_builder = disc_builder

    def update_namespaces_with_subprocess(self):
        '''
        Add subprocess name in namespaces used by the coupling 
        For now only ns_to_update in scatter_map are updated
        TODO : Get all namespaces specified in the cls_builder
        and associate them to the builder with new extra_ns (without shared_ns_dict association)
        '''
        pass
#         if self.map_name is not None:
#             sc_map = self.ee.smaps_manager.get_build_map(self.map_name)
#             ns_to_update = sc_map.get_ns_to_update()
#             for ns_name in ns_to_update:
#                 ns_obj = self.ee.ns_manager.get_shared_namespace(self, ns_name)
#                 updated_value = self.ee.ns_manager.update_ns_value_with_extra_ns(
#                     ns_obj.get_value(), self.SUBCOUPLING_NAME, after_name=self.sos_name)
#                 self.ee.ns_manager.add_ns(
#                     ns_name, updated_value)

    def clean_namespaces_with_subprocess(self):
        '''
        Clean subprocess name in namespaces used by the coupling 
        For now only ns_to_update in scatter_map are updated
        Not needed when the namespaces will be clearly associated (maybe a desassociation will be needed ? )
        '''
        pass
#         sc_map = self.ee.smaps_manager.get_build_map(self.map_name)
#         ns_to_update = sc_map.get_ns_to_update()
#         for ns_name in ns_to_update:
#             ns_obj = self.ee.ns_manager.get_shared_namespace(self, ns_name)
#             updated_value = ns_obj.get_value().replace(
#                 f'.{self.SUBCOUPLING_NAME}', '')
#             self.ee.ns_manager.add_ns(
#                 ns_name, updated_value)

    def prepare_mono_instance_build(self):
        '''
        Get the builder of the single subprocesses in mono-instance builder mode.
        '''
        if self.eval_process_builder is None:
            self._set_eval_process_builder()

        return [self.eval_process_builder] if self.eval_process_builder is not None else []

    def set_eval_in_out_lists(self, in_list, out_list, inside_evaluator=False):
        '''
        Set the evaluation variable list (in and out) present in the DM
        which fits with the eval_in_base_list filled in the usecase or by the user
        '''
        self.eval_in_list = [
            f'{self.ee.study_name}.{element}' for element in in_list]
        self.eval_out_list = [
            f'{self.ee.study_name}.{element}' for element in out_list]

    def set_eval_possible_values(self):
        '''
            Once all disciplines have been run through,
            set the possible values for eval_inputs and eval_outputs in the DM
        '''
        analyzed_disc = self.proxy_disciplines[0]
        possible_in_values_full, possible_out_values_full = self.fill_possible_values(
            analyzed_disc)
        possible_in_values_full, possible_out_values_full = self.find_possible_values(analyzed_disc,
                                                                                      possible_in_values_full, possible_out_values_full)

        # Take only unique values in the list
        possible_in_values = list(set(possible_in_values_full))
        possible_out_values = list(set(possible_out_values_full))

        # these sorts are just for aesthetics
        possible_in_values.sort()
        possible_out_values.sort()

        default_in_dataframe = pd.DataFrame({'selected_input': [False for _ in possible_in_values],
                                             'full_name': possible_in_values})
        default_out_dataframe = pd.DataFrame({'selected_output': [False for _ in possible_out_values],
                                              'full_name': possible_out_values})

        eval_input_new_dm = self.get_sosdisc_inputs('eval_inputs')
        eval_output_new_dm = self.get_sosdisc_inputs('eval_outputs')
        my_ns_eval_path = self._get_disc_shared_ns_value()

        if eval_input_new_dm is None:
            self.dm.set_data(f'{my_ns_eval_path}.eval_inputs',
                             'value', default_in_dataframe, check_value=False)
        # check if the eval_inputs need to be updated after a subprocess
        # configure
        elif set(eval_input_new_dm['full_name'].tolist()) != (set(default_in_dataframe['full_name'].tolist())):
            self.check_eval_io(eval_input_new_dm['full_name'].tolist(), default_in_dataframe['full_name'].tolist(),
                               is_eval_input=True)
            default_dataframe = copy.deepcopy(default_in_dataframe)
            already_set_names = eval_input_new_dm['full_name'].tolist()
            already_set_values = eval_input_new_dm['selected_input'].tolist()
            for index, name in enumerate(already_set_names):
                default_dataframe.loc[default_dataframe['full_name'] == name, 'selected_input'] = already_set_values[
                    index]
            self.dm.set_data(f'{my_ns_eval_path}.eval_inputs',
                             'value', default_dataframe, check_value=False)

        if eval_output_new_dm is None:
            self.dm.set_data(f'{my_ns_eval_path}.eval_outputs',
                             'value', default_out_dataframe, check_value=False)
        # check if the eval_inputs need to be updated after a subprocess
        # configure
        elif set(eval_output_new_dm['full_name'].tolist()) != (set(default_out_dataframe['full_name'].tolist())):
            self.check_eval_io(eval_output_new_dm['full_name'].tolist(), default_out_dataframe['full_name'].tolist(),
                               is_eval_input=False)
            default_dataframe = copy.deepcopy(default_out_dataframe)
            already_set_names = eval_output_new_dm['full_name'].tolist()
            already_set_values = eval_output_new_dm['selected_output'].tolist()
            for index, name in enumerate(already_set_names):
                default_dataframe.loc[default_dataframe['full_name'] == name, 'selected_output'] = already_set_values[
                    index]
            self.dm.set_data(f'{my_ns_eval_path}.eval_outputs',
                             'value', default_dataframe, check_value=False)

    def fill_possible_values(self, disc):
        '''
            Fill possible values lists for eval inputs and outputs
            an input variable must be a float coming from a data_in of a discipline in all the process
            and not a default variable
            an output variable must be any data from a data_out discipline
        '''
        poss_in_values_full = []
        poss_out_values_full = []
        disc_in = disc.get_data_in()
        for data_in_key in disc_in.keys():
            is_input_type = disc_in[data_in_key][self.TYPE] in self.EVAL_INPUT_TYPE
            is_structuring = disc_in[data_in_key].get(
                self.STRUCTURING, False)
            in_coupling_numerical = data_in_key in list(
                ProxyCoupling.DESC_IN.keys())
            full_id = disc.get_var_full_name(
                data_in_key, disc_in)
            is_in_type = self.dm.data_dict[self.dm.data_id_map[full_id]
                                           ]['io_type'] == 'in'
            # is_input_multiplier_type = disc_in[data_in_key][self.TYPE] in self.INPUT_MULTIPLIER_TYPE
            is_editable = disc_in[data_in_key]['editable']
            is_None = disc_in[data_in_key]['value'] is None
            if is_in_type and not in_coupling_numerical and not is_structuring and is_editable:
                # Caution ! This won't work for variables with points in name
                # as for ac_model
                # we remove the study name from the variable full  name for a
                # sake of simplicity
                if is_input_type:
                    poss_in_values_full.append(
                        full_id.split(self.ee.study_name + ".", 1)[1])
                    # poss_in_values_full.append(full_id)

                # if is_input_multiplier_type and not is_None:
                #     poss_in_values_list = self.set_multipliers_values(
                #         disc, full_id, data_in_key)
                #     for val in poss_in_values_list:
                #         poss_in_values_full.append(val)

        disc_out = disc.get_data_out()
        for data_out_key in disc_out.keys():
            # Caution ! This won't work for variables with points in name
            # as for ac_model
            in_coupling_numerical = data_out_key in list(
                ProxyCoupling.DESC_IN.keys()) or data_out_key == 'residuals_history'
            full_id = disc.get_var_full_name(
                data_out_key, disc_out)
            if not in_coupling_numerical:
                # we remove the study name from the variable full  name for a
                # sake of simplicity
                poss_out_values_full.append(
                    full_id.split(self.ee.study_name + ".", 1)[1])
                # poss_out_values_full.append(full_id)
        return poss_in_values_full, poss_out_values_full

    def find_possible_values(
            self, disc, possible_in_values, possible_out_values):
        '''
            Run through all disciplines and sublevels
            to find possible values for eval_inputs and eval_outputs
        '''
        # TODO: does this involve avoidable, recursive back and forths during
        # configuration ?
        if len(disc.proxy_disciplines) != 0:
            for sub_disc in disc.proxy_disciplines:
                sub_in_values, sub_out_values = self.fill_possible_values(
                    sub_disc)
                possible_in_values.extend(sub_in_values)
                possible_out_values.extend(sub_out_values)
                self.find_possible_values(
                    sub_disc, possible_in_values, possible_out_values)

        return possible_in_values, possible_out_values

    def get_x0(self):
        '''
        Get initial values for input values decided in the evaluation
        '''

        return dict(zip(self.eval_in_list,
                        map(self.dm.get_value, self.eval_in_list)))

    def _get_dynamic_inputs_doe(self, disc_in, selected_inputs_has_changed):
        default_custom_dataframe = pd.DataFrame(
            [[NaN for _ in range(len(self.selected_inputs))]], columns=self.selected_inputs)
        dataframe_descriptor = {}
        for i, key in enumerate(self.selected_inputs):
            cle = key
            var = tuple([self.ee.dm.get_data(
                self.eval_in_list[i], 'type'), None, True])
            dataframe_descriptor[cle] = var

        dynamic_inputs = {'samples_df': {'type': 'dataframe', self.DEFAULT: default_custom_dataframe,
                                         'dataframe_descriptor': dataframe_descriptor,
                                         'dataframe_edition_locked': False,
                                         'visibility': SoSWrapp.SHARED_VISIBILITY,
                                         'namespace': 'ns_eval'
                                         }}

        # This reflects 'samples_df' dynamic input has been configured and that
        # eval_inputs have changed
        if 'samples_df' in disc_in and selected_inputs_has_changed:

            if disc_in['samples_df']['value'] is not None:
                from_samples = list(disc_in['samples_df']['value'].keys())
                from_eval_inputs = list(default_custom_dataframe.keys())
                final_dataframe = pd.DataFrame(None, columns=self.selected_inputs)

                len_df = 1
                for element in from_eval_inputs:
                    if element in from_samples:
                        len_df = len(disc_in['samples_df']['value'])

                for element in from_eval_inputs:
                    if element in from_samples:
                        final_dataframe[element] = disc_in['samples_df']['value'][element]

                    else:
                        final_dataframe[element] = [NaN for _ in range(len_df)]

                disc_in['samples_df']['value'] = final_dataframe
            disc_in['samples_df']['dataframe_descriptor'] = dataframe_descriptor
        return dynamic_inputs

    def check_eval_io(self, given_list, default_list, is_eval_input):
        """
        Set the evaluation variable list (in and out) present in the DM
        which fits with the eval_in_base_list filled in the usecase or by the user
        """

        for given_io in given_list:
            if given_io not in default_list:
                if is_eval_input:
                    error_msg = f'The input {given_io} in eval_inputs is not among possible values. Check if it is an ' \
                                f'input of the subprocess with the correct full name (without study name at the ' \
                                f'beginning) and within allowed types (int, array, float). Dynamic inputs might  not ' \
                                f'be created. '

                else:
                    error_msg = f'The output {given_io} in eval_outputs is not among possible values. Check if it is an ' \
                                f'output of the subprocess with the correct full name (without study name at the ' \
                                f'beginning). Dynamic inputs might  not be created. '

                self.logger.warning(error_msg)

    def clean_sub_builders(self):
        '''
        Clean sub_builders as they were at initialization especially for their associated namespaces
        '''
        for builder in self.cls_builder:
            # delete all associated namespaces
            builder.delete_all_associated_namespaces()
            # set back all associated namespaces that was at the init of the
            # evaluator
            builder.add_namespace_list_in_associated_namespaces(
                self.associated_namespaces)
