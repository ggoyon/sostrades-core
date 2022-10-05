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
from sos_trades_core.execution_engine.proc_builder.build_sos_discipline_scatter import BuildSoSDisciplineScatter
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
from sos_trades_core.sos_wrapping.old_sum_value_block_discipline import OldSumValueBlockDiscipline
from sos_trades_core.tools.proc_builder.process_builder_parameter_type import ProcessBuilderParameterType

import pandas as pd
from importlib import import_module


class BuildSoSSimpleMultiScenarioException(Exception):
    pass


class BuildSoSVerySimpleMultiScenario(BuildSoSDisciplineScatter):
    ''' 
    Class that build scatter discipline and linked scatter data from scenario defined in a usecase


    1) Strucrure of Desc_in/Desc_out:
        |_ DESC_IN
            |_ SCENARIO_MAP (structuring)            
               |_ SCENARIO_MAP[INPUT_NAME] (namespace: INPUT_NS if INPUT_NS in SCENARIO_MAP keys / if not then  local, structuring, dynamic : SCENARIO_MAP[INPUT_NAME] != '' or is not None)
        |_ DESC_OUT

    2) Description of DESC parameters:
        |_ DESC_IN
           |_ SCENARIO_MAP:                     All inputs for driver builder in the form of a dictionary of four keys
                                                    INPUT_NAME:           name of the variable to scatter
                                                    INPUT_NS:             Optional key: namespace of the variable to scatter if the INPUT_NS key is this scenario map. 
                                                                          If the key is not here then it is local to the driver.
                                                    OUTPUT_NAME:          name of the variable to overwrite
                                                    SCATTER_NS:           Optional key: Internal namespace associated to the scatter discipline
                                                                          it is a temporary input: its value is put to None as soon as scenario disciplines are instantiated
                                                    GATHER_NS:            Optional key: namespace of the gather discipline associated to the scatter discipline 
                                                                          (input_ns by default). Only used if autogather = True
                                                    NS_TO_UPDATE:         list of namespaces depending on the scatter namespace 
                                                                          (by default, we have the list of namespaces of the nested sub_process)                                                                                                                                          

         |_ DESC_OUT
    '''

#################### Begin: Ontology of the discipline ###################
    # ontology information
    _ontology_data = {
        'label': 'Proc Builder Core Very Simple Multi-Scenario',
        'type': 'Research',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fas fa-stream fa-fw',
        'version': '',
    }
#################### End: Ontology of the discipline #####################
#################### Begin: Constants and parameters #####################
    # -- Disciplinary attributes

    VALUES = "value"

    SUB_PROCESS_INPUTS = 'sub_process_inputs'
    REFERENCE = 'reference'

    SCENARIO_MAP = 'scenario_map'
    INPUT_NAME = 'input_name'
    INPUT_NS = 'input_ns'
    OUTPUT_NAME = 'output_name'
    SCATTER_NS = 'scatter_ns'
    GATHER_NS = 'gather_ns'
    NS_TO_UPDATE = 'ns_to_update'

    DRIVER_NAME = 'driver_name'  # Default vs_MS
    AUTHOGATHER = 'autogather'  # Default  False
    GATHER_NODE = 'gather_node'  # Default None
    BUSINESS_POST_PROC = 'business_post_proc'  # Default False

    SCENARIO_DICT = 'scenario_dict'
    NS_BUSINESS_OUTPUTS = 'ns_business_outputs'

    NS_IN_DF = 'ns_in_df'

    default_process_builder_parameter_type = ProcessBuilderParameterType(
        None, None, 'Empty')

    default_scenario_map = {}
    default_scenario_map[INPUT_NAME] = None
    #default_scenario_map[INPUT_NS] = ''
    #default_scenario_map[OUTPUT_NAME] = ''
    #default_scenario_map[SCATTER_NS] = ''
    #default_scenario_map[GATHER_NS] = ''
    default_scenario_map[NS_TO_UPDATE] = []

    default_full_scenario_map = {}
    default_full_scenario_map[INPUT_NAME] = None
    default_full_scenario_map[INPUT_NS] = ''
    default_full_scenario_map[OUTPUT_NAME] = ''
    default_full_scenario_map[SCATTER_NS] = ''
    default_full_scenario_map[GATHER_NS] = ''
    default_full_scenario_map[NS_TO_UPDATE] = []

    DESC_IN = {
        SUB_PROCESS_INPUTS: {'type': SoSDiscipline.PROC_BUILDER_MODAL,
                             'structuring': True,
                             'default': default_process_builder_parameter_type.to_data_manager_dict(),
                             'user_level': 1,
                             'optional': False
                             },
        SCENARIO_MAP: {'type': 'dict',
                       'structuring': True,
                       'default': default_scenario_map,
                       'user_level': 1,
                       'optional': False
                       }
    }

    #DESC_OUT = {}

#################### End: Constants and parameters #######################

#################### Begin: Main methods ################################

    def __init__(self, sos_name, ee, map_name, cls_builder, autogather, gather_node, business_post_proc, associated_namespaces=[]):
        '''
        Constructor
        '''
        self.__autogather = autogather
        self.__gather_node = gather_node
        self.__build_business_io = business_post_proc
        self.__cls_builder = cls_builder
        BuildSoSDisciplineScatter.__init__(
            self, sos_name, ee, map_name, cls_builder, associated_namespaces=associated_namespaces)
        self._maturity = ''

        self.previous_sub_process_repo = None
        self.previous_sub_process_name = None

        self.previous_sc_map_dict = None
        self.previous_sc_map_name = None

        self.previous_sub_process_usecase_name = 'Empty'
        self.previous_sub_process_usecase_data = {}
        self.dyn_var_sp_from_import_dict = {}

        self.previous_algo_name = ""

        self.sub_process_ns_in_build = None
        self.ns_of_sub_proc = []

        # Possible values: 'Empty_SP', 'Create_SP', 'Replace_SP',
        # 'Unchanged_SP'
        self.sub_proc_build_status = 'Empty_SP'

        # Possible values: 'Empty', 'Create', 'Replace', 'Unchanged'
        self.sc_map_build_status = 'Empty'

        # Possible values: 'No_SP_UC_Import', 'SP_UC_Import'
        self.sub_proc_import_usecase_status = 'No_SP_UC_Import'

    def set_cls_builder(self, value):
        self.__cls_builder = value
        # update also in mother class as attribute has other name
        # self.__builders
        self.set_builders(value)

    def get_cls_builder(self):
        return self.__cls_builder

    def get_autogather(self):
        return self.__autogather

    def get_gather_node(self):
        return self.__gather_node

    def get_build_business_io(self):
        return self.__build_business_io

    def build(self):
        '''
            Overloaded scatter discipline method to "configure" scenarios
            Get and build builder from sub_process of driver
            Added to provide proc builder capability
            Reached from __configure_io in ee.py: self.factory.build() is going from build to build starting from root
            It comes before configuring()
        '''
        self.coupling_per_scatter = True
        # Remark: cls_builder and sc_map added in __init__ of sos_discipline_scatter
        # Remark: Nested build is done by creating (in SoSDisciplineScatter.build/build_sub_coupling)
        # as many coupling as names in the provided list of the data key
        # SCENARIO_MAP['input_name']

        # 1. Create and add scenario_map
        if self.SCENARIO_MAP in self._data_in:
            sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
            sc_map_name = sc_map_dict[self.INPUT_NAME]

            sc_map_ready = False
            sc_map_ns = None
            if self.INPUT_NS in sc_map_dict.keys():  # use of shared ns
                sc_map_ns = sc_map_dict[self.INPUT_NS]
                sc_map_ready = sc_map_name is not None and sc_map_name != '' and sc_map_ns is not None and sc_map_ns != ''
            else:  # use of local ns
                sc_map_ready = sc_map_name is not None and sc_map_name != ''

            if sc_map_ready:  # a filled sc_map is available
                # either Unchanged or Create or Replace
                # 1. set_sc_map_status
                self.set_sc_map_status(sc_map_dict)
                # 2. create and add map
                # in case of replace then we should update/clean the previous existing sc_map.
                # In case of cleaning, can this map be used by other and need
                # not to be clean ?
                if (self.sc_map_build_status == 'Create' or self.sc_map_build_status == 'Replace'):
                    # 1. Take care of the Replace (if any)
                    if (self.sc_map_build_status == 'Replace'):
                        self.clean_all_for_rebuild()
                    # 2. Do the create
                    # 2.1 add sc_map
                    self.ee.smaps_manager.add_build_map(
                        sc_map_name, sc_map_dict)
                    # 2.2 adapt the sc_map_ns namespace (if any)
                    if sc_map_ns is not None:
                        current_ns = self.ee.ns_manager.current_disc_ns
                        self.ee.ns_manager.add_ns(sc_map_ns, current_ns)
                        self.ee.ns_manager.update_others_ns_with_shared_ns(
                            self, sc_map_ns)

        # 2. Create and add cls_builder
        self.build_for_proc_builder()
        # 3. Associate map to discipline and build (automatically done when
        # structuring variable has changed and OK here even if empty proc or
        # map)
        if self.SCENARIO_MAP in self._data_in:
            sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
            sc_map_name = sc_map_dict[self.INPUT_NAME]
            # print(sc_map_dict[self.INPUT_NAME])
            # print(sc_map_dict[self.NS_TO_UPDATE])
            cls_builder = self.get_cls_builder()
            BuildSoSDisciplineScatter._associate_map_to_discipline(self,
                                                                   self.ee, sc_map_name, cls_builder)
            BuildSoSDisciplineScatter.build(self)
            # Dynamically add INST_DESC_IN and  INST_DESC_OUT if autogather is
            # True
            # self.build_business_io()  # should be put in
            # setup_sos_disciplines !

    def configure(self):
        """
            Overloaded SoSDiscipline method
            Configuration of the Build driver
            Reached from __configure_io in ee.py: self.root_process.configure_io() is going from confiure to configure starting from root
            It comes after build()
        """

        # if self._data_in == {} or len(self.get_cls_builder()) == 0:

        BuildSoSDisciplineScatter.configure(self)

        # Treatment of dynamic subprocess inputs in case of change of usecase
        # of subprocess (Added to provide proc builder capability)
        self.configure_for_proc_builder()

    def is_configured(self):
        '''
        Function to modify is_configured if an added condition is needed
        '''
        return SoSDiscipline.is_configured(self)

    def setup_sos_disciplines(self):
        """
           Overload setup_sos_disciplines to create a dynamic desc_in
           Reached from configure() of sos_discipline [SoSDiscipline.configure in config() of scatter discipline].
        """

        # Update of non dynamic desc_in
        # Update default_scenario_map with list of subprocess ns and provide it
        # to  sc_map_name dynamic input if self.SCENARIO_MAP is still with
        # default values

        # 0. Update list of all ns of subprocess in default ns_to_update
        if (self.SCENARIO_MAP in self._data_in and self.sc_map is None):
            sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
            sc_map_name = sc_map_dict[self.INPUT_NAME]
            if sc_map_name == '' or sc_map_name is None:  # provide only if user has not provided already a scenario_map
                # Define a new scenario map with ns_to_update that is with all
                # sub_process namespaces
                new_scenario_map = {}
                new_scenario_map[self.INPUT_NAME] = None
                #new_scenario_map[INPUT_NS] = ''
                #new_scenario_map[OUTPUT_NAME] = ''
                #new_scenario_map[SCATTER_NS] = ''
                #new_scenario_map[GATHER_NS] = ''
                new_scenario_map[self.NS_TO_UPDATE] = self.ns_of_sub_proc
                driver_name = self.name
                self.dm.set_data(f'{self.ee.study_name}.{driver_name}.{self.SCENARIO_MAP}',
                                 'value', new_scenario_map, check_value=False)

        if self.sc_map is not None:
            dynamic_inputs = {}
            dynamic_outputs = {}
            # 1. provide driver inputs based on selected scenario map
            dynamic_inputs = self.setup_sos_disciplines_driver_inputs_depend_on_sub_process(
                dynamic_inputs)
            self.add_inputs(dynamic_inputs)
            self.add_outputs(dynamic_outputs)

        if self.sc_map is not None and self.sub_proc_build_status != 'Empty_SP' and self.sc_map.get_input_name() in self._data_in:
            # 0. Dynamically add INST_DESC_IN and  INST_DESC_OUT if autogather is True
            # self.build_business_io()  # should be put in setup_sos_disciplines !
            # 1. import data from selected sub_process_usecase
            self.manage_import_inputs_from_sub_process()

    def run(self):
        '''
            Overloaded scatter discipline method
            Store business outputs in dictionaries if autogather is True
        '''
        if self.get_autogather():
            self.run_autogather()


#################### End: Main methods ################################


##################### Begin: Sub methods ################################
# Remark: those sub methods should be private functions

    def run_autogather(self):
        '''
           Store business outputs in dictionaries if autogather is True
        '''
        new_values_dict = {}
        for long_key in self._data_in.keys():
            for key in self._data_out.keys():
                if long_key.endswith(key.split('_dict')[0]):
                    if key in new_values_dict:
                        new_values_dict[key].update({long_key.rsplit('.', 1)[0]: self.ee.dm.get_value(
                            self.get_var_full_name(long_key, self._data_in))})
                    else:
                        new_values_dict[key] = {long_key.rsplit('.', 1)[0]: self.ee.dm.get_value(
                            self.get_var_full_name(long_key, self._data_in))}
        self.store_sos_outputs_values(new_values_dict)

    def build_business_io(self):
        '''
           Add SumValueBlockDiscipline ouputs in INST_DESC_IN and dict in INST_DESC_OUT if autogather is True
           Function needed in build(self)
        '''
        if self.get_build_business_io():

            if self.get_gather_node() is None:
                ns_value = f'{self.ee.study_name}.Business'
            else:
                ns_value = f'{self.ee.study_name}.{self.get_gather_node()}.Business'

            if self.NS_BUSINESS_OUTPUTS not in self.ee.ns_manager.shared_ns_dict:
                self.ee.ns_manager.add_ns(self.NS_BUSINESS_OUTPUTS, ns_value)
                self.ee.ns_manager.disc_ns_dict[self]['others_ns'].update(
                    {self.NS_BUSINESS_OUTPUTS: self.ee.ns_manager.shared_ns_dict[self.NS_BUSINESS_OUTPUTS]})

            ns_to_cut = self.ee.ns_manager.get_shared_namespace_value(
                self, self.sc_map.get_gather_ns()) + self.ee.ns_manager.NS_SEP

            # In the case of old sum value block disciplines (used in business cases processes)
            # we needed a gather_data for multi scenario post processing
            # This capability will be replaced by generic gather data created
            # from archi builder when all business processes will be refactored
            for disc in self.ee.factory.sos_disciplines:
                if isinstance(disc, OldSumValueBlockDiscipline):
                    for key in disc._data_out.keys():
                        full_key = disc.get_var_full_name(key, disc._data_out)
                        end_key = full_key.split(ns_to_cut)[-1]
                        if end_key not in self._data_in:
                            self.inst_desc_in.update({end_key: {SoSDiscipline.TYPE: disc._data_out[key][SoSDiscipline.TYPE],
                                                                SoSDiscipline.VISIBILITY: SoSDiscipline.SHARED_VISIBILITY, SoSDiscipline.NAMESPACE: self.sc_map.get_gather_ns(), SoSDiscipline.USER_LEVEL: 3}})
                        if f'{key}_dict' not in self._data_out:
                            self.inst_desc_out.update({f'{key}_dict': {SoSDiscipline.TYPE: 'dict',
                                                                       SoSDiscipline.VISIBILITY: SoSDiscipline.SHARED_VISIBILITY, SoSDiscipline.NAMESPACE: self.NS_BUSINESS_OUTPUTS, SoSDiscipline.USER_LEVEL: 2}})

            # modify SCENARIO_DICT input namespace to store it in
            # NS_BUSINESS_OUTPUTS node
            if self.SCENARIO_DICT in self._data_in and self.ee.dm.get_data(self.get_var_full_name(self.SCENARIO_DICT, self._data_in), SoSDiscipline.NAMESPACE) != self.NS_BUSINESS_OUTPUTS:
                full_key = self.get_var_full_name(
                    self.SCENARIO_DICT, self._data_in)
                self.ee.dm.set_data(
                    full_key, self.NAMESPACE, self.NS_BUSINESS_OUTPUTS)
                self.ee.dm.set_data(
                    full_key, self.NS_REFERENCE, self.get_ns_reference(SoSDiscipline.SHARED_VISIBILITY, self.NS_BUSINESS_OUTPUTS))
                self.ee.dm.generate_data_id_map()
##################### End: Sub methods ################################

##################### Begin: Sub methods  added for proc builder #########

    def set_sc_map_status(self, sc_map_dict):
        '''
            State sc_map CRUD status
            The sc_map is defined by its dictionary
            Function needed in build(self)
        '''
        # We come from outside driver process
        if sc_map_dict != self.previous_sc_map_dict:
            if self.previous_sc_map_dict is None:
                self.previous_sc_map_name = None
            else:
                self.previous_sc_map_name = self.previous_sc_map_dict[self.INPUT_NAME]
            self.previous_sc_map_dict = sc_map_dict
            # driver process with provided sc_map
            if self.previous_sc_map_name == '' or self.previous_sc_map_name is None:
                self.sc_map_build_status = 'Create'
            else:
                self.sc_map_build_status = 'Replace'
        else:
            self.sc_map_build_status = 'Unchanged'
        return self.sc_map_build_status

    def build_driver_subproc(self, sub_process_repo, sub_process_name):
        '''
            Get and build builder from sub_process of the driver
            The subprocess is defined by its name and repository
            Function needed in build(self)
        '''
        # 1. Clean if needed
        if (self.sub_proc_build_status == 'Replace_SP' and self.sc_map_build_status == 'Unchanged'):
            # clean the previous created map
            self.clean_driver_before_rebuilt()
        # 2. Get and set the builder of subprocess
        cls_builder = self.get_nested_builders_from_sub_process(
            sub_process_repo, sub_process_name)
        if not isinstance(cls_builder, list):
            cls_builder = [cls_builder]
        self.set_nested_builders(cls_builder)
        # 3. Capture the input namespace specified at
        # building step

        ns_of_driver = self.get_ns_of_driver()
        ns_of_sub_proc_dict = self.capture_ns_of_sub_proc_and_associated_values(
            ns_of_driver)

        # print('\n')
        # print('ns_of_sub_pro_pd:')
        # print(self.sub_process_ns_in_build)
        # print('\n')

        # 4. Treat ns
        #   4.1 Shift ns with driver name
        self.put_ns_of_subproc_in_driver_context(ns_of_driver, assign_ns=True)

    def clean_all_for_rebuild(self):
        """
            Create_nested builders from their nested process.
            Function needed in build (if import map first) and build_driver_subproc
        """
        # 1 remove all previously instantiated disciplines
        sub_names = []
        BuildSoSDisciplineScatter.clean_scattered_disciplines(self, sub_names)
        # 2 remove previous created scatter map of name
        # "self.previous_sc_map_name"

        # Remark: self.previous_sc_map_name is also self.sc_map.get_input_name()
        # print(self.previous_sc_map_name)
        # print(self.sc_map.get_input_name())
        #sc_map_name_to_remove = self.previous_sc_map_name
        # Why self.previous_sc_map_name is not with the good value (None
        # instead of scenario_list) in Test 05/Step 02 ?

        sc_map_name_to_remove = self.sc_map.get_input_name()
        self.ee.smaps_manager.remove_build_map(sc_map_name_to_remove)
        # 3 We "clean" also all dynamic inputs
        self.add_inputs({})

    def update_namespace_list_with_extra_ns_except_driver(self, extra_ns, driver_ns_list, after_name=None, namespace_list=None):
        '''
            Update the value of a list of namespaces with an extra namespace placed behind after_name
            In our context, we do not want to shift ns of driver_ns_list  already created before nested sub_process
            Function needed in build_driver_subproc(self)
        '''
        if namespace_list is None:
            #namespace_list = self.ee.ns_manager.ns_list
            namespace_list = list(self.ee.ns_manager.shared_ns_dict.values())
            namespace_list = [
                elem for elem in namespace_list if elem.__dict__['name'] not in driver_ns_list]

        self.ee.ns_manager.update_namespace_list_with_extra_ns(
            extra_ns, after_name, namespace_list=namespace_list)

    def manage_import_inputs_from_sub_process(self):
        """
            Function needed in setup_sos_disciplines()
        """
        # Set sub_proc_import_usecase_status
        self.set_sub_process_usecase_status_from_user_inputs()

        # Treat the case of SP_UC_Import
        if self.sub_proc_import_usecase_status == 'SP_UC_Import':
            # 1. Add 'reference' (if not already existing) in data manager for
            # usecase import
            self.add_reference_instance()
            # 2. Add data in data manager for this analysis'reference'
            # 2.1 get anonymized dict
            anonymize_input_dict_from_usecase = self.get_sosdisc_inputs(
                self.SUB_PROCESS_INPUTS)[ProcessBuilderParameterType.USECASE_DATA]
            # 2.2 put anonymized dict in context (unanonymize)
            input_dict_from_usecase = self.put_anonymized_input_dict_in_sub_process_context(
                anonymize_input_dict_from_usecase)
            # print(input_dict_from_usecase)
            # self.ee.display_treeview_nodes(True)
            # 2.3 load data put in context in the dm (if possible) or put them
            # in self.dyn_var_sp_from_import_dict
            self.load_data_in_dm(input_dict_from_usecase, sub_dynamic_mod=True)
            # 2.4 Update parameters
            # Set the status to No_SP_UC_Import'
            self.sub_proc_import_usecase_status = 'No_SP_UC_Import'
            # Empty the anonymized dict in
            sub_process_inputs_dict = self.get_sosdisc_inputs(
                self.SUB_PROCESS_INPUTS)
            sub_process_inputs_dict[ProcessBuilderParameterType.USECASE_DATA] = {
            }
            self.dm.set_data(f'{self.get_disc_full_name()}.{self.SUB_PROCESS_INPUTS}',
                             self.VALUES, sub_process_inputs_dict, check_value=False)
            # Empty the previous_sub_process_usecase_data
            self.previous_sub_process_usecase_data = {}
        # there are still dynamic variables put apart
        elif len(self.dyn_var_sp_from_import_dict) != 0:
            # self.ee.display_treeview_nodes(True)
            self.ee.dm.set_values_from_dict(self.dyn_var_sp_from_import_dict)
            # Is it also OK in case of a dynamic param of dynamic param ?
            self.dyn_var_sp_from_import_dict = {}

    def add_reference_instance(self):
        """
            Add a 'reference' discipline instance (if not already existing) in data manager to
            allow to load data from usecase
            Function needed in manage_import_inputs_from_sub_process()
            Function to be specified per driver
        """
        sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
        map_name = sc_map_dict[self.INPUT_NAME]

        current_scenario_list = self.get_sosdisc_inputs(map_name)
        new_scenario_list = current_scenario_list
        if 'reference' not in current_scenario_list:
            new_scenario_list = new_scenario_list + ['reference']
            driver_name = self.name
            self.dm.set_data(f'{self.ee.study_name}.{driver_name}.{map_name}',
                             'value', new_scenario_list, check_value=False)

    def put_anonymized_input_dict_in_sub_process_context(self, anonymize_input_dict_from_usecase):
        """
            Put_anonymized_input_dict in sub_process context
            Function needed in manage_import_inputs_from_sub_process()
        """
        # Get unanonymized dict (i.e. dict of subprocess in driver context)
        # from anonymized dict and context
        # good prefix in context
        driver_name = self.name
        new_study_placeholder = f'{self.ee.study_name}.{driver_name}.{self.REFERENCE}'
        new_study_placeholder = self.ref_discipline_full_name

        # Following treatment of substitution of new_study_placeholder is
        # OK if variable is a variable with ns  in ns_to_update
        # If not then we should shift only of f'{self.ee.study_name}.{driver_name}
        # For this we need to find ns of each variable
        # self.ee.ns_manager.get_shared_namespace(disc, var_ns)

        input_dict_from_usecase = {}
        for key_to_unanonymize, value in anonymize_input_dict_from_usecase.items():
            converted_key = key_to_unanonymize.replace(
                self.ee.STUDY_PLACEHOLDER_WITHOUT_DOT, new_study_placeholder)  # see def __unanonymize_key  in execution_engine
            uc_d = {converted_key: value}
            input_dict_from_usecase.update(uc_d)
        return input_dict_from_usecase


##################### End: Sub methods for build #########################

######### Begin: Sub methods for build to be specified in build #####

    def set_ref_discipline_full_name(self):
        '''
            Specific function of the driver to define the full name of the reference disvcipline
            Function needed in _init_ of the driver
            Function to be specified per driver
        '''
        driver_name = self.name
        self.ref_discipline_full_name = f'{self.ee.study_name}.{driver_name}.{self.REFERENCE}'

        return

    def clean_driver_before_rebuilt(self):  # come from build_driver_subproc
        '''
            Specific function of the driver to clean all instances before rebuild and reset any needed parameter
            Function needed in build_driver_subproc(self, sub_process_repo, sub_process_name)
            Function to be specified per driver
        '''
        self.clean_all_for_rebuild()
        # Also reset input mat in desc_in to default map
        driver_name = self.name
        self.dm.set_data(f'{self.ee.study_name}.{driver_name}.{self.SCENARIO_MAP}',
                         'value', self.default_scenario_map, check_value=False)
        self.previous_sc_map_dict = None
        self.previous_sc_map_name = None
        # We "clean" also all dynamic inputs to be reloaded by
        # the usecase
        self.add_inputs({})  # is it needed ?
        return

    # come from build_driver_subproc# come from build_driver_subproc
    def get_ns_of_driver(self):
        '''
            Specific function of the driver to get ns of driver
            Function needed in build_driver_subproc(self, sub_process_repo, sub_process_name)
            Function to be specified per driver
        '''
        sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
        if self.INPUT_NS in sc_map_dict.keys():
            ns_of_driver = self.get_ns_of_driver()
            sc_map_ns = sc_map_dict[self.INPUT_NS]
            if sc_map_ns is not None:
                ns_of_driver = [sc_map_ns]
            else:
                ns_of_driver = []
        else:
            ns_of_driver = []
        return ns_of_driver

    def setup_desc_in_dict_of_driver(self):
        """
            Create desc_in_dict for dynamic inputs of the driver depending on sub process
            Function needed in setup_sos_disciplines_driver_inputs_depend_on_sub_process()
            Function to be specified per driver
            Update of SCENARIO_MAP['input_name']
        """
        # Remark: the dynamic input SCENARIO_MAP['input_name'] is initially set in
        # build_inst_desc_in_with_map() in __init__ of sos_discipline_scatter
        desc_in_dict = BuildSoSDisciplineScatter.build_inst_desc_in_with_map(
            self)

        # Set status
        self.sub_proc_build_status = 'Unchanged_SP'
        self.sc_map_build_status = 'Unchanged_SP'
        return desc_in_dict

    def setup_sos_disciplines_driver_inputs_independent_on_sub_process(self, dynamic_inputs, dynamic_outputs):
        """
            setup_dynamic inputs when driver parameters depending on the SP selection are already set
            No dynamic specific parameters to be updated in vsMS
            Function needed in setup_sos_disciplines()
            Function to be specified per driver
        """
        return dynamic_inputs, dynamic_outputs

    def add_reference_instance(self):
        """
            Add a 'reference' discipline instance (if not already existing) in data manager to
            allow to load data from usecase
            Function needed in manage_import_inputs_from_sub_process()
            Function to be specified per driver
        """
        sc_map_dict = self.get_sosdisc_inputs(self.SCENARIO_MAP)
        map_name = sc_map_dict[self.INPUT_NAME]

        current_scenario_list = self.get_sosdisc_inputs(map_name)
        new_scenario_list = current_scenario_list
        if 'reference' not in current_scenario_list:
            new_scenario_list = new_scenario_list + ['reference']
            driver_name = self.name
            self.dm.set_data(f'{self.ee.study_name}.{driver_name}.{map_name}',
                             'value', new_scenario_list, check_value=False)

#####  End: Sub methods for build to be specified in build #####
