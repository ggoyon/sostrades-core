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
# GEMSEO memory overloads.
# (will be merged to GEMSEO in next releases)
from .gemseo_overloads.dependency_graph import DependencyGraph  # pylint: disable=unused-import
from .gemseo_overloads.mda import MDA  # pylint: disable=unused-import
from .gemseo_overloads.mda_factory import MDAFactory  # pylint: disable=unused-import
from .gemseo_overloads.mdo_chain import MDOChain  # pylint: disable=unused-import
from .gemseo_overloads.sequential_mda import GSNewtonMDA  # pylint: disable=unused-import
from .gemseo_overloads.newton import MDANewtonRaphson  # pylint: disable=unused-import
from .gemseo_overloads.data_conversion import DataConversion  # pylint: disable=unused-import
from .gemseo_overloads.discipline import MDODiscipline  # pylint: disable=unused-import

# Execution engine SoSTrades code
from pprint import PrettyPrinter

from sos_trades_core.api import get_sos_logger
from sos_trades_core.execution_engine.data_manager import DataManager
from sos_trades_core.execution_engine.sos_factory import SosFactory
from sos_trades_core.execution_engine.ns_manager import NamespaceManager
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
from sos_trades_core.execution_engine.scattermaps_manager import ScatterMapsManager
from sos_trades_core.execution_engine.post_processing_manager import PostProcessingManager
from sos_trades_core.execution_engine.sos_coupling import SoSCoupling
from sos_trades_core.execution_engine.data_connector.data_connector_factory import ConnectorFactory
from sos_trades_core.tests import data

DEFAULT_FACTORY_NAME = 'default_factory'
DEFAULT_NS_MANAGER_NAME = 'default_ns_namanger'
DEFAULT_SMAPS_MANAGER_NAME = 'default_smap_namanger'


class ExecutionEngineException (Exception):
    pass


class ExecutionEngine:
    """
    SoSTrades execution engine
    """
    STUDY_AND_ROOT_PLACEHODER = '<study_and_root_ph>'
    STUDY_PLACEHOLDER_WITH_DOT = '<study_ph>.'
    STUDY_PLACEHOLDER_WITHOUT_DOT = '<study_ph>'

    def __init__(self, study_name,
                 rw_object=None,
                 root_dir=None,
                 study_filename=None,
                 yield_method=None,
                 logger=None):

        self.study_name = study_name
        self.study_filename = study_filename or study_name
        self.__yield_method = yield_method

        if logger is None:
            self.logger = get_sos_logger('SoS.EE')
        else:
            self.logger = logger

        self.__post_processing_manager = PostProcessingManager(self)

        self.ns_manager = NamespaceManager(
            name=DEFAULT_NS_MANAGER_NAME, ee=self)
        self.dm = DataManager(name=self.study_name,
                              root_dir=root_dir,
                              rw_object=rw_object,
                              study_filename=self.study_filename,
                              ns_manager=self.ns_manager,
                              logger=get_sos_logger(f'{self.logger.name}.DataManager'))
        self.smaps_manager = ScatterMapsManager(
            name=DEFAULT_SMAPS_MANAGER_NAME, ee=self)
        self.__factory = SosFactory(
            self, self.study_name)

        self.root_process = None
        self.root_builder_ist = None

    @property
    def factory(self):
        """ Read-only accessor to the factory object

            :return: current used factory 
            :type: SosFactory
        """
        return self.__factory

    @property
    def post_processing_manager(self):
        """ Read-only accessor to the post_processing_manager object

            :return: current used post_processing_manager 
            :type: PostProcessingManager
        """
        return self.__post_processing_manager

    #-- Public methods
    def select_root_process(self, repo, mod_id):

        # Method usage now ?
        # dead code in comment
        # usage regarding 'select_root_builder_ist'
        # usage only in testing not at runtime
        self.logger.warn(
            'DEPRECATION WARNING (07/2021).\n"select_root_process" methods is flagged to be checked regarding "select_root_builder_ist" method and usage in code (only in testing behaviour)')
        #         builder_list = self.factory.get_builder_from_process(repo=repo,
        #                                                              mod_id=mod_id)
        #         self.factory.set_builders_to_coupling_builder(builder_list)
        #
        #         self.load_study_from_input_dict({})

        # Set main process information to factory
        self.factory.repository = repo
        self.factory.process_identifier = mod_id

        self.select_root_builder_ist(repo, mod_id)
        self.attach_builders_to_root()

    def select_root_builder_ist(self, repo, mod_id):
        self.factory.repository = repo
        self.factory.process_identifier = mod_id
        self.root_builder_ist = self.factory.get_pb_ist_from_process(
            repo, mod_id)

    def attach_builders_to_root(self):
        builder_list_func = getattr(
            self.root_builder_ist, self.factory.BUILDERS_FUNCTION_NAME)
        builder_list = builder_list_func()

        self.factory.set_builders_to_coupling_builder(builder_list)
        #-- We are changing what happend in root, need to reset dm
        self.dm.reset()
        self.load_study_from_input_dict({})

    def set_root_process(self, process_instance):
        # self.dm.reset()s

        if isinstance(process_instance, SoSDiscipline):
            self.root_process = process_instance
        else:
            raise ExecutionEngineException(
                f'Execution engine root process is intended to be an instance or inherited instance of SoSDiscipline class and not {type(process_instance)}.')

    def configure(self):
        self.logger.info('configuring ...')
        self.factory.build()
        self.root_process.configure()

        # create DM treenode to be able to populate it from GUI
        self.dm.treeview = None

    def fill_data_in_with_connector(self):
        """
        Use data connector if needed, in the following case
        1) data is in input, and come from the output of another model --> no data connector used
        2) data is in output --> no data connector use here
        3) data is in input, and does not come from another model --> data connector is used
        """

        dm_data_dict = self.dm.data_dict
        for variable_id in dm_data_dict:
            data = None
            # if connector is needed
            if SoSDiscipline.CONNECTOR_DATA in dm_data_dict[variable_id]:
                if dm_data_dict[variable_id][SoSDiscipline.CONNECTOR_DATA] is not None:
                    if dm_data_dict[variable_id][SoSDiscipline.IO_TYPE] == SoSDiscipline.IO_TYPE_IN:
                        # if variable io_type is in --> use data_connector
                        data = ConnectorFactory.use_data_connector(
                            dm_data_dict[variable_id][SoSDiscipline.CONNECTOR_DATA],
                            self.logger)
                    # else, variable is an output of a disc --> no use of data
                    # connector

            if data is not None:  # update variable value
                dm_data_dict[variable_id][SoSDiscipline.VALUE] = data

    def __configure_io(self):
        self.logger.info('configuring ...')

        self.factory.build()
        self.root_process.configure_io()

    def __configure_execution(self):
        self.root_process.configure_execution()

        # create DM treenode to be able to populate it from GUI
        self.dm.treeview = None

    def update_from_dm(self):
        self.root_process.update_from_dm()

    def update_status_configure(self):
        '''
        Update status configure of all disciplines in factory
        '''
        for disc in self.factory.sos_disciplines:
            disc._update_status_dm(SoSDiscipline.STATUS_CONFIGURE)

    def get_treeview(self, no_data=False, read_only=False):
        ''' returns the treenode build based on datamanager '''
        if self.dm.treeview is None:
            self.dm.create_treeview(
                self.root_process, self.__factory.process_module, no_data, read_only)
        return self.dm.treeview

    def display_treeview_nodes(self, display_variables=None):
        '''
        Display the treeview and create it if not 
        '''
        self.get_treeview()
        tv_to_display = self.dm.treeview.display_nodes(
            display_variables=display_variables)
        self.logger.info(tv_to_display)
        return tv_to_display

    def __anonymize_key(self, key_to_anonymize):
        base_namespace = f'{self.study_name}.{self.root_process.sos_name}'
        converted_key = key_to_anonymize

        if key_to_anonymize == base_namespace:
            converted_key = key_to_anonymize.replace(
                base_namespace, ExecutionEngine.STUDY_AND_ROOT_PLACEHODER, 1)

        elif key_to_anonymize.startswith(f'{base_namespace}.'):
            converted_key = key_to_anonymize.replace(
                f'{base_namespace}.', f'{ExecutionEngine.STUDY_AND_ROOT_PLACEHODER}.', 1)

        elif key_to_anonymize.startswith(f'{self.study_name}.'):
            converted_key = key_to_anonymize.replace(
                f'{self.study_name}.', ExecutionEngine.STUDY_PLACEHOLDER_WITH_DOT, 1)
        elif key_to_anonymize.startswith(f'{self.study_name}'):
            converted_key = key_to_anonymize.replace(
                f'{self.study_name}', ExecutionEngine.STUDY_PLACEHOLDER_WITHOUT_DOT, 1)

        return converted_key

    def __unanonimize_key(self, key_to_unanonimize):
        base_namespace = f'{self.study_name}.{self.root_process.sos_name}'
        converted_key = key_to_unanonimize

        if key_to_unanonimize == ExecutionEngine.STUDY_AND_ROOT_PLACEHODER:
            converted_key = key_to_unanonimize.replace(
                ExecutionEngine.STUDY_AND_ROOT_PLACEHODER, base_namespace)

        elif key_to_unanonimize.startswith(f'{ExecutionEngine.STUDY_AND_ROOT_PLACEHODER}.'):
            converted_key = key_to_unanonimize.replace(
                f'{ExecutionEngine.STUDY_AND_ROOT_PLACEHODER}.', f'{base_namespace}.')

        elif key_to_unanonimize.startswith(ExecutionEngine.STUDY_PLACEHOLDER_WITH_DOT):
            converted_key = key_to_unanonimize.replace(
                ExecutionEngine.STUDY_PLACEHOLDER_WITH_DOT, f'{self.study_name}.')
        elif key_to_unanonimize.startswith(ExecutionEngine.STUDY_PLACEHOLDER_WITHOUT_DOT):
            converted_key = key_to_unanonimize.replace(
                ExecutionEngine.STUDY_PLACEHOLDER_WITHOUT_DOT, f'{self.study_name}')

        return converted_key

    def export_data_dict_and_zip(self, export_dir):
        '''
        serialise data dict of the study keeping namespaced variables
        and generate csv files of whole data
        '''
        self.logger.info('exporting data from study to externalised files...')
        self.logger.debug('dumping study before...')
        return self.dm.export_data_dict_and_zip(export_dir)

    def load_disciplines_status_dict(self, disciplines_status_dict):
        '''
        Read disciplines status dict given as argument and then update the execution engine
        accordingly

        :params: disciplines_status_dict, dictionary {disciplines_key: status}
        '''
        for discipline_key in self.dm.disciplines_dict:

            dm_discipline = self.dm.disciplines_dict[discipline_key][DataManager.DISC_REF]

            # Get basic discipline key
            target_key = dm_discipline.get_disc_full_name()

            # Check if basic key is available in the discipline status
            # dictionary to load
            if target_key not in disciplines_status_dict:
                # If not convert basic key to an anonimized key
                target_key = self.__anonymize_key(target_key)

            if target_key in disciplines_status_dict:
                status_to_load = disciplines_status_dict[target_key]

                if isinstance(status_to_load, dict):
                    if self.dm.disciplines_dict[discipline_key]['classname'] in status_to_load:
                        status = status_to_load[self.dm.disciplines_dict[discipline_key]['classname']]
                        self.dm.disciplines_dict[discipline_key]['status'] = status
                        dm_discipline.status = status
                else:
                    self.dm.disciplines_dict[discipline_key]['status'] = status_to_load
                    dm_discipline.status = status_to_load

    def get_anonimated_disciplines_status_dict(self):
        '''
        Return the execution engine discipline status dictionary but with anonimize key

        :returns: dictionary {anonimize_disciplines_key: status}
        '''
        converted_dict = {}
        dict_to_convert = self.dm.build_disc_status_dict()

        for discipline_key in dict_to_convert.keys():

            converted_key = self.__anonymize_key(discipline_key)
            converted_dict[converted_key] = dict_to_convert[discipline_key]

        return converted_dict

    def load_study_from_input_dict(self, input_dict_to_load, update_status_configure=True):
        '''
        Load a study from an input dictionary : Convert the input_dictionary into a dm-like dictionary
        and compute the function load_study_from_dict
        '''
        dict_to_load = self.convert_input_dict_into_dict(input_dict_to_load)
        self.load_study_from_dict(
            dict_to_load, self.__unanonimize_key, update_status_configure=update_status_configure)

    def get_anonimated_data_dict(self):
        '''
        return execution engine data dict using anonimizin key for serialisation purpose
        '''

        converted_dict = {}
        dict_to_convert = self.dm.convert_data_dict_with_full_name()

        for key in dict_to_convert.keys():
            new_key = self.__anonymize_key(key)
            converted_dict[new_key] = dict_to_convert[key]

        return converted_dict

    def convert_input_dict_into_dict(self, input_dict):

        dm_dict = {key: {SoSDiscipline.VALUE: value}
                   for key, value in input_dict.items()}
        return dm_dict

    def load_study_from_dict(self, dict_to_load, anonymize_function=None, update_status_configure=True):
        '''
        method that imports data from dictionary to discipline tree

        :params: anonymize_function, a function that map a given key of the data
        dictionary using rule given by the execution engine for the saving process.
        If provided this function is use to load an anonymized reference data and
        compare the key with those of the current process
        :type: function

        :params: target_disc, SoSEval discipline to be configured during run method
        Optional parameter used only for evaluator process to avoid the configuration of all disciplines
        :type: SoSEval object
        '''
        self.logger.debug('loads data from dictionary')

        if anonymize_function is None:
            data_cache = dict_to_load
        else:
            data_cache = {}
            for key, value in dict_to_load.items():
                converted_key = anonymize_function(key)
                data_cache.update({converted_key: value})
        # keys of data stored in dumped study file are namespaced, convert them
        # to uuids
        convert_data_cache = self.dm.convert_data_dict_with_ids(data_cache)
        iteration = 0

        loop_stop = False
        # convergence loop: run discipline configuration until the number of sub disciplines is stable
        # that should mean all disciplines under discipline to load are deeply
        # configured

        checked_keys = []

        while not loop_stop:
            if self.__yield_method is not None:
                self.__yield_method()

            self.dm.no_change = True
            for key, value in self.dm.data_dict.items():

                if key in convert_data_cache:
                    # Only inject key which are set as input
                    # Discipline configuration only take care of input
                    # variables
                    # Variables are only set once
                    if value[SoSDiscipline.IO_TYPE] == SoSDiscipline.IO_TYPE_IN and not key in checked_keys:
                        value['value'] = convert_data_cache[key]['value']
                        checked_keys.append(key)

            self.__configure_io()

            if self.__yield_method is not None:
                self.__yield_method()
            convert_data_cache = self.dm.convert_data_dict_with_ids(data_cache)

            iteration = iteration + 1

            if self.root_process.is_configured():
                loop_stop = True
            elif iteration >= 100:
                self.logger.warn(
                    'CONFIGURE WARNING: root process is not configured after 100 iterations')
                loop_stop = True

        # Convergence is ended
        # Set all output variables (to be able to get results
        for key, value in self.dm.data_dict.items():
            if key in convert_data_cache:
                if value[SoSDiscipline.IO_TYPE] == SoSDiscipline.IO_TYPE_OUT:
                    value['value'] = convert_data_cache[key]['value']

        if self.__yield_method is not None:
            self.__yield_method()

        self.__configure_execution()

        #-- Init execute, to fully initialize models in discipline
        if len(dict_to_load):
            self.update_from_dm()
            self.check_inputs(raise_exception=False)
            self.__factory.init_execution()
            if update_status_configure:
                self.update_status_configure()

        self.dm.treeview = None

    def load_connectors_from_dict(self, connectors_to_load):
        '''
        set connectors data into dm
        :params: connectors_to_load, connectors data for each variables
        :type: dict with variableId, dict with connector values
        '''
        data_cache = {}
        for key, value in connectors_to_load.items():
            converted_key = self.__unanonimize_key(key)
            data_cache.update({converted_key: value})
        # keys of data stored in dumped study file are namespaced, convert them
        # to uuids
        convert_data_cache = self.dm.convert_data_dict_with_ids(data_cache)
        for key, value in convert_data_cache.items():
            if key in self.dm.data_dict.keys():
                variable_to_update = self.dm.data_dict[key]
                variable_to_update[SoSDiscipline.CONNECTOR_DATA] = value

    def check_inputs(self, raise_exception=True):
        '''
        Check the inputs in the DataManager
        '''
        self.dm.check_inputs(raise_exception)

    def set_debug_mode(self, mode=None, disc=None):
        ''' set recursively <disc> debug options of in SoSDiscipline
        '''
        if disc is None:
            disc = self.root_process
        mode_str = mode
        if mode_str is None:
            mode_str = "all"
        msg = "Debug mode activated for discipline %s with mode <%s>" % (
            disc.get_disc_full_name(), mode_str)
        self.logger.info(msg)
        # set check options
        if mode is None:
            disc.nan_check = True
            disc.check_if_input_change_after_run = True
            disc.check_linearize_data_changes = True
            disc.check_min_max_gradients = True
            disc.check_min_max_couplings = True
        elif mode == "nan":
            disc.nan_check = True
        elif mode == "input_change":
            disc.check_if_input_change_after_run = True
        elif mode == "linearize_data_change":
            disc.check_linearize_data_changes = True
        elif mode == "min_max_grad":
            disc.check_min_max_gradients = True
        elif mode == "min_max_couplings":
            if isinstance(disc, SoSCoupling):
                for sub_mda in disc.sub_mda_list:
                    sub_mda.debug_mode_couplings = True
        else:
            avail_debug = ["nan", "input_change",
                           "linearize_data_change", "min_max_grad", "min_max_couplings"]
            raise ValueError("Debug mode %s is not among %s" %
                             (mode, str(avail_debug)))
        # set debug modes of subdisciplines
        for disc in disc.sos_disciplines:
            self.set_debug_mode(mode, disc)

    def execute(self):
        ''' execution of the execution engine
        '''
        self.logger.info('PROCESS EXECUTION %s STARTS...',
                         self.root_process.get_disc_full_name())
#         self.root_process.clear_cache()
        self.fill_data_in_with_connector()
        self.update_from_dm()

        self.check_inputs(raise_exception=True)

        #-- init execute
        self.__factory.init_execution()

        #-- execution
        ex_proc = self.root_process.execute()
        self.root_process._update_status_dm(
            SoSDiscipline.STATUS_DONE)

        self.status = self.root_process.status
        self.logger.info('PROCESS EXECUTION %s ENDS.',
                         self.root_process.get_disc_full_name())
        return ex_proc
