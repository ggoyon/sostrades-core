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
from sostrades_core.execution_engine.builder_tools.sos_tool import SosTool
'''
mode: python; py-indent-offset: 4; tab-width: 8; coding: utf-8
'''


class ScatterTool(SosTool):
    '''
    Class that build disciplines using a builder and a map containing data to scatter
    '''

    # ontology information
    _ontology_data = {
        'label': 'Scatter Tool',
        'type': 'Official',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fas fa-indent fa-fw',
        'version': '',
    }

    def __init__(self, sos_name, ee, cls_builder, map_name, coupling_per_scatter=False):
        '''
        Constructor
        '''

        SosTool.__init__(self, sos_name, ee, cls_builder)

        self.map_name = map_name
        self.coupling_per_scatter = coupling_per_scatter

        self.__scattered_disciplines = {}
        self.sub_coupling_builder_dict = {}
        self.__scatter_list = None
        self.local_namespace = None
        self.input_name = None
        self.ns_to_update = None
        self.sc_map = None

    def associate_tool_to_driver(self, driver, cls_builder=None, associated_namespaces=None):
        '''    
        MEthod that associate tool to the driver and add scatter map
        '''
        SosTool.associate_tool_to_driver(
            self, driver, cls_builder=cls_builder, associated_namespaces=associated_namespaces)
        self.sc_map = self.ee.smaps_manager.get_build_map(self.map_name)
        self.ee.smaps_manager.associate_disc_to_build_map(self)
        self.sc_map.configure_map(self.sub_builders)

    def prepare_tool(self):
        '''
        Prepare tool function if some data of the driver are needed to configure the tool
        '''
        if self.driver.SCENARIO_DF in self.driver.get_data_in():
            scenario_df = self.driver.get_sosdisc_inputs(
                self.driver.SCENARIO_DF)
            self.set_scatter_list(scenario_df[scenario_df[self.driver.SELECTED_SCENARIO]
                                              == True][self.driver.SCENARIO_NAME].values.tolist())

        self.local_namespace = self.ee.ns_manager.get_local_namespace_value(
            self.driver)

        ns_to_update_name_list = self.sc_map.get_ns_to_update()
        # store ns_to_update namespace object
        self.ns_to_update = {}
        for ns_name in ns_to_update_name_list:
            self.ns_to_update[ns_name] = self.ee.ns_manager.get_shared_namespace(self.driver,
                                                                                 ns_name)

    def set_scatter_list(self, scatter_list):
        self.__scatter_list = scatter_list

    def build(self):
        ''' 
        Configuration of the SoSscatter : 
        -First configure the scatter 
        -Get the list to scatter on and the associated namespace
        - Look if disciplines are already scatterred and compute the new list to scatter (only new ones)
        - Remove disciplines that are not in the scatter list
        - Scatter the instantiator cls and adapt namespaces depending if it is a list or a singleton
        '''

        if self.__scatter_list is not None:

            new_sub_names = self.clean_scattered_disciplines(
                self.__scatter_list)

            # build sub_process through the factory
            for name in self.__scatter_list:
                new_name = name in new_sub_names
                ns_ids_list = self.update_namespaces(name)
                if self.coupling_per_scatter:
                    self.build_sub_coupling(
                        name, new_name, ns_ids_list)
                else:
                    self.build_child_scatter(
                        name, new_name, ns_ids_list)
            self.clean_builders()

    def build_sub_coupling(self, name, new_name, ns_ids_list):
        '''
        Build a coupling for each name 
        name (string) : scatter_name in the scatter_list
        new_name (bool) : True if name is a new_name in the build
        ns_ids_list (list) : The list of ns_keys that already have been updated with the scatter_name and mus tbe associated to the builder 

        1. Create the coupling with its name
        2. Add all builders to the coupling
        3. Associate new namespaces to the builder (the coupling will associate the namespace to its children)
        4. Build the coupling builder
        5 Add the builded discipline to the driver and factory
        '''
        # Call scatter map to modify the associated namespace

        if new_name:

            coupling_builder = self.ee.factory.create_builder_coupling(name)
            coupling_builder.set_builder_info('cls_builder', self.sub_builders)

            self.associate_namespaces_to_builder(
                coupling_builder, ns_ids_list)
            self.sub_coupling_builder_dict[name] = coupling_builder

            coupling_disc = coupling_builder.build()
            # flag the coupling so that it can be executed in parallel
            coupling_disc.is_parallel = True
            self.add_scatter_discipline(coupling_disc, name)

        else:
            coupling_disc = self.sub_coupling_builder_dict[name].build()
            # flag the coupling so that it can be executed in parallel
            coupling_disc.is_parallel = True

    def update_namespaces(self, name):
        '''
        Update all ns_to_update namespaces and the scatter namespace with the scatter_name just after the local_namespace
        Return the list of namespace keys for future builder association
        All namespaces are not added in shared_ns_dict to be transparent and only associated to the right disciplines
        '''
        ns_scatter_id = self.sc_map.modify_scatter_ns(
            name, self.local_namespace)

        ns_update_ids = self.sc_map.update_ns(
            self.ns_to_update, name, self.local_namespace, add_in_shared_ns_dict=False)
        ns_ids_list = [ns_scatter_id]
        ns_ids_list.extend(ns_update_ids)

        return ns_ids_list

    def build_child_scatter(self, name, new_name, ns_ids_list):
        '''
        #        |_name_1
        #                |_Disc1
        #                |_Disc2
        #        |_name_2
        #                |_Disc1
        #                |_Disc2

        Build scattered disciplines directly under driver
        name (string) : scatter_name in the scatter_list
        new_name (bool) : True if name is a new_name in the build
        ns_ids_list (list) : The list of ns_keys that already have been updated with the scatter_name and mus tbe associated to the builder 

        1. Set builders as a list and loop over builders
        2. Set the new_name of the builder with the scatter name
        3. Associate new namespaces to the builder (the coupling will associate the namespace to its children)
        4. Build the builder
        5 Add the builded discipline to the driver and factory
        6. Set the old name to the builder for next iteration

        '''

        for builder in self.sub_builders:
            old_builder_name = builder.sos_name
            builder.set_disc_name(f'{name}.{old_builder_name}')
            if new_name:
                self.associate_namespaces_to_builder(builder, ns_ids_list)
            disc = builder.build()
            builder.set_disc_name(old_builder_name)
            # Add the discipline only if it is a new_name
            if new_name:
                self.add_scatter_discipline(disc, name)

    def clean_scattered_disciplines(self, sub_names):
        '''
        Clean disciplines that was scattered and are not in the scatter_list anymore
        Return the new scatter names not yet present in the list of scattered disciplines
        '''
        # sort sub_names to filter new names and disciplines to remove
        clean_builders_list = []
        new_sub_names = [
            name for name in sub_names if not name in self.__scattered_disciplines]
        disc_name_to_remove = [
            name for name in self.__scattered_disciplines if not name in sub_names]
        self.remove_scattered_disciplines(disc_name_to_remove)

        return new_sub_names

    def remove_scattered_disciplines(self, disc_to_remove):
        '''
        Remove a list of disciplines from the scattered_disciplines
        '''

        for disc in disc_to_remove:
            self.clean_from_driver(self.__scattered_disciplines[disc])
            if self.coupling_per_scatter:
                del self.sub_coupling_builder_dict[disc]

            del self.__scattered_disciplines[disc]

    def clean_all_disciplines_from_tool(self):
        all_disc_list = self.get_all_builded_disciplines()
        self.clean_from_driver(all_disc_list)

    def clean_from_driver(self, disc_list):
        """
        This method cleans the given list of children from the current discipline
        """
        self.driver.clean_children(disc_list)

    def add_scatter_discipline(self, disc, name):
        '''
        Add the discipline to the factory and to the dictionary of scattered_disciplines
        '''
        self.set_father_discipline()
        self.ee.factory.add_discipline(disc)
        if name in self.__scattered_disciplines.keys():
            self.__scattered_disciplines[name].append(disc)
        else:
            self.__scattered_disciplines.update({name: [disc]})

    def get_all_builded_disciplines(self):

        return [disc for disc_list in self.__scattered_disciplines.values() for disc in disc_list]

    def clean_builders(self):

        for builder in self.sub_builders:
            builder.delete_all_associated_namespaces()
