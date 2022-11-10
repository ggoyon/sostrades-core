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
import numpy as np

from sostrades_core.execution_engine.proxy_discipline import ProxyDiscipline
from sostrades_core.execution_engine.proxy_discipline_builder import ProxyDisciplineBuilder


class SoSDisciplineScatterException(Exception):
    pass


class ScatterTool():
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

    def __init__(self, sos_name, ee, map_name, cls_builder, associated_namespaces=None, coupling_per_scatter=False):
        '''
        Constructor
        '''
        self.__factory = ee.factory
        self.__scattered_builders = {}
        self.sub_coupling_builder_dict = {}
        self.__gathered_disciplines = {}
        self.__scatter_data_map = []
        self.__scatter_build_map = []

        self.coupling_per_scatter = coupling_per_scatter
        # associate map to discipline
        self.map_name = map_name
        self.__scatter_list = None
        self.local_namespace = None
        self.input_name = None
        self.ns_to_update = None
        self.ee = ee
        self.sos_name = sos_name
        if associated_namespaces is None:
            self.associated_namespaces = []
        else:
            self.associated_namespaces = associated_namespaces
        self.sc_map = self.ee.smaps_manager.get_build_map(self.map_name)
        ee.smaps_manager.associate_disc_to_build_map(self)
        self.sc_map.configure_map(cls_builder)
        self.__builders = cls_builder

        # add input_name to inst_desc_in
        self.build_inst_desc_in_with_map()
        self.builder_name = None
        if not isinstance(self.__builders, list):
            self.builder_name = self.__builders.sos_name

    @property
    def scatter_data_map(self):
        return self.__scatter_data_map

    @property
    def scatter_build_map(self):
        return self.__scatter_build_map

    def build_inst_desc_in_with_map(self):
        '''
        Consult the associated scatter map and adapt the inst_desc_in of the gather with the scatter var_name 
        '''
        self.scatter_list_name = self.sc_map.get_input_name()
        input_type = 'list'
        input_subtype_descriptor = {'list': 'string'}

        if self.sc_map.INPUT_NS in self.sc_map.get_map():
            scatter_desc_in = {self.scatter_list_name: {
                ProxyDiscipline.TYPE: input_type, ProxyDiscipline.SUBTYPE: input_subtype_descriptor,
                ProxyDiscipline.VISIBILITY: ProxyDiscipline.SHARED_VISIBILITY,
                ProxyDisciplineBuilder.NAMESPACE: self.sc_map.get_input_ns(), ProxyDiscipline.STRUCTURING: True}}
        else:
            scatter_desc_in = {self.scatter_list_name: {
                ProxyDiscipline.TYPE: input_type, ProxyDiscipline.SUBTYPE: input_subtype_descriptor,
                ProxyDiscipline.VISIBILITY: ProxyDiscipline.LOCAL_VISIBILITY,
                ProxyDiscipline.STRUCTURING: True}}

        self.__scatter_list_desc_in = scatter_desc_in

    def get_scatter_list_desc_in(self):

        return self.__scatter_list_desc_in

    def prepare_tool(self, driver):

        if self.scatter_list_name in driver.get_data_in().keys():
            scatter_list = driver.get_sosdisc_inputs(
                self.scatter_list_name)
            self.set_scatter_list(scatter_list)
        self.local_namespace = self.ee.ns_manager.get_local_namespace_value(
            driver)

        ns_to_update_name_list = self.sc_map.get_ns_to_update()
        # store ns_to_update namespace object
        self.ns_to_update = {}
        for ns_name in ns_to_update_name_list:
            self.ns_to_update[ns_name] = self.ee.ns_manager.get_shared_namespace(driver,
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

        # old_current_discipline = self.ee.factory.current_discipline
        # self.ee.factory.current_discipline = self

        # TO DO : do this with associate_namespaces
        # self.ee.ns_manager.update_shared_ns_with_others_ns(self)

        clean_builder_list = []
        if self.__scatter_list is not None:

            new_sub_names, clean_builder_list = self.clean_scattered_builders(
                self.__scatter_list)

            # build sub_process through the factory
            for name in self.__scatter_list:
                if self.coupling_per_scatter:
                    self.build_sub_coupling(
                        name, self.local_namespace, new_sub_names, self.ns_to_update)
                else:
                    self.build_child_scatter(
                        name, self.local_namespace, new_sub_names, self.ns_to_update)

            self.ee.ns_manager.shared_ns_dict.update(self.ns_to_update)

        return self.get_all_builders(), clean_builder_list

    def build_sub_coupling(self, name, local_namespace, new_sub_names, old_ns_to_update):

        # Call scatter map to modify the associated namespace
        self.sc_map.modify_scatter_ns(self.builder_name, name, local_namespace)

        self.sc_map.update_ns(
            old_ns_to_update, name, self.sos_name)

        if name in new_sub_names:

            coupling_builder = self.ee.factory.create_builder_coupling(name)

            coupling_builder.set_builder_info('cls_builder', self.__builders)
            coupling_builder.set_builder_info('with_data_io', True)
            self.sub_coupling_builder_dict[name] = coupling_builder

#             coupling_disc = coupling_builder.build()
#             # flag the coupling so that it can be executed in parallel
#             coupling_disc.is_parallel = True
            self.add_scatter_builder(coupling_builder, name)

#         else:
#             coupling_disc = self.sub_coupling_builder_dict[name].build()
#             # flag the coupling so that it can be executed in parallel
#             coupling_disc.is_parallel = True

    def build_child_scatter(self, name, local_namespace, new_sub_names, old_ns_to_update):

        # Call scatter map to modify the associated namespace
        ns_ids = []

        ns_scatter_id = self.sc_map.modify_scatter_ns(
            self.builder_name, name, local_namespace)
        ns_ids.append(ns_scatter_id)
        ns_update_ids = self.sc_map.update_ns(
            old_ns_to_update, name, self.sos_name, add_in_shared_ns_dict=False)
        ns_ids.extend(ns_update_ids)
        # Case of a scatter of coupling :
        if isinstance(self.__builders, list):
            self.build_scatter_of_coupling(
                name, local_namespace, new_sub_names, ns_update_ids)

        # Case of a coupling of scatter :
        else:
            self.build_coupling_of_scatter(
                name, new_sub_names, ns_update_ids)

    def build_scatter_of_coupling(self, name, local_namespace, new_sub_names, ns_update_ids):
        '''
        # We set the scatter_name in the namespace and the discipline is called with its origin name
        #
        # scatter
        #        |_name_1
        #                |_Disc1
        #                |_Disc2
        #        |_name_2
        #                |_Disc1
        #                |_Disc2

        '''
        for builder in self.__builders:
            #             self.ee.ns_manager.set_current_disc_ns(
            #                 f'{local_namespace}.{name}')
            old_builder_name = builder.sos_name
            builder.set_disc_name(f'{name}.{old_builder_name}')
            if builder.associated_namespaces != []:
                builder.add_namespace_list_in_associated_namespaces(
                    self.associated_namespaces)

            builder.add_namespace_list_in_associated_namespaces(
                ns_update_ids)
            #disc = builder.build()
            # Add the discipline only if it is in
            # new_sub_names
            if name in new_sub_names:
                self.add_scatter_builder(builder, name)

    def build_coupling_of_scatter(self, name, new_sub_names, ns_update_ids):
        '''
        # We set the scatter_name as the discipline name in the scatter which has already the name of the builder
        # Disc1 is the scatter
        #
        # Disc1
        #        |_name_1
        #        |_name_2
        '''
        old_builder_name = self.__builders.sos_name
        self.__builders.set_disc_name(name)
        if self.__builders.associated_namespaces != []:
            self.__builders.add_namespace_list_in_associated_namespaces(
                self.associated_namespaces)

        self.__builders.add_namespace_list_in_associated_namespaces(
            ns_update_ids)
        #disc = self.__builders.build()
        # self.__builders.set_disc_name(old_builder_name)

        # Add the discipline only if it is in
        # new_sub_names
        if name in new_sub_names:
            self.add_scatter_builder(self.__builders, name)

        return self.__builders

    def clean_scattered_builders(self, sub_names):
        '''
        Clean disciplines that was scattered and are not in the scatter_list anymore
        Return the new scatter names not yet present in the list of scattered disciplines
        '''
        # sort sub_names to filter new names and disciplines to remove
        clean_builders_list = []
        new_sub_names = [
            name for name in sub_names if not name in self.__scattered_builders]
        disc_name_to_remove = [
            name for name in self.__scattered_builders if not name in sub_names]
        for disc_name in disc_name_to_remove:
            clean_builders_list.append(self.__scattered_builders[disc_name])
            if self.coupling_per_scatter:
                del self.sub_coupling_builder_dict[disc_name]

            del self.__scattered_builders[disc_name]

        return new_sub_names, clean_builders_list

    def remove_scattered_disciplines(self, disc_to_remove):
        '''
        Remove a list of disciplines from the scattered_disciplines
        '''

        for disc in disc_to_remove:
            if self.coupling_per_scatter:
                del self.sub_coupling_builder_dict[disc]

            del self.__scattered_builders[disc]

    def add_scatter_builder(self, builder, name):
        '''
        Add the discipline to the factory and to the dictionary of scattered_disciplines
        '''
        # self.__factory.add_discipline(disc)

        self.__scattered_builders.update({name: builder})
#         if disc not in self.built_proxy_disciplines:
#             self.built_proxy_disciplines.append(disc)

    def get_all_builders(self):

        return list(self.__scattered_builders.values())
