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

class Namespace:
    '''
    Specification: Namespace class describes name, value and dependencies of namespace object
    '''

    def __init__(self, name, value):
        '''
        Class to describe a namespace and manage several instance of the same namespace
        '''
        self.name = name
        self.value = value
        self.dependency_disc_list = []  # list of dependency disciplines

    def to_dict(self):
        ''' Method that serialize as dict a Namespace object '''
        return self.__dict__

    def update_value(self, val):
        '''
        Mechanism to update value
        '''
        self.value = val

    def get_value(self):
        '''
        Get the value in the Namespace
        '''
        return self.value

    def get_dependency_disc_list(self):
        '''
        Get the list of disciplines which use the namespace
        '''
        return self.dependency_disc_list

    def add_dependency(self, disc_id):
        '''
        Add namespace disciplinary dependency
        '''
        if disc_id not in self.dependency_disc_list:
            self.dependency_disc_list.append(disc_id)

    def remove_dependency(self, disc_id):
        '''
        Remove disciplinary dependency
        '''
        if disc_id in self.dependency_disc_list:
            self.dependency_disc_list.remove(disc_id)

    def __eq__(self, other):

        same_name = self.name == other.name
        same_value = self.value == other.value
        return same_name and same_value
