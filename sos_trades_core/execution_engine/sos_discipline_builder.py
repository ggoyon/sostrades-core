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

from abc import abstractmethod

from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline


class SoSDisciplineBuilderException(Exception):
    pass


class SoSDisciplineBuilder(SoSDiscipline):
    '''**SoSDisciplineBuilder is a sosdiscipline which has the faculty to build sub disciplines
    '''

    # -- Disciplinary attributes
    @abstractmethod
    def build(self):
        ''' to be overloaded by subclasses
        Builds sub processes (i.e., in case of scatters, ...)'''

    def clean(self):
        """This method cleans a sos_discipline_builder, which is a discipline that can build other disciplines;
        We first begin by cleaning all the disciplines children, afterward we clean the discipline itself
        """
        for discipline in self.built_sos_disciplines:
            discipline.clean()
            self.ee.factory.remove_discipline_from_father_executor(discipline)
        SoSDiscipline.clean(self)

    def clean_children(self, list_children):
        """This method cleans the given list of children from the current discipline
        """
        for discipline in list_children:
            self.built_sos_disciplines.remove(discipline)
            discipline.clean()
            self.ee.factory.remove_discipline_from_father_executor(discipline)
