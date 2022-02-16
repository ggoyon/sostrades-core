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
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline


class Disc2(SoSDiscipline):

    # ontology information
    _ontology_data = {
        'label': 'sos_trades_core.sos_wrapping.test_discs.disc2_scenario',
        'type': 'Research',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fas fa-exchange fa-fw',
        'version': '',
    }
    _maturity = 'Fake'
    DESC_IN = {
        'y_dict': {'type': 'float', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_scenario'},
        'z': {'type': 'float', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_scenario'}
    }
    DESC_OUT = {
        't': {'type': 'float', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_scenario'},
        'u': {'type': 'float'}
    }

    def run(self):
        y_dict = self.get_sosdisc_inputs('y_dict')
        z = self.get_sosdisc_inputs('z')

        t = 0
        for val in y_dict.values():
            t += val

        dict_values = {'t': t * z,
                       'u': t + z}

        self.store_sos_outputs_values(dict_values)
