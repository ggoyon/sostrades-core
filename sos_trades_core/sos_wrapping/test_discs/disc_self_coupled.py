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


class SelfCoupledDiscipline(SoSDiscipline):
    _maturity = 'Fake'

    DESC_IN = {
        'x': {'type': 'float', 'default': 1}
    }
    DESC_OUT = {
        'x': {'type': 'float'}

    }

    def run(self):
        x = self.get_sosdisc_inputs('x')

        dict_values = {'x': x / 2}

        self.store_sos_outputs_values(dict_values)
