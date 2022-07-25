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
from sostrades_core.execution_engine.SoSWrapp import SoSWrapp
from sostrades_core.execution_engine.proxy_discipline import ProxyDiscipline
from sostrades_core.tools.post_processing.charts.two_axes_instanciated_chart import InstanciatedSeries, TwoAxesInstanciatedChart
from sostrades_core.tools.post_processing.charts.chart_filter import ChartFilter


class Disc1(SoSWrapp):

    # ontology information
    _ontology_data = {
        'label': 'sostrades_core.sos_wrapping.test_discs.disc1',
        'type': 'Research',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fas fa-plane fa-fw',
        'version': '',
    }
    _maturity = 'Fake'
    DESC_IN = {
        'x': {'type': 'float', 'unit': '-', 'visibility': ProxyDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_ac'},
        'a': {'type': 'float', 'unit': '-'},
        'b': {'type': 'float', 'unit': '-'}
    }
    DESC_OUT = {
        'indicator': {'type': 'float', 'unit': '-'},
        'y': {'type': 'float', 'unit': '-', 'visibility': ProxyDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_ac'}
    }

    def run(self, proxy):
        x = proxy.get_sosdisc_inputs('x')
        a = proxy.get_sosdisc_inputs('a')
        b = proxy.get_sosdisc_inputs('b')
        dict_values = {'indicator': a * b, 'y': a * x + b}
        # put new field value in data_out
        proxy.store_sos_outputs_values(dict_values)

    # def get_chart_filter_list(self):
    #
    #     chart_filters = []
    #
    #     chart_list = ['y vs x']
    #
    #     chart_filters.append(ChartFilter(
    #         'Charts', chart_list, chart_list, 'graphs'))
    #
    #     return chart_filters
    #
    # def get_post_processing_list(self, filters=None):
    #
    #     instanciated_charts = []
    #
    #     # Overload default value with chart filter
    #     if filters is not None:
    #         for chart_filter in filters:
    #             if chart_filter.filter_key == 'graphs':
    #                 charts_list = chart_filter.selected_values
    #
    #     if 'y vs x' in charts_list:
    #
    #         chart_name = 'y vs x'
    #
    #         y = self.get_sosdisc_outputs('y')
    #         x = self.get_sosdisc_inputs('x')
    #         print(y, x)
    #         new_chart = TwoAxesInstanciatedChart('x (-)', 'y (-)',
    #                                              chart_name=chart_name)
    #         serie = InstanciatedSeries(
    #             [x], [y], '', 'lines')
    #
    #         new_chart.series.append(serie)
    #
    #         instanciated_charts.append(new_chart)
    #
    #     return instanciated_charts
