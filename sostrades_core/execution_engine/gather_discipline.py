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
from pandas import DataFrame
from copy import copy
from sostrades_core.tools.post_processing.charts.two_axes_instanciated_chart import InstanciatedSeries, \
    TwoAxesInstanciatedChart
from sostrades_core.tools.post_processing.charts.chart_filter import ChartFilter
from sostrades_core.execution_engine.sos_wrapp import SoSWrapp


class GatherDiscipline(SoSWrapp):
    """
    Generic Gather Discipline getting children outputs as inputs and gathering them as outputs
    """

    # ontology information
    _ontology_data = {
        'label': 'Gather Discipline',
        'type': 'Research',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': '',
        'version': '',
    }
    _maturity = 'Research'

    NEEDED_DATA_KEYS = ['type', 'unit', 'user_level', 'range', 'possible_values',
                        'dataframe_descriptor', 'dataframe_edition_locked',
                        'default', 'optional', 'numerical', SoSWrapp.VISIBILITY, SoSWrapp.NAMESPACE,
                        SoSWrapp.NS_REFERENCE]

    def setup_sos_disciplines(self):
        '''
           We add to the desc_in all the outputs of each child 
           We add to the desc_out the dict which will gather all inputs by name 
        '''
        dynamic_inputs, dynamic_outputs = self.build_dynamic_io()

        self.add_inputs(dynamic_inputs)
        self.add_outputs(dynamic_outputs)

    def build_dynamic_io(self):
        dynamic_inputs = {}
        dynamic_outputs = {}

        children_list = self.config_dependency_disciplines
        for child in children_list:
            # child_name = child.sos_name.replace(f'{self.sos_name}.', '')
            # child_name = child.get_disc_full_name().split(
            #     f'{self.sos_name}.')[-1]
            for output, output_dict in child.get_data_io_dict(self.IO_TYPE_OUT).items():

                data_in_dict = {
                    key: value for key, value in output_dict.items() if key in self.NEEDED_DATA_KEYS}

                # if input is local : then put it to shared visibility and add the local namespace from child to the gather discipline as shared namespace
                # if input is shared : copy the namespace and rename it (at least two namespaces with same name but different value since it is a gather)
                # then add it as shared namespace for the gather discipline
                output_namespace = copy(data_in_dict[self.NS_REFERENCE])
                if data_in_dict[self.VISIBILITY] == self.LOCAL_VISIBILITY:
                    data_in_dict[self.VISIBILITY] = self.SHARED_VISIBILITY
                else:
                    output_namespace.name = output_namespace.value.split('.', 1)[-1]

                output_namespace_name = output_namespace.name

                short_alias = '.'.join([substr for substr in output_namespace_name.split('.') if
                                        substr not in self.get_disc_display_name().split('.')])
                self.add_new_shared_ns(output_namespace)
                data_in_dict[self.NAMESPACE] = output_namespace_name

                dynamic_inputs[(output, short_alias)] = data_in_dict
                if output.endswith('_gather'):
                    output_name = output
                else:
                    output_name = f'{output}_gather'
                dynamic_outputs[output_name] = data_in_dict.copy()
                dynamic_outputs[output_name][self.TYPE] = 'dict'
                dynamic_outputs[output_name][self.VISIBILITY] = self.LOCAL_VISIBILITY
                del dynamic_outputs[output_name][self.NS_REFERENCE]
                del dynamic_outputs[output_name][self.NAMESPACE]
        return dynamic_inputs, dynamic_outputs

    def run(self):
        '''
        The run function of the generic gather discipline will only gather variables
        '''
        input_dict = self.get_sosdisc_inputs()
        output_dict = {}
        output_keys = self.get_sosdisc_outputs().keys()
        for out_key in output_keys:
            if out_key.endswith('_gather'):
                output_dict[out_key] = {}
                var_key = out_key.replace('_gather', '')

                for input_key in input_dict:
                    if isinstance(input_key, tuple) and input_key[0] == out_key:
                        # Then input_dict[input_key] is a dict
                        for input_input_key in input_dict[input_key]:
                            output_dict[out_key][input_input_key] = input_dict[input_key][input_input_key]

                    if isinstance(input_key, tuple) and input_key[0] == var_key:
                        output_dict[out_key][input_key[1]] = input_dict[input_key]

        self.store_sos_outputs_values(output_dict)

    def get_chart_filter_list(self):

        chart_filters = []
        output_dict = self.get_sosdisc_outputs()

        chart_list = [key.replace('_gather', '')
                      for key in output_dict.keys() if key.endswith('_gather')]

        chart_filters.append(ChartFilter(
            'Charts gather', chart_list, chart_list, 'Charts gather'))

        return chart_filters

    def get_post_processing_list(self, chart_filters=None):

        instanciated_charts = []
        # scenario_name = self.get_sosdisc_inputs('shared_scenario_name')

        # Overload default value with chart filter
        if chart_filters is not None:
            for chart_filter in chart_filters:
                if chart_filter.filter_key == 'Charts gather':
                    graphs_list = chart_filter.selected_values

        output_dict = self.get_sosdisc_outputs()

        for output_key, output_value in output_dict.items():
            if output_key.endswith('_gather'):

                chart_name = output_key.replace('_gather', '')
                chart_unit = self.get_data_out()[output_key][self.UNIT]
                first_value = list(output_value.values())[0]
                if isinstance(first_value, DataFrame):
                    if 'years' in list(output_value.values())[0].columns:

                        for column in list(output_value.values())[0].columns:
                            if column != 'years':
                                # need at list two values to compare to create
                                # a chart :
                                column_to_compare = 0
                                for gathered_key, gathered_output in output_value.items():
                                    if column in gathered_output:
                                        column_to_compare += 1
                                if column_to_compare >= 2:
                                    new_chart = TwoAxesInstanciatedChart('years', f'{column} [{chart_unit}]',
                                                                         chart_name=chart_name)
                                    for gathered_key, gathered_output in output_value.items():

                                        if column in gathered_output:
                                            product_serie = InstanciatedSeries(
                                                gathered_output['years'].values.tolist(
                                                ),
                                                gathered_output[column].values.tolist(), f'{gathered_key}', 'lines')

                                            new_chart.series.append(
                                                product_serie)
                                    instanciated_charts.append(new_chart)
                elif isinstance(first_value, (float, int)):
                    new_chart = TwoAxesInstanciatedChart('scenarios', f'{chart_name} [{chart_unit}]',
                                                         chart_name=f'{chart_name} comparison')
                    for gathered_key, gathered_output in output_value.items():
                        serie = InstanciatedSeries(
                            [gathered_key],
                            [gathered_output], f'{gathered_key}', 'bar')
                        new_chart.series.append(
                            serie)
                    instanciated_charts.append(new_chart)
        return instanciated_charts
