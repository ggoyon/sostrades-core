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

from copy import deepcopy

'''
mode: python; py-indent-offset: 4; tab-width: 8; coding: utf-8
'''

from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
import pandas as pd

import numpy as np

from sos_trades_core.execution_engine.data_connector.ontology_data_connector import (
    OntologyDataConnector)

import openturns as ot
from scipy.interpolate import RegularGridInterpolator
from scipy.stats import norm
import chaospy as cp

from sos_trades_core.tools.post_processing.charts.chart_filter import ChartFilter
import plotly.graph_objects as go
from sos_trades_core.tools.post_processing.post_processing_tools import format_currency_legend
from sos_trades_core.tools.post_processing.plotly_native_charts.instantiated_plotly_native_chart import \
    InstantiatedPlotlyNativeChart


class UncertaintyQuantification(SoSDiscipline):
    '''
    Generic Uncertainty Quantification class
    '''

    # ontology information
    _ontology_data = {
        'label': 'Uncertainty Quantification Model',
        'type': 'Research',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': 'fa-solid fa-chart-area',
        'version': '',
    }

    EVAL_INPUTS = 'eval_inputs'
    EVAL_OUTPUTS = 'eval_outputs'
    DEFAULT = 'default'
    UPPER_BOUND = "upper_bnd"
    LOWER_BOUND = "lower_bnd"
    NB_POINTS = 'nb_points'

    DESC_IN = {
        'samples_inputs_df': {'type': 'dataframe', 'unit': None, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
                              'namespace': 'ns_grid_search', },
        'samples_outputs_df': {'type': 'dataframe', 'unit': None, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
                               'namespace': 'ns_grid_search', },
        'design_space': {
            'type': 'dataframe',
            'dataframe_descriptor': {
                'shortest_name': ('string', None, False),
                LOWER_BOUND: ('float', None, True),
                UPPER_BOUND: ('float', None, True),
                NB_POINTS: ('int', None, True),
                'full_name': ('string', None, False),
            },
            'structuring': True, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
            'namespace': 'ns_grid_search'
        },

        'confidence_interval': {'type': 'float', 'unit': '%', 'default': 90, 'range': [0., 100.],
                                'visibility': SoSDiscipline.SHARED_VISIBILITY,
                                'namespace': 'ns_uncertainty_quantification', 'structuring': False, 'numerical': True,
                                'user_level': 2},
        'sample_size': {'type': 'float', 'unit': None, 'default': 1000, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
                        'namespace': 'ns_uncertainty_quantification', 'structuring': False, 'numerical': True,
                        'user_level': 2},

        EVAL_INPUTS: {
            'type': 'dataframe',
            'dataframe_descriptor': {
                'selected_input': ('bool', None, True),
                'full_name': ('string', None, False),
                'shortest_name': ('string', None, False),
                # 'ontology_name': ('string', None, False),je
            },
            'dataframe_edition_locked': False,
            'structuring': True, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
            'namespace': 'ns_grid_search'
        },
        EVAL_OUTPUTS: {
            'type': 'dataframe',
            'dataframe_descriptor': {
                'selected_output': ('bool', None, True),
                'full_name': ('string', None, False),
                'shortest_name': ('string', None, False),
                # 'ontology_name': ('string', None, False),
            },
            'dataframe_edition_locked': False,
            'structuring': True, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
            'namespace': 'ns_grid_search'
        },

    }

    DESC_OUT = {
        'input_parameters_samples_df': {'type': 'dataframe', 'unit': None,
                                        'visibility': SoSDiscipline.SHARED_VISIBILITY,
                                        'namespace': 'ns_uncertainty_quantification'},
        'output_interpolated_values_df': {'type': 'dataframe', 'unit': None,
                                          'visibility': SoSDiscipline.SHARED_VISIBILITY,
                                          'namespace': 'ns_uncertainty_quantification'},
    }

    def setup_sos_disciplines(self):
        if self._data_in != {}:

            dynamic_outputs = {}
            dynamic_inputs = {}
            if (self.EVAL_INPUTS in self._data_in) & (self.EVAL_INPUTS in self._data_in):
                # if (self.get_sosdisc_inputs('samples_inputs_df') is not None) & (self.get_sosdisc_inputs(
                # 'samples_outputs_df') is not None):
                eval_outputs = self.get_sosdisc_inputs('eval_outputs')
                eval_inputs = self.get_sosdisc_inputs('eval_inputs')

                # samples_df = self.get_sosdisc_inputs(
                #     'samples_inputs_df')

                if (eval_inputs is not None) & (eval_outputs is not None):

                    selected_inputs = eval_inputs[eval_inputs['selected_input']
                                                  == True]['full_name']

                    in_param = selected_inputs.tolist()
                    # in_param.sort()

                    selected_outputs = eval_outputs[eval_outputs['selected_output']
                                                    == True]['full_name']

                    out_param = selected_outputs.tolist()
                    out_param.sort()

                    # ontology name
                    ontology_connector = OntologyDataConnector()
                    data_connection = {
                        'endpoint': 'https://sostradesdemo.eu.airbus.corp:31234/api/ontology'
                    }
                    args = in_param + out_param
                    args = [val.split('.')[-1]
                            for val in args]
                    ontology_connector.set_connector_request(
                        data_connection, OntologyDataConnector.PARAMETER_REQUEST, args)
                    conversion_full_ontology = ontology_connector.load_data(
                        data_connection)

                    possible_distrib = ['Normal', 'PERT',
                                        'LogNormal', 'Triangular']

                    # distrib = [possible_distrib[random.randrange(
                    # len(possible_distrib))] for i in range(len(in_param))]
                    # distrib = ['Normal', 'PERT', 'Triangular']
                    def random_distribution(input):
                        return np.random.choice([i for i in range(len(possible_distrib))],
                                                # p=[1 / len(possible_distrib)
                                                # for input in
                                                # possible_distrib])
                                                p=[0, 1, 0, 0])

                    # distrib = [possible_distrib[random_distribution(input)] for input in selected_inputs.tolist()]
                    distrib = ['PERT' for input in selected_inputs.tolist()]

                    if (('design_space' in self._data_in) & (len(in_param) > 0)):

                        if (self._data_in['design_space']['value'] is not None):

                            lower_bnd = self._data_in['design_space']['value'][self.LOWER_BOUND]
                            upper_bnd = self._data_in['design_space']['value'][self.UPPER_BOUND]
                            input_distribution_default = pd.DataFrame(
                                {'parameter': in_param, 'distribution': distrib, 'lower_parameter': lower_bnd,
                                 'upper_parameter': upper_bnd,
                                 'most_probable_value': [(a + b) / 2 for a, b in zip(lower_bnd, upper_bnd)]})

                            input_distribution_default.loc[input_distribution_default['distribution']
                                                           == 'Normal', 'most_probable_value'] = np.nan
                            input_distribution_default.loc[input_distribution_default['distribution']
                                                           == 'LogNormal', 'most_probable_value'] = np.nan

                            data_details_default = pd.DataFrame()
                            for input in list(set(in_param)):
                                try:
                                    [name, unit] = conversion_full_ontology[input.split(
                                        '.')[-1]]
                                except Exception as ex:
                                    print(
                                        'The following exception occurs when trying to reach Ontology server', ex)
                                    [name, unit] = [input, None]
                                data_details_default = data_details_default.append(
                                    {'type': 'input', 'variable': input, 'name': name, 'unit': unit}, ignore_index=True)
                            for output in list(set(out_param)):
                                try:
                                    [name, unit] = conversion_full_ontology[output.split(
                                        '.')[-1]]
                                except Exception as ex:
                                    print(
                                        'The following exception occurs when trying to reach Ontology server', ex)
                                    [name, unit] = [output, None]

                                data_details_default = data_details_default.append(
                                    {'type': 'output', 'variable': output, 'name': name, 'unit': unit}, ignore_index=True)

                            dynamic_inputs['input_distribution_parameters_df'] = {
                                'type': 'dataframe',
                                'dataframe_descriptor': {
                                    'parameter': ('string', None, False),
                                    'distribution': ('string', None, True),
                                    'lower_parameter': ('float', None, True),
                                    'upper_parameter': ('float', None, True),
                                    'most_probable_value': ('float', None, True),
                                },
                                'unit': '-',
                                'visibility': SoSDiscipline.SHARED_VISIBILITY,
                                'namespace': 'ns_uncertainty_quantification',
                                'default': input_distribution_default,
                                'structuring': False
                            }

                            dynamic_inputs['data_details_df'] = {
                                'type': 'dataframe',
                                'dataframe_descriptor': {
                                    'type': ('string', None, False),
                                    'variable': ('string', None, False),
                                    'name': ('string', None, True),
                                    'unit': ('string', None, True),
                                },
                                'unit': None,
                                'visibility': SoSDiscipline.SHARED_VISIBILITY,
                                'namespace': 'ns_uncertainty_quantification',
                                'default': data_details_default,
                                'structuring': False
                            }

                            if 'input_distribution_parameters_df' in self._data_in:
                                self._data_in['input_distribution_parameters_df']['value'] = self.get_sosdisc_inputs(
                                    'input_distribution_parameters_df')
                                self._data_in['data_details_df']['value'] = self.get_sosdisc_inputs(
                                    'data_details_df')
                                if (self.get_sosdisc_inputs('design_space')['full_name'].to_list() != in_param) or (self.get_sosdisc_inputs('input_distribution_parameters_df')['lower_parameter'].to_list() != self.get_sosdisc_inputs('design_space')['lower_bnd'].to_list()) or (self.get_sosdisc_inputs('input_distribution_parameters_df')['upper_parameter'].to_list() != self.get_sosdisc_inputs('design_space')['upper_bnd'].to_list()):
                                    self._data_in['input_distribution_parameters_df']['value'] = input_distribution_default
                                    self._data_in['data_details_df']['value'] = data_details_default
                                if (self.get_sosdisc_inputs('data_details_df')['variable'].to_list() != (in_param + out_param)):
                                    self._data_in['data_details_df']['value'] = data_details_default

            self.add_inputs(dynamic_inputs)
            self.add_outputs(dynamic_outputs)

    def run(self):
        self.check_inputs_consistency()
        inputs_dict = self.get_sosdisc_inputs()
        samples_df = inputs_dict['samples_inputs_df']
        data_df = inputs_dict['samples_outputs_df']
        reference_scenario_index = len(data_df) - 1
        samples_df = samples_df.drop(index=reference_scenario_index)
        data_df = data_df.drop(index=reference_scenario_index)
        confidence_interval = inputs_dict['confidence_interval'] / 100
        sample_size = inputs_dict['sample_size']
        input_parameters_names = list(samples_df.columns)[1:]
        output_names = list(data_df.columns)[1:]
        input_distribution_parameters_df = deepcopy(inputs_dict['input_distribution_parameters_df'])
        input_distribution_parameters_df['values'] = [sorted(
            list(samples_df[input_name].unique())) for input_name in input_parameters_names]

        # fixes a particular state of the random generator algorithm thanks to
        # the seed sample_size
        np.random.seed(42)
        ot.RandomGenerator.SetSeed(42)

        # INPUT PARAMETERS DISTRIBUTION IN
        # [NORMAL, PERT, LOGNORMAL,TRIANGULAR]
        input_parameters_samples_df = pd.DataFrame()
        distrib_list = []
        for input_name in input_parameters_names:
            if input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter'] == input_name][
                    'distribution'].values[0] == 'Normal':
                distrib = self.Normal_distrib(
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['lower_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['upper_parameter'].values[0],
                    confidence_interval=confidence_interval
                )
            elif input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter'] == input_name][
                    'distribution'].values[0] == 'PERT':
                distrib = self.PERT_distrib(
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['lower_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['upper_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['most_probable_value'].values[0],
                )
            elif input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter'] == input_name][
                    'distribution'].values[0] == 'LogNormal':
                distrib = self.LogNormal_distrib(
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['lower_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['upper_parameter'].values[0],
                    confidence_interval=confidence_interval
                )
            elif input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter'] == input_name][
                    'distribution'].values[0] == 'Triangular':
                distrib = self.Triangular_distrib(
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['lower_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['upper_parameter'].values[0],
                    input_distribution_parameters_df.loc[input_distribution_parameters_df['parameter']
                                                         == input_name]['most_probable_value'].values[0],
                )
            else:
                self.logger.exception(
                    'Exception occurred: possible values in distribution are [Normal, PERT, Triangular, LogNormal].'
                )
            distrib_list.append(distrib)
            input_parameters_samples_df[f'{input_name}'] = pd.DataFrame(
                np.array(distrib.getSample(sample_size)))

        # MONTECARLO COMPOSED DISTRIBUTION
        R = ot.CorrelationMatrix(len(input_parameters_names))
        copula = ot.NormalCopula(R)
        distribution = ot.ComposedDistribution(distrib_list, copula)
        composed_distrib_sample = distribution.getSample(sample_size)

        # plot
        # for i in range(len(input_parameters)):
        #     graph = distribution.drawMarginal1DPDF(i, 90, 100, 256)
        #     graph.setTitle(
        #         f'{input_parameters[i]} {input_param_dict[input_parameters[i]]["distribution"]} distribution')
        #     view = View(graph, plot_kw={'color': 'blue'})

        # INTERPOLATION
        input_parameters_single_values_tuple = tuple([input_distribution_parameters_df.loc[
            input_distribution_parameters_df['parameter'] == input_name][
            'values'].values[0]
            for input_name in input_parameters_names])
        input_dim_tuple = tuple([len(sub_t)
                                 for sub_t in input_parameters_single_values_tuple])

        # merge and sort data according to scenarii in the right order for
        # interpolation
        all_data_df = samples_df.merge(data_df, on='scenario', how='left')
        all_data_df = all_data_df.sort_values(by=input_parameters_names)
        output_interpolated_values_df = pd.DataFrame()
        for output_name in output_names:
            y = list(all_data_df[output_name])
            # adapt output format to be used by RegularGridInterpolator
            output_values = np.reshape(y, input_dim_tuple)
            f = RegularGridInterpolator(
                input_parameters_single_values_tuple, output_values, bounds_error=False)
            output_interpolated_values = f(composed_distrib_sample)
            output_interpolated_values_df[f'{output_name}'] = output_interpolated_values

        dict_values = {
            'input_parameters_samples_df': input_parameters_samples_df,
            'output_interpolated_values_df': output_interpolated_values_df,
        }

        self.store_sos_outputs_values(dict_values)

    def Normal_distrib(self, lower_bnd, upper_bnd, confidence_interval=0.95):
        # Normal distribution
        # 90% confidence interval : ratio = 3.29
        # 95% confidence interval : ratio = 3.92
        # 99% confidence interval : ratio = 5.15
        norm_val = float(format(1 - confidence_interval, '.2f')) / 2
        ratio = norm.ppf(1 - norm_val) - norm.ppf(norm_val)

        mu = (lower_bnd + upper_bnd) / 2
        sigma = (upper_bnd - lower_bnd) / ratio
        distrib = ot.Normal(mu, sigma)

        # plot
        # graph = distrib.drawMarginal1DPDF(0, lower_bnd, upper_bnd, 256)
        # view = View(graph, plot_kw={'color': 'blue'})

        return distrib

    def PERT_distrib(self, lower_bnd, upper_bnd, most_probable_val):
        # PERT distribution (from chaopsy library cause ot doesnt have it)
        chaospy_dist = cp.PERT(lower_bnd, most_probable_val, upper_bnd)
        distrib = ot.Distribution(
            ot.ChaospyDistribution(chaospy_dist))

        # plot
        # graph = distrib.drawMarginal1DPDF(0, lower_bnd, upper_bnd, 256)
        # view = View(graph, plot_kw={'color': 'blue'})

        return distrib

    def Triangular_distrib(self, lower_bnd, upper_bnd, most_probable_val):
        distrib = ot.Triangular(int(lower_bnd), int(
            most_probable_val), int(upper_bnd))

        # plot
        # graph = distrib.drawMarginal1DPDF(0, lower_bnd, upper_bnd, 256)
        # view = View(graph, plot_kw={'color': 'blue'})

        return distrib

    def LogNormal_distrib(self, lower_bnd, upper_bnd, confidence_interval=0.95):
        # Normal distribution
        # 90% confidence interval : ratio = 3.29
        # 95% confidence interval : ratio = 3.92
        # 99% confidence interval : ratio = 5.15
        norm_val = float(format(1 - confidence_interval, '.2f')) / 2
        ratio = norm.ppf(1 - norm_val) - norm.ppf(norm_val)

        mu = (lower_bnd + upper_bnd) / 2
        sigma = (upper_bnd - lower_bnd) / ratio

        distrib = ot.LogNormal()
        distrib.setParameter(ot.LogNormalMuSigma()([mu, sigma, 0]))

        # plot
        # graph = distrib.drawMarginal1DPDF(0, lower_bnd, upper_bnd, 256)
        # view = View(graph, plot_kw={'color': 'blue'})

        return distrib

    def check_inputs_consistency(self):
        """ check consistency between inputs from eval_inputs and samples_inputs_df
        """
        inputs_dict = self.get_sosdisc_inputs()
        eval_inputs = inputs_dict['eval_inputs']
        selected_inputs = eval_inputs[eval_inputs['selected_input']
                                      == True]['full_name']
        selected_inputs = selected_inputs.tolist()
        inputs_from_samples = inputs_dict['samples_inputs_df']
        input_from_samples = list(inputs_from_samples.columns)[1:]

        if set(selected_inputs) != set(input_from_samples):
            self.logger.exception(
                'selected inputs from eval inputs must be the same than inputs from the samples inputs dataframe')

    def get_chart_filter_list(self):

        # For the outputs, making a graph for tco vs year for each range and for specific
        # value of ToT with a shift of five year between then

        chart_filters = []

        in_names = []
        out_names = []
        if 'data_details_df' in self.get_sosdisc_inputs():
            data_df = self.get_sosdisc_inputs(['data_details_df'])
            in_names = data_df.loc[data_df['type']
                                   == 'input', 'name'].to_list()
        if 'output_interpolated_values_df' in self.get_sosdisc_outputs():
            out_df = self.get_sosdisc_outputs(
                ['output_interpolated_values_df']).keys().to_list()
            out_names = [n.split('.')[-1] for n in out_df]

        names_list = in_names + out_names
        chart_list = [n + ' Distribution' for n in names_list]
        chart_filters.append(ChartFilter(
            'Charts', chart_list, chart_list, 'Charts'))

        return chart_filters

    def get_post_processing_list(self, filters=None):

        # For the outputs, making a bar graph with gradients values

        instanciated_charts = []

        if filters is not None:
            for chart_filter in filters:
                if chart_filter.filter_key == 'Charts':
                    graphs_list = chart_filter.selected_values

        if 'output_interpolated_values_df' in self.get_sosdisc_outputs():
            output_distrib_df = deepcopy(
                self.get_sosdisc_outputs('output_interpolated_values_df')
            )
        if 'input_parameters_samples_df' in self.get_sosdisc_outputs():
            input_parameters_distrib_df = deepcopy(
                self.get_sosdisc_outputs('input_parameters_samples_df')
            )
        if 'data_details_df' in self.get_sosdisc_inputs():
            self.data_details = deepcopy(
                self.get_sosdisc_inputs(['data_details_df']))
        if 'input_distribution_parameters_df' in self.get_sosdisc_inputs():
            input_distribution_parameters_df = deepcopy(
                self.get_sosdisc_inputs(['input_distribution_parameters_df']))
        if 'confidence_interval' in self.get_sosdisc_inputs():
            confidence_interval = deepcopy(
                self.get_sosdisc_inputs(['confidence_interval'])) / 100

        for input_name in list(input_parameters_distrib_df.columns):
            input_distrib = list(input_parameters_distrib_df[input_name])
            input_distrib_name = self.data_details.loc[self.data_details["variable"]
                                                       == input_name]["name"].values[0] + ' Distribution'
            if input_distrib_name in graphs_list:
                new_chart = self.input_histogram_graph(
                    input_distrib, input_name, input_distribution_parameters_df, confidence_interval)
                instanciated_charts.append(new_chart)

        for output_name in list(output_distrib_df.columns):
            output_distrib = list(output_distrib_df[output_name])
            output_distrib_name = output_name.split('.')[-1] + ' Distribution'
            if not all(np.isnan(output_distrib)):
                if output_distrib_name in graphs_list:
                    new_chart = self.output_histogram_graph(
                        output_distrib, output_name, confidence_interval)
                    instanciated_charts.append(new_chart)

        return instanciated_charts

    def input_histogram_graph(self, data, data_name, distrib_param, confidence_interval):
        name = self.data_details.loc[self.data_details["variable"]
                                     == data_name]["name"].values[0]
        unit = self.data_details.loc[self.data_details["variable"]
                                     == data_name]["unit"].values[0]
        hist_y = go.Figure()
        hist_y.add_trace(go.Histogram(x=list(data),
                                      nbinsx=100, histnorm='probability'))

        # statistics on data list
        distribution_type = distrib_param.loc[distrib_param['parameter']
                                              == data_name]['distribution'].values[0]
        data_list = [x for x in data if np.isnan(x) == False]
        bins = np.histogram_bin_edges(data_list, bins=100)
        hist = np.histogram(data_list, bins=bins)[0]
        norm_hist = hist / np.cumsum(hist)[-1]

        y_max = max(norm_hist)
        most_probable_val = bins[np.argmax(norm_hist)]
        median = np.median(data_list)
        y_mean = np.mean(data_list)
        if distrib_param.loc[distrib_param['parameter'] == data_name]['distribution'].values[0] in ['Normal',
                                                                                                    'LogNormal']:
            # left boundary confidence interval
            lb = float(format(1 - confidence_interval, '.2f')) / 2
            y_left_boundary = np.nanquantile(list(data), lb)
            y_right_boundary = np.nanquantile(list(data), 1 - lb)
        else:
            y_left_boundary = distrib_param.loc[distrib_param['parameter']
                                                == data_name]['lower_parameter'].values[0]
            y_right_boundary = distrib_param.loc[distrib_param['parameter']
                                                 == data_name]['upper_parameter'].values[0]

        hist_y.update_layout(xaxis=dict(
            title=name,
            ticksuffix=unit),
            yaxis=dict(title='Probability'))

        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_left_boundary,
                         x1=y_left_boundary,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))

        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_right_boundary,
                         x1=y_right_boundary,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))

        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_mean,
                         x1=y_mean,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))

        hist_y.add_annotation(
            x=y_left_boundary,
            y=y_max,
            font=dict(color="black", size=12),
            text=" Lower parameter ",
            showarrow=False,
            xanchor="right",
        )
        hist_y.add_annotation(
            x=y_right_boundary,
            y=y_max,
            font=dict(color="black", size=12),
            text=" Upper parameter ",
            showarrow=False,
            xanchor="left",
        )
        hist_y.add_annotation(
            x=y_mean,
            y=0.75 * y_max,
            font=dict(color="black", size=12),
            text=" Mean ",
            showarrow=False,
            xanchor="left",
        )
        hist_y.add_annotation(
            x=0.85,
            y=1.15,
            font=dict(
                family='Arial',
                color='#7f7f7f',
                size=10),
            text=f' Mean: {format_currency_legend(y_mean, unit)} <br> Median: {format_currency_legend(median, unit)} ',
            showarrow=False,
            xanchor="left",
            align="right",
            xref='paper',
            yref='paper',
            bordercolor='black',
            borderwidth=1,
        )

        hist_y.update_layout(showlegend=False)

        new_chart = InstantiatedPlotlyNativeChart(
            fig=hist_y, chart_name=f'{name} - {distribution_type} Distribution', default_legend=False)

        # new_chart.to_plotly().show()

        return new_chart

    def output_histogram_graph(self, data, data_name, confidence_interval):

        name = data_name
        unit = None
        eval_output_name = data_name

        if len(data_name.split('.')) > 1:
            name = data_name.split('.')[1]
            eval_output_name = data_name.split('.')[0]

        ns_from_var = self.ee.dm.get_all_namespaces_from_var_name(
            eval_output_name)
        ns_from_var_wo_study_list = [elem.split(
            self.ee.study_name + '.')[1] for elem in ns_from_var]
        var_name_list = list(set(ns_from_var_wo_study_list) & set(
            self.data_details['variable'].to_list()))
        if len(var_name_list) > 0:
            var_name = var_name_list[0]
        else:
            var_name = None

        if var_name is not None:
            unit = self.data_details.loc[self.data_details["variable"]
                                         == var_name]["unit"].values[0]
        hist_y = go.Figure()
        hist_y.add_trace(go.Histogram(x=list(data),
                                      nbinsx=100, histnorm='probability'))

        # statistics on data list
        data_list = [x for x in data if np.isnan(x) == False]
        bins = np.histogram_bin_edges(data_list, bins=100)
        hist = np.histogram(data_list, bins=bins)[0]
        norm_hist = hist / np.cumsum(hist)[-1]
        y_max = max(norm_hist)
        most_probable_val = bins[np.argmax(norm_hist)]
        median = np.median(data_list)
        y_mean = np.mean(data_list)

        # left boundary confidence interval
        lb = float(format(1 - confidence_interval, '.2f')) / 2
        y_left_boundary = np.nanquantile(list(data), lb)
        y_right_boundary = np.nanquantile(list(data), 1 - lb)
        hist_y.update_layout(xaxis=dict(
            title=name,
            ticksuffix=unit),
            yaxis=dict(title='Probability'))

        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_left_boundary,
                         x1=y_left_boundary,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))

        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_right_boundary,
                         x1=y_right_boundary,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))
        hist_y.add_shape(type='line', xref='x', yref='paper',
                         x0=y_mean,
                         x1=y_mean,
                         y0=0, y1=1,
                         line=dict(color="black", width=2, dash="dot",))

        hist_y.add_shape(type='rect', xref='x', yref='paper',
                         x0=y_left_boundary,
                         x1=y_right_boundary,
                         y0=0, y1=1,
                         line=dict(color="LightSeaGreen"),
                         fillcolor="PaleTurquoise", opacity=0.2)

        hist_y.add_annotation(
            x=y_left_boundary,
            y=y_max,
            font=dict(color="black", size=12),
            text=f' {format_currency_legend(y_left_boundary, unit)} ',
            showarrow=False,
            xanchor="right",
        )

        hist_y.add_annotation(
            x=y_right_boundary,
            y=y_max,
            font=dict(color="black", size=12),
            text=f' {format_currency_legend(y_right_boundary, unit)}',
            showarrow=False,
            xanchor="left",
        )

        hist_y.add_annotation(
            x=y_mean,
            y=0.75 * y_max,
            font=dict(color="black", size=12),
            text=f' {format_currency_legend(y_mean, unit)} ',
            showarrow=False,
            xanchor="left",
        )

        hist_y.add_annotation(
            x=0.60,
            y=1.15,
            font=dict(
                family='Arial',
                color='#7f7f7f',
                size=10),
            text=f'Confidence Interval: {int(confidence_interval * 100)} % [{format_currency_legend(y_left_boundary,"")}, {format_currency_legend(y_right_boundary,"")}] {unit} <br> Mean: {format_currency_legend(y_mean, unit)} <br> Median: {format_currency_legend(median, unit)} ',
            showarrow=False,
            xanchor="left",
            align="right",
            xref='paper',
            yref='paper',
            bordercolor='black',
            borderwidth=1,
        )

        hist_y.update_layout(showlegend=False)

        # percent_pos = len([p for p in data if p > 0]) / len(data) * 100

        new_chart = InstantiatedPlotlyNativeChart(
            fig=hist_y, chart_name=f'{name} Distribution', default_legend=False)

        # new_chart.to_plotly().show()

        return new_chart
