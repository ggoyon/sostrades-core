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
import logging
from sostrades_core.tools.base_functions.compute_len import compute_len
from numpy import zeros

LOGGER = logging.getLogger(__name__)

class SoSWrappException(Exception):
    pass

class SoSWrapp(object):
    '''**SoSWrapp** is the class from which inherits our model wrapper when using 'SoSTrades' wrapping mode.

    It contains necessary information for the discipline configuration. It is owned by both the MDODisciplineWrapp and
    the SoSMDODiscipline.

    Its methods setup_sos_disciplines, run,... are overloaded by the user-provided Wrapper.

    N.B.: setup_sos_disciplines needs take as argument the proxy and call proxy.add_inputs() and/or proxy.add_outputs().

    Attributes:
        sos_name (string): name of the discipline
        local_data_short_name (Dict[Dict]): short name version of the local data for model input and output
        local_data (Dict[Any]): output of the model last run
    '''
    # -- Disciplinary attributes
    DESC_IN = {}
    DESC_OUT = {}
    TYPE = 'type'
    SUBTYPE = 'subtype_descriptor'
    COUPLING = 'coupling'
    VISIBILITY = 'visibility'
    LOCAL_VISIBILITY = 'Local'
    INTERNAL_VISIBILITY = 'Internal'
    SHARED_VISIBILITY = 'Shared'
    NAMESPACE = 'namespace'
    VALUE = 'value'
    DEFAULT = 'default'
    EDITABLE = 'editable'
    USER_LEVEL = 'user_level'
    STRUCTURING = 'structuring'
    POSSIBLE_VALUES = 'possible_values'
    RANGE = 'range'
    UNIT = 'unit'
    NUMERICAL = 'numerical'
    DESCRIPTION = 'description'
    VISIBLE = 'visible'
    CONNECTOR_DATA = 'connector_data'
    VAR_NAME = 'var_name'
    # Dict  ex: {'ColumnName': (column_data_type, column_data_range,
    # column_editable)}
    DATAFRAME_DESCRIPTOR = 'dataframe_descriptor'
    DATAFRAME_EDITION_LOCKED = 'dataframe_edition_locked'
    DEFAULT_EXCLUDED_COLUMNS = ['year', 'years']
    IO_TYPE_IN = 'in'
    IO_TYPE_OUT = 'out'

    def __init__(self, sos_name):
        '''
        Constructor.

        Arguments:
            sos_name (string): name of the discipline
        '''
        self.sos_name = sos_name
        self.input_full_name_map = {}
        self.output_full_name_map = {}
        self.input_data_names = []
        self.output_data_names = []
        self.attributes = {}
        self.local_data = {}
        self.jac_dict = {}
        self.jac_boundaries = {}

    def setup_sos_disciplines(self, proxy):  # type: (...) -> None
        """
        Define the set_up_sos_discipline of its proxy

        To be overloaded by subclasses.

        Arguments:
            proxy (ProxyDiscipline): the proxy discipline for dynamic i/o configuration
        """
        pass

    def run(self):  # type: (...) -> None
        """
        Define the run of the discipline

        To be overloaded by subclasses.
        """
        raise NotImplementedError()
    
    def get_sosdisc_inputs(self, keys=None, in_dict=False, full_name_keys=False):
        """
        Accessor for the inputs values as a list or dict.

        Arguments:
            keys (List): the input short or full names list (depending on value of full_name_keys)
            in_dict (bool): if output format is dict
            full_name_keys (bool): if keys in args AND returned dictionary are full names or short names. Note that only
                                   True allows to query for variables of the subprocess as well as of the discipline itself.
        Returns:
            The inputs values list or dict
        """
        if keys is None:
            # if no keys, get all discipline keys and force
            # output format as dict
            if full_name_keys:
                keys = self.input_data_names # discipline and subprocess
            else:
                keys = list(self.attributes['input_full_name_map'].keys()) # discipline only
            in_dict = True
        inputs = self._get_sosdisc_io(
            keys, io_type=self.IO_TYPE_IN, full_name_keys = full_name_keys)
        if in_dict:
            # return inputs in an dictionary
            return inputs
        else:
            # return inputs in an ordered tuple (default)
            if len(inputs) > 1:
                return list(inputs.values())
            else:
                return list(inputs.values())[0]

    def get_sosdisc_outputs(self, keys=None, in_dict=False, full_name_keys = False):
        """
        Accessor for the outputs values as a list or dict.

        Arguments:
            keys (List): the output short or full names list (depending on value of full_name_keys)
            in_dict (bool): if output format is dict
            full_name_keys (bool): if keys in args AND returned dictionary are full names or short names. Note that only
                                   True allows to query for variables of the subprocess as well as of the discipline itself.
        Returns:
            The outputs values list or dict
        """
        if keys is None:
            # if no keys, get all discipline keys and force
            # output format as dict
            if full_name_keys:
                keys = self.output_data_names # discipline and subprocess
            else:
                keys = list(self.attributes['output_full_name_map'].keys()) # discipline only
            in_dict = True
        outputs = self._get_sosdisc_io(
            keys, io_type=self.IO_TYPE_OUT, full_name_keys=full_name_keys)
        if in_dict:
            # return outputs in an dictionary
            return outputs
        else:
            # return outputs in an ordered tuple (default)
            if len(outputs) > 1:
                return list(outputs.values())
            else:
                return list(outputs.values())[0]

    def _get_sosdisc_io(self, keys, io_type, full_name_keys):
        """
        Generic method to retrieve sos inputs and outputs

        Arguments:
            keys (List[String]): the data names list in short or full names (depending on value of full_name_keys)
            io_type (string): IO_TYPE_IN or IO_TYPE_OUT
            full_name_keys: if keys in args and returned dict are full names. Note that only True allows to query for
                            variables of the subprocess as well as of the discipline itself.
        Returns:
            dict of keys values
        Raises:
            ValueError if i_o type is not IO_TYPE_IN or IO_TYPE_OUT
            KeyError if asked for an output key when self.local_data is not initialized
        """

        # convert local key names to namespaced ones
        if isinstance(keys, str):
            keys = [keys]


        if full_name_keys:
            query_keys = keys
        else:
            if io_type == self.IO_TYPE_IN:
                query_keys = [self.attributes['input_full_name_map'][key] for key in keys]
            elif io_type == self.IO_TYPE_OUT:
                query_keys = [self.attributes['output_full_name_map'][key] for key in keys]
            else:
                raise ValueError("Unknown io_type :" +
                                 str(io_type))

        values_dict = dict(zip(keys, map(self.local_data.get, query_keys)))
        return values_dict
    
    def _run(self):
        """
        Run user-defined model.

        Returns:
            local_data (Dict): outputs of the model run
        """
        self.run()
        return self.local_data
    
    def store_sos_outputs_values(self, dict_values, full_name_keys=False):
        """"
        Store run outputs in the local_data attribute.

        NB: permits coherence with EEV3 wrapper run definition.

        Arguments:
            dict_values (Dict): variables' values to store
        """
        if full_name_keys:
            self.local_data.update(dict_values) 
        else:
            outputs = dict(zip(map(self.attributes['output_full_name_map'].get, dict_values.keys()), dict_values.values()))
            self.local_data.update(outputs)

    def get_chart_filter_list(self):
        """ Return a list of ChartFilter instance base on the inherited
        class post processing filtering capabilities

        :return: ChartFilter[]
        """
        return []

    def get_post_processing_list(self, proxy, filters=None):
        """ Return a list of post processing instance using the ChartFilter list given
        as parameter, to be overload in subclasses

        :params: chart_fiters : filter to apply during post processing making
        :type: ChartFilter[]

        :return post processing instance list
        """
        return []

    def set_partial_derivative(self, y_key, x_key, value):
        """
        Method to fill the jacobian dict attribute of the wrapp with a partial derivative (value) given
        a specific input (x_key) and output (y_key). Input and output keys are returned in full name

        @param y_key: String, short name of the output whose derivative is calculated
        @param x_key: String, short name of the input whose derivative is calculated
        @param value: Array, values of the given derivative its dimensions depends on the input/output sizes
        """
        y_key_full = self.attributes['output_full_name_map'][y_key]
        x_key_full = self.attributes['input_full_name_map'][x_key]
        if y_key_full not in self.jac_dict.keys():
            self.jac_dict[y_key_full]={}
        self.jac_dict[y_key_full].update({x_key_full: value})

    def set_partial_derivative_for_other_types(self, y_key_column, x_key_column, value):
        '''
        Set the derivative of the column y_key by the column x_key inside the jacobian of GEMS self.jac
        y_key_column = 'y_key, column_name'
        '''
        if len(y_key_column) == 2:
            y_key, y_column = y_key_column
        else:
            y_key = y_key_column[0]
            y_column = None

        lines_nb_y, index_y_column = self.get_boundary_jac_for_columns(
            y_key, y_column, self.IO_TYPE_OUT)

        if len(x_key_column) == 2:
            x_key, x_column = x_key_column
        else:
            x_key = x_key_column[0]
            x_column = None

        lines_nb_x, index_x_column = self.get_boundary_jac_for_columns(
            x_key, x_column, self.IO_TYPE_IN)

        # Convert keys in namespaced keys in the jacobian matrix for GEMS
        y_key_full = self.attributes['output_full_name_map'][y_key]

        x_key_full = self.attributes['input_full_name_map'][x_key]

        # Code when dataframes are filled line by line in GEMS, we keep the code for now
        #         if index_y_column and index_x_column is not None:
        #             for iy in range(value.shape[0]):
        #                 for ix in range(value.shape[1]):
        #                     self.jac[new_y_key][new_x_key][iy * column_nb_y + index_y_column,
        # ix * column_nb_x + index_x_column] = value[iy, ix]
        if y_key_full not in self.jac_dict.keys():
            self.jac_dict[y_key_full] = {}
        if x_key_full not in self.jac_dict[y_key_full]:
            self.jac_dict[y_key_full][x_key_full] = zeros(self.get_jac_matrix_shape(y_key, x_key))
        if index_y_column is not None and index_x_column is not None:
            self.jac_dict[y_key_full][x_key_full][index_y_column * lines_nb_y:(index_y_column + 1) * lines_nb_y,
            index_x_column * lines_nb_x:(index_x_column + 1) * lines_nb_x] = value
            self.jac_boundaries.update({f'{y_key_full},{y_column}': {'start': index_y_column * lines_nb_y,
                                                                    'end': (index_y_column + 1) * lines_nb_y},
                                        f'{x_key_full},{x_column}': {'start': index_x_column * lines_nb_x,
                                                                    'end': (index_x_column + 1) * lines_nb_x}})

        elif index_y_column is None and index_x_column is not None:
            self.jac_dict[y_key_full][x_key_full][:, index_x_column *
                                              lines_nb_x:(index_x_column + 1) * lines_nb_x] = value

            self.jac_boundaries.update({f'{y_key_full},{y_column}': {'start': 0,
                                                                    'end': -1},
                                        f'{x_key_full},{x_column}': {'start': index_x_column * lines_nb_x,
                                                                    'end': (index_x_column + 1) * lines_nb_x}})
        elif index_y_column is not None and index_x_column is None:
            self.jac_dict[y_key_full][x_key_full][index_y_column * lines_nb_y:(index_y_column + 1) * lines_nb_y,
            :] = value
            self.jac_boundaries.update({f'{y_key_full},{y_column}': {'start': index_y_column * lines_nb_y,
                                                                    'end': (index_y_column + 1) * lines_nb_y},
                                        f'{x_key_full},{x_column}': {'start': 0,
                                                                    'end': -1}})
        else:
            raise Exception(
                'The type of a variable is not yet taken into account in set_partial_derivative_for_other_types')

    def get_jac_matrix_shape(self, y_key, x_key):
        y_value = self.get_sosdisc_outputs(y_key)
        x_value = self.get_sosdisc_inputs(x_key)
        n_out_j = compute_len(y_value)
        n_in_j = compute_len(x_value)
        expected_shape = (n_out_j, n_in_j)

        return expected_shape

    def get_boundary_jac_for_columns(self, key, column, io_type):
        if io_type == self.IO_TYPE_IN:
            #var_full_name = self.attributes['input_full_name_map'][key]
            key_type = self.DESC_IN[key]['type']
            value = self.get_sosdisc_inputs(key)
        if io_type == self.IO_TYPE_OUT:
            #var_full_name = self.attributes['output_full_name_map'][key]
            key_type = self.DESC_OUT[key]['type']
            value = self.get_sosdisc_outputs(key)
        self.DEFAULT_EXCLUDED_COLUMNS = ['year', 'years']
        if key_type == 'dataframe':
            # Get the number of lines and the index of column from the metadata
            lines_nb = len(value)
            index_column = [column for column in value.columns if column not in self.DEFAULT_EXCLUDED_COLUMNS].index(
                column)
        elif key_type == 'array' or key_type == 'float':
            lines_nb = None
            index_column = None
        elif key_type == 'dict':
            dict_keys = list(value.keys())
            lines_nb = len(value[column])
            index_column = dict_keys.index(column)

        return lines_nb, index_column
