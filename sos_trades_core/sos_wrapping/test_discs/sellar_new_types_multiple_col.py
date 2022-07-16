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
# -*-mode: python; py-indent-offset: 4; tab-width: 8; coding:utf-8 -*-
'''
Implementation of Sellar Disciplines (Sellar, 1996)
Adapted from GEMSEO examples
'''

from cmath import exp, sqrt
import numpy as np
import pandas as pd
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
from sos_trades_core.execution_engine.sos_coupling import SoSCoupling
from sos_trades_core.execution_engine.execution_engine import ExecutionEngine


class SellarProblem(SoSDiscipline):
    """ Sellar Optimization Problem functions
    """
    _maturity = 'Fake'
    DESC_IN = {
        'x': {'type': 'dict', 'subtype_descriptor': {'dict': 'array'}, 'visibility': SoSDiscipline.SHARED_VISIBILITY,
              'namespace': 'ns_OptimSellar'},
        'y_1': {'type': 'dataframe', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
        'y_2': {'type': 'dataframe', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
        'z': {'type': 'array', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
        'local_dv': {'type': 'float'}}

    DESC_OUT = {'c_1': {'type': 'array', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
                'c_2': {'type': 'dataframe', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
                'obj': {'type': 'array', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'}}

    def run(self):
        """ computes
        """
        x, y_1, y_2, z = self.get_sosdisc_inputs(['x', 'y_1', 'y_2', 'z'])
        local_dv = self.get_sosdisc_inputs('local_dv')

        obj = self.obj(x, z, y_1, y_2)
        c_1 = self.c_1(y_1)
        c_2 = self.c_2(y_1, y_2)
        obj += local_dv
        out = {'obj': obj, 'c_1': c_1, 'c_2': c_2}
        self.store_sos_outputs_values(out)

    @staticmethod
    def obj(x, z, y_1, y_2):
        """Objective function

        :param x: local design variables
        :type x: numpy.array
        :param z: shared design variables
        :type z: numpy.array
        :param y_1: coupling variable from discipline 1
        :type y_1: numpy.array
        :param y_2: coupling variable from discipline 2
        :type y_2: numpy.array
        :returns: Objective value
        :rtype: float
        """
        out = x['value'][0] ** 2 + z[1] + \
              y_1['value_a'].values[0] + y_1['value_b'].values[0] * 0.2 +\
              exp(-y_2['value'].values[0])
        return np.array([out])

    @staticmethod
    def c_1(y_1):
        """First constraint on system level

        :param y_1: coupling variable from discipline 1
        :type y_1: numpy.array
        :returns: Value of the constraint 1
        :rtype: float
        """
        return np.array([3.16 - y_1['value_a'].values[0]])

    @staticmethod
    def c_2(y_1, y_2):
        """Second constraint on system level

        :param y_2: coupling variable from discipline 2
        :type y_2: numpy.array
        :returns: Value of the constraint 2
        :rtype: float
        """
        return pd.DataFrame({'value_a': y_2['value'].values[0] - 24. + y_1['value_a'].values[0]*5.3,
                             'value_b': 0.01*(y_2['value'].values[0]+6) - y_1['value_b'].values[0]*2.1}, index=[0])


    def compute_sos_jacobian(self):
        """

        :param inputs: linearization should be performed with respect
            to inputs list. If None, linearization should
            be performed wrt all inputs (Default value = None)
        :param outputs: linearization should be performed on outputs list.
            If None, linearization should be performed
            on all outputs (Default value = None)
        """
        # Initialize all matrices to zeros

        x, y_2 = self.get_sosdisc_inputs(['x', 'y_2'])

        self.set_partial_derivative_for_other_types(
            ('c_1',), ('y_1', 'value_a'), [- 1.0, 0.0, 0.0, 0.0])

        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_a'), ('y_1', 'value_a'), [5.3, 0.0, 0.0, 0.0])
        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_b'), ('y_1', 'value_a'), [0.0, 0.0, 0.0, 0.0])
        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_a'), ('y_1', 'value_b'), [0.0, 0.0, 0.0, 0.0])
        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_b'), ('y_1', 'value_b'), [-2.1, 0.0, 0.0, 0.0])
        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_a'), ('y_2', 'value'), [1.0, 0.0, 0.0, 0.0])
        self.set_partial_derivative_for_other_types(
            ('c_2', 'value_b'), ('y_2', 'value'), [0.01, 0.0, 0.0, 0.0])

        self.set_partial_derivative_for_other_types(('obj',), ('x', 'value'), np.array([
            2.0 * x['value'][0], 0.0, 0.0, 0.0]))

        self.set_partial_derivative('obj', 'z', np.atleast_2d(np.array(
            [0.0, 1.0])))
        self.set_partial_derivative_for_other_types(
            ('obj',), ('y_1', 'value_a'), np.array([1.0, 0.0, 0.0, 0.0]))
        self.set_partial_derivative_for_other_types(
            ('obj',), ('y_1', 'value_b'), np.array([0.2, 0.0, 0.0, 0.0]))

        self.set_partial_derivative_for_other_types(
            ('obj',), ('y_2', 'value'), np.array([-exp(-y_2.iloc[0]['value']), 0.0, 0.0, 0.0]))

        self.set_partial_derivative('obj', 'local_dv', np.atleast_2d(np.array(
            [1.0])))


class Sellar1Df(SoSDiscipline):
    """ Discipline 1
    """
    _maturity = 'Fake'
    DESC_IN = {'x': {'type': 'dict', 'subtype_descriptor': {'dict': 'array'}, 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
               'y_2': {'type': 'dataframe', 'visibility': SoSDiscipline.SHARED_VISIBILITY,
                       'namespace': 'ns_OptimSellar'},
               'z': {'type': 'array', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'}}

    DESC_OUT = {'y_1': {'type': 'dataframe',
                        'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'}}

    def run(self):
        """ Discipline 1 execution
        """
        x, y_2, z = self.get_sosdisc_inputs(['x', 'y_2', 'z'])
        y_1 = self.compute_y_1(x, y_2, z)
        y1_out = {'y_1': y_1}
        self.store_sos_outputs_values(y1_out)

    @staticmethod
    def compute_y_1(x, y_2, z):
        """Solve the first coupling equation in functional form.

        :param x: vector of design variables local to discipline 1
        :type x: numpy.array
        :param z: vector of shared design variables
        :type z: numpy.array
        :param y_2: coupling variable of discipline 2
        :type y_2: numpy.array
        :returns: coupling variable y_1 of discipline 1
        :rtype: float
        """
        out = pd.DataFrame({'years': np.arange(1, 5), 'value_a': 0.0, 'value_b': 0.0})
        i = 0
        y_2['years'] = y_2['years'].astype('int64')
        for year in out['years']:
            out.loc[out['years'] == year, 'value_a'] = z[0] ** 2 + x['value'][i] + z[1] - 0.2 * \
                                                     y_2.loc[y_2['years'] == year,
                                                             'value'].values[0]
            out.loc[out['years'] == year, 'value_b'] = x['value'][i]*0.12 + z[1]/2
            i += 1
        return out

    def compute_sos_jacobian(self):
        """

        :param inputs: linearization should be performed with respect
            to inputs list. If None, linearization should
            be performed wrt all inputs (Default value = None)
        :param outputs: linearization should be performed on outputs list.
            If None, linearization should be performed
            on all outputs (Default value = None)
        """

        z = self.get_sosdisc_inputs('z')

        lines_nb = len(np.arange(1, 5))
        self.set_partial_derivative_for_other_types(
            ('y_1', 'value_a'), ('x', 'value'), np.identity(lines_nb))

        self.set_partial_derivative_for_other_types(
            ('y_1', 'value_a'), ('z',), [[2.0 * z[0], 1.0] for i in range(lines_nb)])

        self.set_partial_derivative_for_other_types(
            ('y_1', 'value_a'), ('y_2', 'value'), -0.2 * np.identity(lines_nb))

        self.set_partial_derivative_for_other_types(
            ('y_1', 'value_b'), ('x', 'value'), np.identity(lines_nb)*0.12)

        self.set_partial_derivative_for_other_types(
            ('y_1', 'value_b'), ('z',), [[0.0, 1.0/2.0] for i in range(lines_nb)])


class Sellar2Df(SoSDiscipline):
    """ Discipline 2
    """

    # ontology information
    _ontology_data = {
        'label': 'sos_trades_core.sos_wrapping.test_discs.sellar_new_types',
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
    _maturity = 'Fake'
    DESC_IN = {
        'y_1': {'type': 'dataframe', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'},
        'z': {'type': 'array', 'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'}}

    DESC_OUT = {'y_2': {'type': 'dataframe',
                        'visibility': SoSDiscipline.SHARED_VISIBILITY, 'namespace': 'ns_OptimSellar'}}

    def run(self):
        """ solves Discipline1
        """
        y_1, z = self.get_sosdisc_inputs(['y_1', 'z'])
        y_2 = self.compute_y_2(y_1, z)
        y1_out = {'y_2': y_2}
        self.store_sos_outputs_values(y1_out)

    @staticmethod
    def compute_y_2(y_1, z):
        """Solve the second coupling equation in functional form.

        :param z: vector of shared design variables
        :type z: numpy.array
        :param y_1: coupling variable of discipline 1
        :type y_1: numpy.array
        :returns: coupling variable y_2
        :rtype: float
        """

        out = pd.DataFrame({'years': np.arange(1, 5), 'value': 0.0})
        y_1['years'] = y_1['years'].astype('int64')
        for year in out['years']:
            out.loc[out['years'] == year, 'value'] = z[0] + z[1] + sqrt(y_1.loc[y_1['years'] == year,
                                                                                'value_a'].values[0])
        out['value']=out['value'].astype(y_1['value_a'].dtype)
        return out

    def compute_sos_jacobian(self):
        y_1 = self.get_sosdisc_inputs('y_1')

        lines_nb = len(np.arange(1, 5))
        self.set_partial_derivative_for_other_types(('y_2', 'value'), ('y_1', 'value_a'),
                                                    1.0 / (2 * sqrt(y_1.iloc[0]['value_a'])) * np.identity(lines_nb))

        self.set_partial_derivative_for_other_types(('y_2', 'value'), ('z',), [
            [1.0, 1.0] for i in range(lines_nb)])


if __name__ == '__main__':
    disc_id = 'coupling_disc'
    namespace = "test"
    ee = ExecutionEngine("test")
