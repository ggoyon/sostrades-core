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
from pandas.core.frame import DataFrame

from sos_trades_core.api import get_sos_logger
from sos_trades_core.execution_engine.ns_manager import NS_SEP
from sos_trades_core.execution_engine.sos_coupling import SoSCoupling
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
from sos_trades_core.execution_engine.sos_discipline_builder import SoSDisciplineBuilder


class SoSEvalException(Exception):
    pass


class SoSEval(SoSDisciplineBuilder):
    '''
        SOSEval class which creates a sub process to evaluate
        with different methods (Gradient,FORM,Sensitivity ANalysis, DOE, ...)
    '''

    # ontology information
    _ontology_data = {
        'label': 'Core Eval Model',
        'type': 'Official',
        'source': 'SoSTrades Project',
        'validated': '',
        'validated_by': 'SoSTrades Project',
        'last_modification_date': '',
        'category': '',
        'definition': '',
        'icon': '',
        'version': '',
    }
    DESC_IN = {
        'eval_inputs': {'type': 'string_list', 'unit': None, 'structuring': True},
        'eval_outputs': {'type': 'string_list', 'unit': None, 'structuring': True},
    }

    def __init__(self, sos_name, ee, cls_builder):
        '''
        Constructor
        '''
        super(SoSEval, self).__init__(sos_name, ee)
        self.eval_in_base_list = None
        self.eval_in_list = None
        self.eval_out_base_list = None
        self.eval_out_list = None
        # Needed to reconstruct objects from flatten list
        self.eval_out_type = []
        self.eval_out_list_size = []
        self.logger = get_sos_logger(f'{self.ee.logger.name}.Eval')
        self.cls_builder = cls_builder
        # Create the eval process builder associated to SoSEval
        self.eval_process_builder = self._set_eval_process_builder()

    def _set_eval_process_builder(self):
        '''
        Create the eval process builder, in a coupling if necessary
        '''
        if len(self.cls_builder) > 1 or not self.cls_builder[0]._is_executable:
            # if eval process is a list of builders or a non executable builder,
            # then we build a coupling containing the eval porcess
            disc_builder = self.ee.factory.create_builder_coupling(
                self.sos_name)
            disc_builder.set_builder_info('cls_builder', self.cls_builder)
        else:
            # else return the single builder
            disc_builder = self.cls_builder[0]

        return disc_builder

    def set_eval_in_out_lists(self, in_list, out_list):
        '''
        Set the evaluation variable list (in and out) present in the DM
        which fits with the eval_in_base_list filled in the usecase or by the user
        '''
        self.eval_in_base_list = in_list
        self.eval_out_base_list = out_list
        self.eval_in_list = []
        for v_id in in_list:
            full_id_list = self.dm.get_all_namespaces_from_var_name(v_id)
            for full_id in full_id_list:
                self.eval_in_list.append(full_id)

        self.eval_out_list = []
        for v_id in out_list:
            full_id_list = self.dm.get_all_namespaces_from_var_name(v_id)
            for full_id in full_id_list:
                self.eval_out_list.append(full_id)

    def fill_possible_values(self, disc):
        '''
            Fill possible values lists for eval inputs and outputs
            an input variable must be a float coming from a data_in of a discipline in all the process
            and not a default variable
            an output variable must be any data from a data_out discipline
        '''

        poss_in_values = []
        poss_out_values = []
        for data_in_key in disc._data_in.keys():
            is_float = disc._data_in[data_in_key][self.TYPE] == 'float'
            in_coupling_numerical = data_in_key in list(
                SoSCoupling.DESC_IN.keys())
            full_id = self.dm.get_all_namespaces_from_var_name(data_in_key)[0]
            is_in_type = self.dm.data_dict[self.dm.data_id_map[full_id]]['io_type'] == 'in'
            if is_float and is_in_type and not in_coupling_numerical:
                # Caution ! This won't work for variables with points in name
                # as for ac_model
                poss_in_values.append(data_in_key)
        for data_out_key in disc._data_out.keys():
            # Caution ! This won't work for variables with points in name
            # as for ac_model
            poss_out_values.append(data_out_key.split(NS_SEP)[-1])

        return poss_in_values, poss_out_values

    def build(self):
        '''
        Method copied from SoSCoupling: build and store disciplines in sos_disciplines
        '''
        # set current_discipline to self to build and store eval process in the
        # children of SoSEval
        old_current_discipline = self.ee.factory.current_discipline
        self.ee.factory.current_discipline = self

        # if we want to build an eval coupling containing eval process,
        # we have to remove SoSEval name in current_ns to build eval coupling
        # at the same node as SoSEval
        if self.cls_builder[0] != self.eval_process_builder:
            current_ns = self.ee.ns_manager.current_disc_ns
            self.ee.ns_manager.set_current_disc_ns(
                current_ns.split(f'.{self.sos_name}')[0])
            # build coupling containing eval process
            eval_process_disc = self.eval_process_builder.build()
            # store coupling in the children of SoSEval
            if eval_process_disc not in self.sos_disciplines:
                self.ee.factory.add_discipline(eval_process_disc)
            # reset current_ns after build
            self.ee.ns_manager.set_current_disc_ns(current_ns)
        else:
            # build and store eval process in the children of SoSEval
            eval_process_disc = self.eval_process_builder.build()
            if eval_process_disc not in self.sos_disciplines:
                self.ee.factory.add_discipline(eval_process_disc)

        # If the old_current_discipline is None that means that it is the first build of a coupling then self is the high
        # level coupling and we do not have to restore the current_discipline
        if old_current_discipline is not None:
            self.ee.factory.current_discipline = old_current_discipline

    def get_disciplines_to_configure(self):
        '''
        Method copied from SoSCoupling: get sub disciplines list to configure
        '''
        disc_to_configure = []
        for disc in self.sos_disciplines:
            if not disc.is_configured():
                disc_to_configure.append(disc)
        return disc_to_configure

    def configure(self):
        '''
        Configure the SoSEval and its children sos_disciplines + set eval possible values for the GUI 
        '''
        # configure eval process stored in children
        for disc in self.get_disciplines_to_configure():
            disc.configure()

        if self._data_in == {} or self.get_disciplines_to_configure() == []:
            # Call standard configure methods to set the process discipline
            # tree
            SoSDiscipline.configure(self)

            # Extract variables for eval analysis
            if len(self.sos_disciplines) > 0:
                self.set_eval_possible_values()

    def is_configured(self):
        '''
        Return False if discipline is not configured or structuring variables have changed or children are not all configured
        '''
        return SoSDiscipline.is_configured(self) and (self.get_disciplines_to_configure() == [])

    def set_eval_possible_values(self):
        '''
            Once all disciplines have been run through,
            set the possible values for eval_inputs and eval_outputs in the DM
        '''
        # the eval process to analyse is stored as the only child of SoSEval
        # (coupling chain of the eval process or single discipline)
        analyzed_disc = self.sos_disciplines[0]

        possible_in_values, possible_out_values = self.fill_possible_values(
            analyzed_disc)

        possible_in_values, possible_out_values = self.find_possible_values(
            analyzed_disc, possible_in_values, possible_out_values)

        # Take only unique values in the list
        possible_in_values = list(set(possible_in_values))
        possible_out_values = list(set(possible_out_values))

        # Fill the possible_values of eval_inputs
        self.dm.set_data(f'{self.get_disc_full_name()}.eval_inputs',
                         self.POSSIBLE_VALUES, possible_in_values)
        self.dm.set_data(f'{self.get_disc_full_name()}.eval_outputs',
                         self.POSSIBLE_VALUES, possible_out_values)

    def find_possible_values(
            self, disc, possible_in_values, possible_out_values):
        '''
            Run through all disciplines and sublevels
            to find possible values for eval_inputs and eval_outputs
        '''
        if len(disc.sos_disciplines) != 0:
            for sub_disc in disc.sos_disciplines:
                sub_in_values, sub_out_values = self.fill_possible_values(
                    sub_disc)
                possible_in_values.extend(sub_in_values)
                possible_out_values.extend(sub_out_values)
                self.find_possible_values(
                    sub_disc, possible_in_values, possible_out_values)

        return possible_in_values, possible_out_values

    def get_x0(self):
        '''
        Get initial values for input values decided in the evaluation
        '''
        x0 = []
        for x_id in self.eval_in_list:
            x_val = self.dm.get_value(x_id)
            x0.append(x_val)
        return np.array(x0)

    def FDeval_func(self, x, convert_to_array=True):
        '''
        Call to the function to evaluate with x : values which are modified by the evaluator (only input values with a delta)
        Only these values are modified in the dm. Then the eval_process is executed and output values are convert into arrays. 
        '''
        # -- need to clear cash to avoir GEMS preventing execution when using disciplinary variables
        self.clear_cache()

        values_dict = {}
        for i, x_id in enumerate(self.eval_in_list):
            values_dict[x_id] = x[i]

        # configure eval process with values_dict inputs
        self.ee.load_study_from_input_dict(
            values_dict, update_status_configure=False)

        # execute eval process stored in children
        if len(self.sos_disciplines) > 1:
            # the only child must be a coupling or a single discipline
            raise SoSEvalException(
                f'SoSEval discipline has more than one child discipline')
        else:
            self.sos_disciplines[0].execute()

        if convert_to_array:
            out_values = self.convert_output_results_toarray()
        else:
            out_values = []
            for y_id in self.eval_out_list:
                y_val = self.dm.get_value(y_id)
                out_values.append(y_val)

        return out_values

    def convert_output_results_toarray(self):
        '''
        COnvert toutput results into array in order to apply FDGradient on it for example
        '''
        out_values = []
        self.eval_out_type = []
        self.eval_out_list_size = []
        for y_id in self.eval_out_list:

            y_val = self.dm.get_value(y_id)
            self.eval_out_type.append(type(y_val))
            # Need a flatten list for the eval computation if val is dict
            if type(y_val) in [dict, DataFrame]:
                val_dict = {y_id: y_val}
                dict_flatten = self._convert_new_type_into_array(
                    val_dict)
                y_val = dict_flatten[y_id].tolist()

            else:
                y_val = [y_val]
            self.eval_out_list_size.append(len(y_val))
            out_values.extend(y_val)

        return np.array(out_values)

    def reconstruct_output_results(self, outputs_eval):
        '''
        Reconstruct the metadata saved earlier to get same object in output
        instead of a flatten list
        '''
        outeval_final_dict = {}
        for j, key_in in enumerate(self.eval_in_list):
            outeval_dict = {}
            old_size = 0
            for i, key in enumerate(self.eval_out_list):

                output_eval_key = outputs_eval[old_size:old_size +
                                                        self.eval_out_list_size[i]]
                old_size = self.eval_out_list_size[i]

                if self.eval_out_type[i] in [dict, DataFrame]:
                    outeval_dict[key] = np.array([
                        sublist[j] for sublist in output_eval_key])
                else:
                    outeval_dict[key] = output_eval_key[0][j]

            outeval_dict = self._convert_array_into_new_type(outeval_dict)
            outeval_base_dict = {f'{key_out} vs {key_in}': value for key_out, value in zip(
                self.eval_out_list, outeval_dict.values())}
            outeval_final_dict.update(outeval_base_dict)

        return outeval_final_dict

    def run(self):
        '''
        Overloaded SoSDiscpline method
        '''
        pass
