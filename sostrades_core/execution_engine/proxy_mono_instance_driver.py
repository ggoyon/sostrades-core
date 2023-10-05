'''
Copyright (c) 2023 Capgemini

All rights reserved

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer
in the documentation and/or mother materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND OR ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

'''
mode: python; py-indent-offset: 4; tab-width: 8; coding: utf-8
'''
import copy
import pandas as pd
from numpy import NaN
from sostrades_core.execution_engine.sos_wrapp import SoSWrapp
from sostrades_core.execution_engine.proxy_driver_evaluator import ProxyDriverEvaluator


class ProxyMonoInstanceDriverException(Exception):
    pass


class ProxyMonoInstanceDriver(ProxyDriverEvaluator):

    def setup_sos_disciplines(self):
        disc_in = self.get_data_in()
        if disc_in:
            dynamic_inputs = {self.EVAL_INPUTS: {self.TYPE: 'dataframe',
                                                 self.DATAFRAME_DESCRIPTOR: {'selected_input': ('bool', None, True),
                                                                             'full_name': ('string', None, False)},
                                                 self.DATAFRAME_EDITION_LOCKED: False,
                                                 self.STRUCTURING: True,
                                                 self.VISIBILITY: self.SHARED_VISIBILITY,
                                                 self.NAMESPACE: self.NS_EVAL},
                              self.EVAL_OUTPUTS: {self.TYPE: 'dataframe',
                                                  self.DATAFRAME_DESCRIPTOR: {'selected_output': ('bool', None, True),
                                                                              'full_name': ('string', None, False),
                                                                              'output_name': ('multiple', None, True)},
                                                  self.DATAFRAME_EDITION_LOCKED: False,
                                                  self.STRUCTURING: True,
                                                  self.VISIBILITY: self.SHARED_VISIBILITY,
                                                  self.NAMESPACE: self.NS_EVAL},
                              'n_processes': {self.TYPE: 'int', self.NUMERICAL: True, self.DEFAULT: 1},
                              'wait_time_between_fork': {self.TYPE: 'float', self.NUMERICAL: True,
                                                         self.DEFAULT: 0.0}
                              }

            dynamic_outputs = {
                'samples_inputs_df': {self.TYPE: 'dataframe', 'unit': None, self.VISIBILITY: self.SHARED_VISIBILITY,
                                      self.NAMESPACE: self.NS_EVAL}
            }

            selected_inputs_has_changed = False
            if self.EVAL_INPUTS in disc_in:
                # if len(disc_in) != 0:

                eval_outputs = self.get_sosdisc_inputs(self.EVAL_OUTPUTS)
                eval_inputs = self.get_sosdisc_inputs(self.EVAL_INPUTS)

                # we fetch the inputs and outputs selected by the user
                selected_outputs = eval_outputs[eval_outputs['selected_output']
                                                == True]['full_name']
                selected_inputs = eval_inputs[eval_inputs['selected_input']
                                              == True]['full_name']
                if 'output_name' in eval_outputs.columns:
                    eval_out_names = eval_outputs[eval_outputs['selected_output']
                                                  == True]['output_name'].tolist()
                else:
                    eval_out_names = [None for _ in selected_outputs]
                if set(selected_inputs.tolist()) != set(self.selected_inputs):
                    selected_inputs_has_changed = True
                    self.selected_inputs = selected_inputs.tolist()
                self.selected_outputs = selected_outputs.tolist()

                if len(selected_inputs) > 0 and len(selected_outputs) > 0:
                    # TODO: OK that it blocks config. with empty input ? also, might want an eval without outputs ?
                    # we set the lists which will be used by the evaluation function of sosEval
                    self.set_eval_in_out_lists(
                        self.selected_inputs, self.selected_outputs)

                    # setting dynamic outputs. One output of type dict per selected output
                    self.eval_out_names = []
                    for out_var, out_name in zip(self.selected_outputs, eval_out_names):
                        _out_name = out_name or f'{out_var}{self.GATHER_DEFAULT_SUFFIX}'
                        self.eval_out_names.append(_out_name)
                        dynamic_outputs.update(
                            {_out_name: {self.TYPE: 'dict',
                                         self.VISIBILITY: 'Shared',
                                         self.NAMESPACE: self.NS_EVAL}})
                    dynamic_inputs.update(self._get_dynamic_inputs_doe(
                        disc_in, selected_inputs_has_changed))
                    dynamic_outputs.update({'samples_outputs_df': {self.TYPE: 'dataframe',
                                                                   self.VISIBILITY: 'Shared',
                                                                   self.NAMESPACE: self.NS_EVAL}})
            self.add_inputs(dynamic_inputs)
            self.add_outputs(dynamic_outputs)

    def configure_driver(self):
        disc_in = self.get_data_in()
        if self.EVAL_INPUTS in disc_in and len(self.proxy_disciplines) > 0:
            # CHECK USECASE IMPORT AND IMPORT IT IF NEEDED
            # Manage usecase import
            ref_discipline_full_name = f'{self.ee.study_name}.Eval'
            self.manage_import_inputs_from_sub_process(
                ref_discipline_full_name)
            # SET EVAL POSSIBLE VALUES
            self.set_eval_possible_values()

    def set_wrapper_attributes(self, wrapper):
        super().set_wrapper_attributes(wrapper)
        if self.eval_in_list is not None:
            # specific to mono-instance
            eval_attributes = {'eval_in_list': self.eval_in_list,
                               'eval_out_list': self.eval_out_list,
                               'eval_out_names': self.eval_out_names,
                               'reference_scenario': self.get_x0(),
                               'activated_elems_dspace_df': [[True, True]
                                                             if self.ee.dm.get_data(var,
                                                                                    self.TYPE) == 'array' else [
                                   True]
                                                             for var in self.eval_in_list],
                               # NB: this works with an array of dimensions >2 even though it looks incoherent
                               'driver_name': self.get_disc_full_name(),
                               'reduced_dm': self.ee.dm.reduced_dm,  # for conversions
                               'selected_inputs': self.selected_inputs,
                               'selected_outputs': self.selected_outputs,
                               }
            wrapper.attributes.update(eval_attributes)

    def prepare_build(self):
        '''
        Get the builder of the single subprocesses in mono-instance builder mode.
        '''
        if self.get_data_in() and self.eval_process_builder is None:
            self._set_eval_process_builder()

        return [self.eval_process_builder] if self.eval_process_builder else []

    def update_reference(self):
        return bool(self.get_data_in())

    def is_configured(self):
        return super().is_configured() and self.sub_proc_import_usecase_status == 'No_SP_UC_Import'

    def get_x0(self):
        '''
        Get initial values for input values decided in the evaluation
        '''

        return dict(zip(self.eval_in_list,
                        map(self.dm.get_value, self.eval_in_list)))

    def _get_dynamic_inputs_doe(self, disc_in, selected_inputs_has_changed):
        default_custom_dataframe = pd.DataFrame(
            [[NaN for _ in range(len(self.selected_inputs))]], columns=self.selected_inputs)
        dataframe_descriptor = {}
        for i, key in enumerate(self.selected_inputs):
            var_f_name = self.eval_in_list[i]
            if var_f_name in self.ee.dm.data_id_map:
                var = tuple([self.ee.dm.get_data(
                    var_f_name, self.TYPE), None, True])
                dataframe_descriptor[key] = var
            elif self.MULTIPLIER_PARTICULE in var_f_name:
                # for multipliers assume it is a float
                dataframe_descriptor[key] = ('float', None, True)
            else:
                raise KeyError(f'Selected input {var_f_name} is not in the Data Manager')

        dynamic_inputs = {'samples_df': {self.TYPE: 'dataframe', self.DEFAULT: default_custom_dataframe,
                                         self.DATAFRAME_DESCRIPTOR: dataframe_descriptor,
                                         self.DATAFRAME_EDITION_LOCKED: False,
                                         self.VISIBILITY: SoSWrapp.SHARED_VISIBILITY,
                                         self.NAMESPACE: self.NS_EVAL
                                         }}

        # This reflects 'samples_df' dynamic input has been configured and that
        # eval_inputs have changed
        if 'samples_df' in disc_in and selected_inputs_has_changed:

            if disc_in['samples_df']['value'] is not None:
                from_samples = list(disc_in['samples_df']['value'].keys())
                from_eval_inputs = list(default_custom_dataframe.keys())
                final_dataframe = pd.DataFrame(
                    None, columns=self.selected_inputs)

                len_df = 1
                for element in from_eval_inputs:
                    if element in from_samples:
                        len_df = len(disc_in['samples_df']['value'])

                for element in from_eval_inputs:
                    if element in from_samples:
                        final_dataframe[element] = disc_in['samples_df']['value'][element]

                    else:
                        final_dataframe[element] = [NaN for _ in range(len_df)]

                disc_in['samples_df'][self.VALUE] = final_dataframe
            disc_in['samples_df'][self.DATAFRAME_DESCRIPTOR] = dataframe_descriptor
        return dynamic_inputs

    def _set_eval_process_builder(self):
        '''
        Create the eval process builder, in a coupling if necessary, which will allow mono-instance builds.
        '''
        updated_ns_list = self.update_sub_builders_namespaces()
        if len(self.cls_builder) == 0:  # added condition for proc build
            disc_builder = None
        elif len(self.cls_builder) == 1:
            # Note no distinction is made whether the builder is executable or not; old implementation used to put
            # scatter builds under a coupling automatically too.
            disc_builder = self.cls_builder[0]
        else:
            # If eval process is a list of builders then we build a coupling
            # containing the eval process
            if self.flatten_subprocess:
                disc_builder = None
            else:
                disc_builder = self.create_sub_builder_coupling(
                    self.SUBCOUPLING_NAME, self.cls_builder)
                self.hide_coupling_in_driver_for_display(disc_builder)

        self.eval_process_builder = disc_builder

        self.eval_process_builder.add_namespace_list_in_associated_namespaces(
            updated_ns_list)

    def update_sub_builders_namespaces(self):
        '''
        Update sub builders namespaces with the driver name in monoinstance case
        '''

        ns_ids_list = []
        extra_name = f'{self.sos_name}'
        after_name = self.father_executor.get_disc_full_name()

        for ns_name in self.sub_builder_namespaces:
            old_ns = self.ee.ns_manager.get_ns_in_shared_ns_dict(ns_name)
            updated_value = self.ee.ns_manager.update_ns_value_with_extra_ns(
                old_ns.get_value(), extra_name, after_name=after_name)
            display_value = old_ns.get_display_value_if_exists()
            ns_id = self.ee.ns_manager.add_ns(
                ns_name, updated_value, display_value=display_value, add_in_shared_ns_dict=False)
            ns_ids_list.append(ns_id)

        return ns_ids_list

    def hide_coupling_in_driver_for_display(self, disc_builder):
        '''
        Set the display_value of the sub coupling to the display_value of the driver
        (if no display_value filled the display_value is the simulation value)
        '''
        driver_display_value = self.ee.ns_manager.get_local_namespace(
            self).get_display_value()
        self.ee.ns_manager.add_display_ns_to_builder(
            disc_builder, driver_display_value)

    def set_eval_in_out_lists(self, in_list, out_list, inside_evaluator=False):
        '''
        Set the evaluation variable list (in and out) present in the DM
        which fits with the eval_in_base_list filled in the usecase or by the user
        '''

        # final_in_list, final_out_list = self.remove_pseudo_variables(in_list, out_list) # only actual subprocess variables
        if in_list is not None:
            self.eval_in_list = [
                f'{self.get_disc_full_name()}.{element}' for element in in_list]
        self.eval_out_list = [
            f'{self.get_disc_full_name()}.{element}' for element in out_list]

    # def remove_pseudo_variables(self, in_list, out_list):
    #     # and add the real variables that will be used as reference variables or MorphMatrix combinations
    #     # in this case managing only multiplier particles
    #     new_in_list = []
    #     for element in in_list:
    #         if self.MULTIPLIER_PARTICULE in element:
    #             if '@' in element:
    #                 new_in_list.append(element.rsplit('@', 1)[0])
    #             else:
    #                 new_in_list.append(element.rsplit(self.MULTIPLIER_PARTICULE, 1)[0])
    #         else:
    #             new_in_list.append(element)
    #     return new_in_list, out_list

