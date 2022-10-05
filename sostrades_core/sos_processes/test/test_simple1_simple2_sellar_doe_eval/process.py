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
"""
mode: python; py-indent-offset: 4; tab-width: 4; coding: utf-8
Generate a doe scenario
"""
from sostrades_core.sos_processes.base_process_builder import BaseProcessBuilder


class ProcessBuilder(BaseProcessBuilder):
    # ontology information
    _ontology_data = {
        'label': 'sostrades_core.sos_processes.test.test_simple1_simple2_sellar_doe_eval',
        'description': '',
        'category': '',
        'version': '',
    }

    def get_builders(self):
        '''
        default initialisation test
        '''
        # add disciplines Sellar
        disc_dir = 'sostrades_core.sos_wrapping.test_discs.'
        mods_dict_sellar = {'Sellar_Problem': disc_dir + 'sellar.SellarProblem',
                            'Sellar_2': disc_dir + 'sellar.Sellar2',
                            'Sellar_1': disc_dir + 'sellar.Sellar1'}
        builder_list_sellar = self.create_builder_list(mods_dict_sellar,
                                                ns_dict={'ns_OptimSellar': self.ee.study_name,
                                                         'ns_doe_eval': f'{self.ee.study_name}.DoEEval'}
                                                )
        doe_eval_builder = self.ee.factory.create_evaluator_builder(
            'DoEEval', 'doe_eval', builder_list_sellar)

        mods_dict2 = {'Simple_Disc2': disc_dir + 'simple_discs_doe_eval.SimpleDisc2'}
        builder_list2 = self.create_builder_list(mods_dict2,
                                                 ns_dict={'ns_OptimSellar': self.ee.study_name,
                                                         'ns_doe_eval': f'{self.ee.study_name}.DoEEval'}
                                                 )

        mods_dict = {'Simple_Disc1': disc_dir + 'simple_discs_doe_eval.SimpleDisc1'}
        builder_list = self.create_builder_list(mods_dict,
                                                ns_dict={'ns_OptimSellar': self.ee.study_name,
                                                         'doe.DoEEval.sampling_algo': f'{self.ee.study_name}.DoEEval'}
                                                )

        builder_list.append(doe_eval_builder)
        builder_list.append(builder_list2)

        return builder_list
