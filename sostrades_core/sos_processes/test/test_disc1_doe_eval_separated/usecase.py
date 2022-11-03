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
import pandas as pd
from numpy import array

from sostrades_core.study_manager.study_manager import StudyManager


class Study(StudyManager):

    def __init__(self, run_usecase=False, execution_engine=None):
        super().__init__(__file__, run_usecase=run_usecase, execution_engine=execution_engine)

    def setup_usecase(self):
        """
        Usecase for lhs DoE and Eval on x variable of Sellar Problem
        """

        ns = f'{self.study_name}'
        dspace_dict = {'variable': ['Eval.Disc1.a'],

                         'lower_bnd': [0.],
                         'upper_bnd': [1.],

                         }
        dspace = pd.DataFrame(dspace_dict)

        input_selection_a = {'selected_input': [False, True, False],
                             'full_name': ['x', 'Eval.Disc1.a', 'Eval.Disc1.b']}
        input_selection_a = pd.DataFrame(input_selection_a)

        output_selection_ind = {'selected_output': [False, True],
                                'full_name': ['y', 'Eval.Disc1.indicator']}
        output_selection_ind = pd.DataFrame(output_selection_ind)

        disc_dict = {}
        # DoE inputs
        n_samples = 10
        levels = [0.25, 0.5, 0.75]
        centers = [5]
        disc_dict[f'{ns}.DoE.sampling_algo'] = 'OT_FACTORIAL'
        disc_dict[f'{ns}.DoE.design_space'] = dspace
        disc_dict[f'{ns}.DoE.algo_options'] = {'n_samples': n_samples, 'levels': levels, 'centers': centers}
        disc_dict[f'{ns}.eval_inputs'] = input_selection_a
        disc_dict[f'{ns}.eval_outputs'] = output_selection_ind

        # Disc1 inputs
        disc_dict[f'{ns}.x'] = array([10.])
        disc_dict[f'{ns}.Eval.Disc1.a'] = array([0.5])
        disc_dict[f'{ns}.Eval.Disc1.b'] = array([25.])
        disc_dict[f'{ns}.y'] = array([4.])
        disc_dict[f'{ns}.Eval.Disc1.indicator'] = array([53.])

        return [disc_dict]


if '__main__' == __name__:
    uc_cls = Study(run_usecase=True)
    uc_cls.load_data()
    uc_cls.run()
