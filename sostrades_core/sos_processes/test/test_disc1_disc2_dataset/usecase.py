'''
Copyright 2023 Capgemini

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
# mode: python; py-indent-offset: 4; tab-width: 8; coding:utf-8
from sostrades_core.study_manager.study_manager import StudyManager
from sostrades_core.tools.post_processing.post_processing_factory import PostProcessingFactory
import time


class Study(StudyManager):

    def __init__(self, execution_engine=None):
        super().__init__(__file__, execution_engine=execution_engine)

    def setup_usecase(self):

        dict_values = {
            f'{self.study_name}.a': 1,
            f'{self.study_name}.Disc1.b': "StringInputDisc1",
            f'{self.study_name}.Disc2.b': "StringInputDisc2",
            f'{self.study_name}.Disc1.c': "CCCCCC11111",
            f'{self.study_name}.Disc2.c': "CCCCCC222222",
            f'{self.study_name}.Disc1VirtualNode.x': 4.,
            f'{self.study_name}.Disc2VirtualNode.x': 5.,
            }
        return dict_values


if '__main__' == __name__:
    start = time.time()
    uc_cls = Study()
    uc_cls.load_data()
    uc_cls.run(for_test=True)
    stop = time.time()
    print(stop - start)



    ppf = PostProcessingFactory()
    # for disc in uc_cls.execution_engine.root_process.proxy_disciplines:
    #         filters = ppf.get_post_processing_filters_by_discipline(
    #             disc)
    #         graph_list = ppf.get_post_processing_by_discipline(
    #             disc, filters, as_json=False)

    #         for graph in graph_list:
    #             graph.to_plotly().show()        
    
    print(uc_cls.execution_engine.dm.get_value(f'{uc_cls.study_name}.Disc2.flag'))
    print(uc_cls.execution_engine.dm.get_value(f'{uc_cls.study_name}.a'))