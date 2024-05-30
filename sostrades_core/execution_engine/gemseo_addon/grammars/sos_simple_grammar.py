'''
Copyright 2022 Airbus SAS
Modifications on 2023/05/12-2024/05/16 Copyright 2023 Capgemini

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

import logging
from typing import ClassVar

from gemseo.core.grammars.simpler_grammar import SimplerGrammar
from gemseo.core.grammars.required_names import RequiredNames

"""Most basic grammar implementation."""

LOGGER = logging.getLogger("gemseo.addons.grammars.sos_simple_grammar")


class SoSSimpleGrammar(SimplerGrammar):
    """Store the names and types of the elements as Python lists.

    Attributes:
        data_names (List[str]): The names of the elements.
        data_types (List[type]): The types of the elements,
            stored in the same order as ``data_names``.
    """
    DATA_CONVERTER_CLASS: ClassVar[str] = "SoSTradesDataConverter"

    # def load_data(
    #         self,
    #         data,  # type: Mapping[str,Any]
    #         raise_exception=True,  # type: bool
    # ):  # type: (...) -> Mapping[str,Any]
    #     #         self.check(data, raise_exception)
    #     return data
    #
    # def set_item_value(self, item_name, item_value):
    #     """
    #     Sets the value of an item
    #
    #     :param item_name: the item name to be modified
    #     :param item_value: value of the item
    #     """
    #     if not self.is_data_name_existing(item_name):
    #         raise ValueError("Item " + str(item_name) + " not in grammar " +
    #                          self.name)
    #     self._update_field(item_name, item_value['type'])
    def validate(self, data,
                 raise_exception=True):
        pass

    @property
    def defaults(self):
        """The mapping from the names to the default values, if any."""
        return self._defaults

    @defaults.setter
    def defaults(self, data) -> None:
        self._defaults = data

    def clear(self) -> None:
        """Empty the grammar."""
        self.to_namespaced = {}
        self.from_namespaced = {}
        self._defaults = {}
        self._required_names = RequiredNames(self)
        self._clear()
