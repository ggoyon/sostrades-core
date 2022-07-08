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
"""
from json import dumps
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline
from os.path import dirname, isdir, isfile, join
import inspect
import os
import base64
import re
from os import listdir


class TreeNode:
    """
    Class to build tree node from data manager
    """
    STATUS_INPUT_DATA = 'INPUT_DATA'
    MARKDOWN_NAME_KEY = 'name'
    MARKDOWN_DOCUMENTATION_KEY = 'documentation'

    needed_variables = [SoSDiscipline.TYPE, SoSDiscipline.USER_LEVEL, SoSDiscipline.EDITABLE,
                        SoSDiscipline.COUPLING, SoSDiscipline.VALUE, SoSDiscipline.NUMERICAL, SoSDiscipline.OPTIONAL]

    def __init__(self, name):
        """ class constructor
        """
        self.name = name

        # Children treenode: TreeNode[]
        self.children = []

        # Data coming from DataManager.data_dict for the associated namespace
        self.data = {}  # current node data

        # Store the discipline type
        self.node_type = 'data'

        # Treenode status (bijective with associated discipline if exist)
        self.status = TreeNode.STATUS_INPUT_DATA

        # Namespace associated to the disciplines
        self.full_namespace = ''

        # Disciplines indentifier (execution engine side => uuid)
        self.identifier = ''

        # Lisf of discipline identifier associated to this TreeNode
        self.disc_ids = []

        # Dict with addition of maturity for each discipline on this TreeNode
        self.multi_discipline_maturity = {}

        # Disciplines maturity (determined using the discipline maturity)
        self.maturity = ''

        # Data at discipline level (not namespace level => cf. self.data
        self.disc_data = {}  # to be able to show all variable at each discipline level

        # Treenode documentation (markdown format)
        self.markdown_documentation = []

        # List of models present at current treenode
        self.models_full_path_list = []

        self.model_name = None
        self.model_name_full_path = None
        self.last_treenode = None

    def to_json(self):
        dict_obj = self.to_dict()
        return dumps(dict_obj)

    def to_dict(self):
        dict_obj = {}
        # Serialize name attribute
        dict_obj.update({'name': self.name})

        # Serialize data attribute
        dict_obj.update({'data': self.data})

        # Serialize node_type attribute
        dict_obj.update({'node_type': self.node_type})

        # Serialize status attribute
        dict_obj.update({'status': self.status})

        # Serialize full_namespace attribute
        dict_obj.update({'full_namespace': self.full_namespace})

        # Serialize identifier attribute
        dict_obj.update({'identifier': self.identifier})

        # Serialize maturity attribute
        dict_obj.update({'maturity': self.maturity})

        # Serialize disc_data attribute
        dict_obj.update({'disc_data': self.disc_data})

        # Serialize model_name_full_path attribute
        dict_obj.update({'model_name_full_path': self.model_name_full_path})

        # Serialize models_full_path_list attribute
        dict_obj.update({'models_full_path_list': self.models_full_path_list})

        # Serialize markdown_documentation
        dict_obj.update(
            {'markdown_documentation': self.markdown_documentation})

        # Serialize children attribute
        dict_child = []
        for tn in self.children:
            dict_child.append(tn.to_dict())
        dict_obj.update({'children': dict_child})
        return dict_obj

    def update_treenode_attributes(self, discipline, no_data=False, read_only=False):
        """ Inject discipline data into the current treenode

        :params: discipline to set into the treenode
        :type: SoSDiscipline
        """
        self.full_namespace = discipline.get_disc_full_name()
        self.identifier = discipline.disc_id
        self.disc_ids.append(self.identifier)
        self.node_type = discipline.__class__.__name__
        self.model_name_full_path = discipline.__module__
        self.models_full_path_list.append(discipline.__module__)

        if self.node_type != 'SoSCoupling':
            self.model_name = discipline.__module__.split('.')[-2]
        # Some modification has to be done on variable:
        # identifier : variable namespace + variable name
        # I/O type : 'in' for data_in and 'out' for data_out
        data_in = discipline.get_data_in()
        if not no_data:
            for key, data_key in data_in.items():
                namespaced_key = discipline.get_var_full_name(
                    key, data_in)
                new_disc_data = {
                    needed_key: data_key[needed_key] for needed_key in self.needed_variables}
                new_disc_data[SoSDiscipline.IO_TYPE] = SoSDiscipline.IO_TYPE_IN
                if read_only:
                    new_disc_data[SoSDiscipline.EDITABLE] = False
                self.update_disc_data(
                    new_disc_data, namespaced_key, discipline)

        data_out = discipline.get_data_out()
        if not no_data:
            for key, data_key in data_out.items():
                namespaced_key = discipline.get_var_full_name(
                    key, data_out)
                new_disc_data = {
                    needed_key: data_key[needed_key] for needed_key in self.needed_variables}
                new_disc_data[SoSDiscipline.IO_TYPE] = SoSDiscipline.IO_TYPE_OUT
                if read_only:
                    new_disc_data[SoSDiscipline.EDITABLE] = False
                self.update_disc_data(
                    new_disc_data, namespaced_key, discipline)

        self.__manage_status(discipline.status)

        # Convert maturity dictionary to string for display purpose
        # Update multi_discipline_maturity with current discipline maturity
        o_mat = discipline.get_maturity()
        if isinstance(o_mat, dict):
            for k, v in o_mat.items():
                if k in self.multi_discipline_maturity:
                    self.multi_discipline_maturity[k] += v
                else:
                    self.multi_discipline_maturity[k] = v
        else:
            if len(o_mat) > 0:
                if o_mat in self.multi_discipline_maturity:
                    self.multi_discipline_maturity[o_mat] += 1
                else:
                    self.multi_discipline_maturity[o_mat] = 1

        if len(self.multi_discipline_maturity.items()) > 0:
            l_mat = [
                f'{k}({v})' for k, v in self.multi_discipline_maturity.items() if v != 0]
            s_mat = ' '.join(l_mat)
        else:
            s_mat = ''
        self.maturity = s_mat

        # Manage markdown documentation
        filepath = inspect.getfile(discipline.__class__)
        markdown_data = TreeNode.get_markdown_documentation(filepath)
        self.add_markdown_documentation(markdown_data, discipline.__module__)

    def update_disc_data(self, new_disc_data, namespace, discipline):
        """ Set variable from discipline into treenode disc_data
        :params: new_disc_data, variable data
        :type: SoSDiscipline variable data_dict

        :params: namespace, namespace of the variable
        :type: string

        :params: discipline to set into the treenode
        :type: SoSDiscipline
        """
        if namespace not in self.disc_data:
            self.disc_data[namespace] = new_disc_data
            self.disc_data[namespace][SoSDiscipline.DISCIPLINES_FULL_PATH_LIST] = [
                discipline.__module__]
        else:
            for key, value in new_disc_data.items():
                self.disc_data[namespace][key] = value
            if discipline.__module__ not in self.disc_data[namespace][SoSDiscipline.DISCIPLINES_FULL_PATH_LIST]:
                self.disc_data[namespace][SoSDiscipline.DISCIPLINES_FULL_PATH_LIST].append(
                    discipline.__module__)

    def add_markdown_documentation(self, markdown_data, key):
        """ Add a markdon documentation to the treenode

        :params: markdown_data, markdown documenation to set
        :type: str

        :params: key, associated key (used to manage multiple documentation into the same treenode
        :type: key 
        """

        if markdown_data is not None and markdown_data != "":
            self.markdown_documentation.append({
                TreeNode.MARKDOWN_NAME_KEY: key,
                TreeNode.MARKDOWN_DOCUMENTATION_KEY: markdown_data
            })

    @staticmethod
    def get_markdown_documentation(filepath):
        # Manage markdown documentation

        doc_folder_path = join(dirname(filepath), 'documentation')
        filename = os.path.basename(filepath).split('.')[0]
        markdown_data = ""
        if isdir(doc_folder_path):
            # look for markdown file with extension .markdown or .md
            markdown_list = [join(doc_folder_path, md_file) for md_file in listdir(doc_folder_path) if ((
                md_file.endswith(r".markdown") or md_file.endswith(r".md")) and md_file.startswith(filename))]

            if len(markdown_list) > 0:
                # build file path
                markdown_filepath = markdown_list[0]

                if isfile(markdown_filepath):
                    markdown_data = ''

                    with open(markdown_filepath, 'r+t', encoding='utf-8') as f:
                        markdown_data = f.read()

                    # Find file reference in markdown file
                    place_holder = f'!\\[(.*)\\]\\((.*)\\)'
                    matches = re.finditer(place_holder, markdown_data)

                    images_base_64 = {}
                    base64_image_tags = []

                    for matche in matches:
                        # Format:
                        # (0) => full matche line
                        # (1) => first group (place holder name)
                        # (2) => second group (image path/name)

                        image_name = matche.group(2)

                        # Convert markdown image link to link to base64 image
                        image_filepath = join(doc_folder_path, image_name)

                        if isfile(image_filepath):
                            image_data = open(image_filepath, 'r+b').read()
                            encoded = base64.b64encode(
                                image_data).decode('utf-8')

                            images_base_64.update({image_name: encoded})

                            # first replace the matches
                            matche_value = matche.group(1)
                            matches_replace = f'![{matche_value}]({image_name})'
                            matches_replace_by = f'![{matche_value}][{image_name}]'

                            base64_image_tag = f'[{image_name}]:data:image/png;base64,{images_base_64[image_name]}'
                            base64_image_tags.append(base64_image_tag)

                            markdown_data = markdown_data.replace(
                                matches_replace, matches_replace_by)

                    for image_tag in base64_image_tags:
                        markdown_data = f'{markdown_data}\n\n{image_tag}'

        return markdown_data

    def __str__(self):
        children_str = ''.join([str(c) for c in self.children])
        return f'name : {self.name}, data : {self.data}, node_type : {self.node_type}, status : {self.status}, maturity: {self.maturity}\nchildren : {children_str}'

    def __manage_status(self, new_status):
        """ Each treenode can have multiple discipline
            for each new discipline, we compare the new status with the old one to choose
            which status is the most prioritary
        """
        current_status_priority = TreeNode.status_to_integer(self.status)
        new_status_priority = TreeNode.status_to_integer(new_status)

        if new_status_priority > current_status_priority:
            self.status = new_status

    @staticmethod
    def status_to_integer(status):
        """ Convert status to integer value to make some sort with status

            :params: status, disciplines status name
            :type: string

            :return: integer
        """

        if status == TreeNode.STATUS_INPUT_DATA:
            return -1
        elif status == SoSDiscipline.STATUS_VIRTUAL:
            return 0
        elif status == SoSDiscipline.STATUS_CONFIGURE:
            return 10
        elif status == SoSDiscipline.STATUS_DONE:
            return 20
        elif status == SoSDiscipline.STATUS_PENDING:
            return 30
        elif status == SoSDiscipline.STATUS_RUNNING:
            return 40
        else:  # status = SoSDiscipline.STATUS_FAILED
            return 50
