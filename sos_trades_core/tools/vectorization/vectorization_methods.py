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
Created on 27 july 2022

@author: NG9430A
'''
from logging import Logger

import pandas as pd
from sos_trades_core.execution_engine.sos_discipline import SoSDiscipline

BREAKDOWN_COLUMN = 'PATH'


def get_parent_path(PATH):
    path_list = PATH.split('.')
    path_list.pop()
    if len(path_list) > 0:
        return '.'.join(path_list)
    else:
        return path_list


def merge_df_dict_with_path(df_dict: dict) -> pd.DataFrame:
    """Method to merge a dictionary of dataframe into a single dataframe.
    A new column for the aircraft_breakdown is created and the dictionary key is used as the value for the resulting dataframe

    Args:
        df_dict (dict): dictionary of dataframe. All dataframe must have identical columns

    Returns:
        pd.DataFrame: merged dataframe with a new column BREAKDOWN_COLUMN with the dict key as value
    """
    df_with_path = pd.DataFrame({})
    for key, df in df_dict.items():
        df_copy = df.copy(deep=True)
        df_copy.insert(0, BREAKDOWN_COLUMN, key)
        df_with_path = df_with_path.append(df_copy, ignore_index=True)

    return df_with_path


def compute_parent_path_sum(df, path, based_on, columns_not_to_sum):
    all_columns = df.columns.to_list()
    col_to_sum = [val for val in all_columns if val not in columns_not_to_sum]
    filtered_df = df.loc[df[BREAKDOWN_COLUMN].str.startswith(path)]
    df_with_col_sum = filtered_df.groupby(based_on)[col_to_sum].sum().reset_index()
    df_with_col_not_sum = filtered_df[columns_not_to_sum]
    df_merged = pd.merge(
        df_with_col_sum, df_with_col_not_sum, on=based_on, how='left'
    ).drop_duplicates()
    df_merged[BREAKDOWN_COLUMN] = path
    df_merged = df_merged[all_columns]
    df = df.append(df_merged)
    return df


# check compute sum of val for each possible parent paths except for columns_not_to_sum (with sum based on)
def check_compute_parent_path_sum(df, columns_not_to_sum, based_on):
    path_list = df[BREAKDOWN_COLUMN].unique().tolist()
    for path in path_list:
        path = get_parent_path(path)
        if path in path_list or len(path) == 0:
            pass
        else:
            path_list.append(path)
            df = compute_parent_path_sum(df, path, based_on, columns_not_to_sum)
    df = df.sort_values(["PATH"] + [based_on])
    return df.reset_index(drop=True)


# return input_parameter filtered on PATH parameter
# if parent_path_admissible, return iput_parameter filtered on parent path if existing
def get_inputs_for_path(
    input_parameter: pd.DataFrame,
    PATH: str,
    parent_path_admissible: bool = False,
    unique_value: bool = False,
):
    filtered_input_parameter = None
    if BREAKDOWN_COLUMN in input_parameter:
        while filtered_input_parameter is None and len(PATH) > 0:
            if PATH in input_parameter[BREAKDOWN_COLUMN].unique():
                filtered_input_parameter = input_parameter.loc[
                    input_parameter[BREAKDOWN_COLUMN] == PATH
                ]
            else:
                if parent_path_admissible:
                    PATH = get_parent_path(PATH)
                else:
                    raise Exception(
                        f'the path {PATH} is not found in PATH input_parameter column'
                    )
    else:
        raise Exception(
            f"The column {BREAKDOWN_COLUMN} is not found as an input_parameter column"
        )
    if filtered_input_parameter is None:
        raise Exception("Can not find parent path")
    else:
        filtered_input_parameter = filtered_input_parameter.drop(
            columns=['PATH']
        ).reset_index(drop=True)
        # check if result dataframe is only one column
        if filtered_input_parameter.shape[1] == 1:
            # in that case, return only the column values as a list
            values_list = filtered_input_parameter.iloc[:, 0].values.tolist()
            if unique_value:
                return values_list[0]
            else:
                return values_list
        else:
            return filtered_input_parameter


def check_aircraft_breakdown_in_inputs(
    inputs_dict: dict,
    parameters_dict: dict,
    aircraft_breakdown_tree: list,
    logger: Logger,
    sos_discipline: SoSDiscipline,
):
    aircraft_breakdown_list = flatten(nested_dict=aircraft_breakdown_tree)
    for param_name, conf_dict in parameters_dict.items():
        parameter = inputs_dict.get(param_name, None)
        if parameter is not None:
            if BREAKDOWN_COLUMN in parameter:
                for aircraft_breakdown in aircraft_breakdown_list:
                    if not aircraft_breakdown in parameter[BREAKDOWN_COLUMN].values:
                        logger.warning(
                            f'Aircraft Breakdown {aircraft_breakdown} is missing for parameter {param_name} for discipline {sos_discipline.get_disc_full_name()}'
                        )
            else:
                logger.warning(
                    f'Parameter {param_name} does not have the column {BREAKDOWN_COLUMN}, impossible to check if all values are present for discipline {sos_discipline.get_disc_full_name()}'
                )

        else:
            logger.warning(
                f'Parameter {param_name} is not in input dictionary for discipline {sos_discipline.get_disc_full_name()}'
            )


def flatten(nested_dict, flatten_list=None, parent_key=None):
    if flatten_list is None:
        flatten_list = []
    for key, sub_dict in sorted(nested_dict.items()):
        if parent_key is None:
            full_key = key
        else:
            full_key = f'{parent_key}.{key}'
        flatten_list.append(full_key)
        if sub_dict:
            flatten(sub_dict, flatten_list, parent_key=full_key)
    return flatten_list
