#  Copyright 2022-2023 The FormS Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import pandas as pd

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode
from forms.executor.dfexecutor.utils import (
    get_execution_node_n_formula,
    construct_df_table,
    get_single_value,
)
from forms.executor.dfexecutor.lookup.utils.utils import (
    clean_string_values,
    get_df,
    get_literal_value
)
from forms.executor.dfexecutor.lookup.api import vlookup


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, df, col_idxes, approx = get_vlookup_params(physical_subtree)
    result_df = vlookup(values, df, col_idxes, approx=approx)
    return construct_df_table(result_df)


# Retrives parameters for VLOOKUP.
def get_vlookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size).iloc[:, 0])
    df: pd.DataFrame = get_df(children[1])
    col_idxes: pd.DataFrame = get_literal_value(children[2], size).iloc[:, 0].astype(int)
    if len(children) == 4:
        approx = get_single_value(children[3]) != 0

    return values, df, col_idxes, approx
