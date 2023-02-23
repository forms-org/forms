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
    construct_df_table,
    get_execution_node_n_formula,
    get_single_value,
)
from forms.executor.dfexecutor.lookup.utils import (
    clean_col_idxes,
    clean_string_values,
    get_df,
    get_literal_value,
)
from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_hash_vector


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, df, col_idxes, approx = get_vlookup_params(physical_subtree)
    values, col_idxes = values.iloc[:, 0], col_idxes.iloc[:, 0]
    if approx:
        result_df = vlookup_approx_np_vector(values, df, col_idxes)
    else:
        result_df = vlookup_exact_hash_vector(values, df, col_idxes)
    return construct_df_table(result_df)


# Retrives parameters for VLOOKUP.
def get_vlookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size))
    df: pd.DataFrame = get_df(children[1])
    col_idxes: pd.DataFrame = clean_col_idxes(get_literal_value(children[2], size))
    approx: bool = True
    if len(children) == 4:
        approx = get_single_value(children[3]) != 0

    return values, df, col_idxes, approx
