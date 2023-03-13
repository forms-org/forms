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
import numpy as np
import pandas as pd
from forms.executor.executionnode import FunctionExecutionNode, ExecutionNode
from forms.executor.dfexecutor.utils import get_single_value

from time import time


# Partitions a dataframe based on bins and groups by the bin id.
def range_partition_df(physical_subtree: FunctionExecutionNode, bins: list[any], num_cores: int):
    context = physical_subtree.exec_context
    children = physical_subtree.children
    index = list(range(context.start_formula_idx, context.end_formula_idx))
    size = context.end_formula_idx - context.start_formula_idx
    values = get_literal_value(children[0], size).set_axis(index)
    col_idxes = get_literal_value(children[2], size).set_axis(index)
    df = pd.concat([values, col_idxes], axis=1)
    print(children[0])
    binned_df = pd.Series(np.searchsorted(bins, values.iloc[:, 0]), index=df.index)
    return [df[binned_df == i] for i in range(num_cores)]


# Locally hashes a dataframe with 1 column and groups it by hash.
def hash_partition_df(df: pd.DataFrame, num_cores: int):
    hashed_df = pd.util.hash_array(df.iloc[:, 0].to_numpy()) % num_cores
    return [df[hashed_df == i] for i in range(num_cores)]


# Splits the df into bins for range partitioning.
def get_df_bins(df, physical_subtree_list):
    return [
        df.iloc[physical_subtree.children[1].exec_context.end_formula_idx - 1]
        for physical_subtree in physical_subtree_list
    ][:-1]


def get_literal_value(child: ExecutionNode, size: int) -> pd.DataFrame:
    value = get_single_value(child)
    if not isinstance(value, pd.DataFrame):
        value = pd.DataFrame(np.full(size, value))
    elif value.shape[0] == 1:
        value = pd.DataFrame(np.full(size, value.iloc[0, 0]))
    return value