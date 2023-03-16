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

from forms.executor.table import DFTable
from forms.planner.plannode import LiteralNode, RefType
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode, LitExecutionNode
from forms.executor.dfexecutor.utils import (
    get_execution_node_n_formula,
    construct_df_table,
    get_single_value,
)
from forms.executor.dfexecutor.lookup.utils import (
    clean_string_values,
    get_df,
    get_literal_value,
    get_df_bins,
    get_ref_df,
    get_ref_series
)
from forms.executor.dfexecutor.lookup.api import vlookup
from forms.executor.planexecutor import PlanExecutor


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, df, col_idxes, approx = get_vlookup_params_broadcast_values(physical_subtree)
    result_df = vlookup(values, df, col_idxes, approx=approx)
    if result_df is None:
        result_df = pd.DataFrame([])
    elif len(result_df) == len(values):
        result_df = result_df.set_index(values.index)
    return construct_df_table(result_df)


# Retrives parameters for VLOOKUP.
def get_vlookup_params_broadcast_df(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size).iloc[:, 0])
    df: pd.DataFrame = get_df(children[1])
    col_idxes: pd.DataFrame = get_literal_value(children[2], size).iloc[:, 0].astype(int)
    approx = True
    if len(children) == 4:
        approx = get_single_value(children[3]) != 0

    return values, df, col_idxes, approx


def get_full_node_df(df, child):
    if isinstance(child, RefExecutionNode):
        row, col = child.ref.row, child.ref.col
        if child.out_ref_type == RefType.FF:
            v = child.table.get_table_content().iloc[row, col]
            return pd.Series(np.full(df.shape[0], v))
        else:
            return df.iloc[:, col]
    else:
        assert isinstance(child, LitExecutionNode)
        return pd.Series(np.full(df.shape[0], child.literal))


# Retrives parameters for VLOOKUP.
def get_vlookup_params_broadcast_values(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    cores = physical_subtree.cores
    subtree_idx = physical_subtree.subtree_idx

    # get df, values and col_idxes
    df_child = physical_subtree.children[1]
    ref, context = df_child.ref, df_child.exec_context
    full_df = df_child.table.get_table_content()
    values_child = physical_subtree.children[0]
    col_idxes_child: RefExecutionNode = physical_subtree.children[2]
    all_values = clean_string_values(get_full_node_df(full_df, values_child))
    all_col_idxes = get_full_node_df(full_df, col_idxes_child).astype(int)
    approx = True
    if len(children) == 4:
        approx = get_single_value(children[3]) != 0

    full_df = full_df.iloc[:, ref.col: ref.last_col + 1]
    if approx:
        bins = get_df_bins(full_df.iloc[:, 0], cores)[0]
        binned_data = pd.Series(np.searchsorted(bins, all_values), index=all_values.index)
        values = all_values[binned_data == subtree_idx]
        col_idxes = all_col_idxes[binned_data == subtree_idx]
        start, end = context.start_formula_idx, context.end_formula_idx
        df: pd.DataFrame = full_df.iloc[start: end]
    else:
        values_hash = pd.util.hash_array(all_values.to_numpy()) % cores
        search_range = full_df.iloc[:, 0].astype(all_values.dtype)
        search_range_hash = pd.util.hash_array(search_range.to_numpy()) % cores
        values = all_values[values_hash == subtree_idx]
        col_idxes = all_col_idxes[values_hash == subtree_idx]
        df = full_df[search_range_hash == subtree_idx]

    return values, df, col_idxes, approx


def vlookup_plan_executor(plan_executor: PlanExecutor, df_table, formula_plan, size, client):
    sub_plans = formula_plan.children
    assert len(sub_plans) == 3 or len(sub_plans) == 4

    values = clean_string_values(get_ref_series(plan_executor, df_table, sub_plans[0], size))
    df = get_ref_df(plan_executor.execute_formula_plan(df_table, sub_plans[1]), sub_plans[1])
    col_idxes = get_ref_series(plan_executor, df_table, sub_plans[2], size).astype(int)
    approx = True
    if len(sub_plans) == 4:
        if isinstance(sub_plans[3], LiteralNode):
            literal = sub_plans[3].literal
            approx = literal != 0 and str(literal).strip().lower() != "false"

    res = vlookup(values, df, col_idxes.astype(int), approx, dask_client=client)
    return DFTable(df=res)