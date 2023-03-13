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
from forms.executor.dfexecutor.utils import construct_df_table, get_execution_node_n_formula
from forms.executor.dfexecutor.lookup.utils import (
    clean_string_values,
    get_df,
    get_literal_value,
    get_ref_series,
    get_ref_df
)
from forms.executor.dfexecutor.lookup.api import lookup
from forms.executor.planexecutor import PlanExecutor


def lookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, search_range, result_range = get_lookup_params(physical_subtree)
    values = values.iloc[:, 0]
    result_df = lookup(values, search_range, result_range)
    return construct_df_table(result_df)


# Retrives parameters for LOOKUP.
def get_lookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify LOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 2 or num_children == 3

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size))
    search_range: pd.DataFrame = get_df(children[1])
    if num_children == 2:
        result_range = search_range.iloc[:, -1]
        search_range = search_range.iloc[:, 0]
    else:
        result_range = get_df(children[2]).iloc[:, 0]
        search_range = search_range.iloc[:, 0]

    return values, search_range, result_range


def lookup_plan_executor(plan_executor: PlanExecutor, df_table, formula_plan, size, client):
    sub_plans = formula_plan.children

    values = get_ref_series(plan_executor, df_table, sub_plans[0], size)
    search_range = get_ref_df(plan_executor.execute_formula_plan(df_table, sub_plans[1]), sub_plans[1])

    if len(sub_plans) == 2:
        result_range = search_range.iloc[:, -1]
        search_range = search_range.iloc[:, 0]
    else:
        result_range = get_ref_series(plan_executor, df_table, sub_plans[2], size)
        search_range = search_range.iloc[:, 0]

    res = lookup(values, search_range, result_range, dask_client=client)
    return DFTable(df=res)
