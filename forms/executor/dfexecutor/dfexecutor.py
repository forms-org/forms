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

from time import time
from forms.core.config import DFConfig
from forms.executor.dfexecutor.dfexecnode import (
    DFExecContext,
    DFExecNode,
    DFFuncExecNode,
    from_plan_to_execution_tree,
)

from forms.planner.plannode import PlanNode
from forms.executor.dfexecutor.dftable import DFTable
from forms.executor.dfexecutor.dfexecnode import create_intermediate_ref_node
from forms.executor.dfexecutor.basicfuncexecutor import find_function_executor
from forms.utils.metrics import MetricsTracker
from forms.utils.treenode import link_parent_to_children


def execute_physical_plan(physical_plan: DFExecNode) -> DFTable:
    new_children = []
    for child in physical_plan.children:
        if isinstance(child, DFFuncExecNode):
            df_table = execute_physical_plan(child)
            ref_node = create_intermediate_ref_node(df_table, child)
            new_children.append(ref_node)
        else:
            new_children.append(child)
    link_parent_to_children(physical_plan, new_children)

    function_executor = find_function_executor(physical_plan.function)
    res_table = function_executor(physical_plan)
    return res_table


class DFExecutor:
    def __init__(
        self, df_config: DFConfig, exec_context: DFExecContext, metrics_tracker: MetricsTracker
    ):
        self.df_config = df_config
        self.exec_context = exec_context
        self.metrics_tracker = metrics_tracker

    def execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        df_table = DFTable(df)
        physical_plan = from_plan_to_execution_tree(formula_plan, df_table)
        physical_plan.set_exec_context(self.exec_context)

        start = time()
        res_table = execute_physical_plan(physical_plan)
        execution_time = time() - start
        print(f"Execution time: {execution_time}")
        self.metrics_tracker.put_one_metric("execution_time", execution_time)

        return res_table.get_table_content()

    def clean_up(self):
        pass
