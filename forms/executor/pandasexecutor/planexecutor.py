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

from forms.planner.plannode import PlanNode
from forms.executor.table import Table, DFTable
from forms.executor.executionnode import FunctionExecutionNode, create_intermediate_ref_node
from forms.executor.planexecutor import PlanExecutor
from forms.executor.pandasexecutor.functionexecutor import find_function_executor


class DFPlanExecutor(PlanExecutor):
    def df_execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        df_table = DFTable(df)
        res_table = super().execute_formula_plan(df_table, formula_plan)

        assert isinstance(res_table, DFTable)

        return res_table.df

    def execute_one_subtree(self, physical_subtree: FunctionExecutionNode) -> Table:
        new_children = []
        for child in physical_subtree.children:
            if isinstance(child, FunctionExecutionNode):
                df_table = self.execute_one_subtree(child)
                ref_node = create_intermediate_ref_node(df_table, child)
                new_children.append(ref_node)
            else:
                new_children.append(child)

        if physical_subtree.exec_context is None:
            physical_subtree.set_exec_context(physical_subtree.parent.exec_context)
        for new_child in new_children:
            new_child.parent = physical_subtree
            new_child.set_exec_context(physical_subtree.exec_context)
        physical_subtree.children = new_children

        function_executor = find_function_executor(physical_subtree.function)
        return function_executor(physical_subtree)

    def collect_results(self, futures, axis: int) -> Table:
        df_list = [future.result() for future in futures]
        res_df = pd.concat(df_list, axis)
        return DFTable(res_df)
