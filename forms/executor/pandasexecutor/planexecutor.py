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
from forms.executor.pandasexecutor.functionexecutor import function_to_executor_dict


class DFPlanExecutor(PlanExecutor):
    def df_execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        df_table = DFTable(df)
        res_table = super().execute_formula_plan(df_table, formula_plan)

        assert isinstance(res_table, DFTable)

        return res_table.df

    def execute_one_subtree(self, physical_subtree_list: list, axis: int) -> Table:
        df_list = [
            self.execute_on_one_core(physical_subtree).df for physical_subtree in physical_subtree_list
        ]
        res_df = pd.concat(df_list, axis=axis)
        return DFTable(res_df)

    def execute_on_one_core(self, physical_subtree: FunctionExecutionNode) -> DFTable:

        new_children = []
        for child in physical_subtree.children:
            if isinstance(child, FunctionExecutionNode):
                df_table = self.execute_on_one_core(child)
                ref_node = create_intermediate_ref_node(df_table, child)
                new_children.append(ref_node)
            else:
                new_children.append(child)

        for new_child in new_children:
            new_child.parent = physical_subtree
        physical_subtree.children = new_children

        function_executor = function_to_executor_dict(physical_subtree.function)
        return function_executor(physical_subtree)
