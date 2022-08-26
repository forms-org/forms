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

from forms.planner.plannode import PlanNode
from forms.executor.table import Table, DFTable
from forms.executor.executionnode import FunctionExecutionNode, create_intermediate_ref_node
from forms.executor.planexecutor import PlanExecutor
from forms.executor.dfexecutor.functionexecutor import find_function_executor
from forms.utils.treenode import link_parent_to_children
from forms.core.config import FormSConfig
from forms.runtime.runtime import create_runtime_by_name


def execute_one_subtree(physical_subtree: FunctionExecutionNode) -> Table:
    new_children = []
    for child in physical_subtree.children:
        if isinstance(child, FunctionExecutionNode):
            df_table = execute_one_subtree(child)
            ref_node = create_intermediate_ref_node(df_table, child)
            new_children.append(ref_node)
        else:
            new_children.append(child)
    link_parent_to_children(physical_subtree, new_children)

    function_executor = find_function_executor(physical_subtree.function)
    return function_executor(physical_subtree)


class DFPlanExecutor(PlanExecutor):
    def __init__(self, forms_config: FormSConfig):
        super().__init__(forms_config)
        self.runtime = create_runtime_by_name(forms_config.runtime, forms_config)
        self.execute_one_subtree = execute_one_subtree

    def df_execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        start = time()
        df_table = DFTable(df, self.runtime.distribute_data(df))
        print(f"Distributing data time: {time() - start}")
        start = time()
        res_table = super().execute_formula_plan(df_table, formula_plan)
        print(f"Execution time: {time() - start}")

        assert isinstance(res_table, DFTable)

        return res_table.df

    def collect_results(self, remote_object_list, axis: int) -> Table:
        df_list = [remote_obj.get_content().get_table_content() for remote_obj in remote_object_list]
        res_df = pd.concat(df_list, axis=axis, ignore_index=True)
        return DFTable(res_df)

    def distribute_results(self, table: Table):
        if table is not None:
            df_remote_object = self.runtime.distribute_data(table.get_table_content())
            table.remote_object = df_remote_object

    def clean_up(self):
        self.runtime.shut_down()
