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

from dask.distributed import Client, get_client
from forms.planner.plannode import PlanNode
from forms.executor.table import Table, DFTable
from forms.executor.executionnode import FunctionExecutionNode, create_intermediate_ref_node
from forms.executor.planexecutor import PlanExecutor
from forms.executor.pandasexecutor.functionexecutor import find_function_executor
from forms.utils.treenode import link_parent_to_children
from forms.core.config import FormSConfig


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
        self.client = Client(processes=True, n_workers=self.forms_config.cores)
        self.execute_one_subtree = execute_one_subtree

    def df_execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        df_table = DFTable(df, self.client.scatter(df))
        res_table = super().execute_formula_plan(df_table, formula_plan)

        assert isinstance(res_table, DFTable)

        return res_table.df

    def collect_results(self, futures, axis: int) -> Table:
        df_list = [future.result().get_table_content() for future in futures]
        res_df = pd.concat(df_list, axis=axis)
        return DFTable(res_df)

    def scatter_results(self, table: Table):
        if table is not None:
            df_future = self.client.scatter(table.get_table_content())
            table.df_future = df_future

    def clean_up(self):
        self.client.close()
