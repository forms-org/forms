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
import numpy as np

from time import time

from forms.executor.dfexecutor.remotedf import partition_df, find_rows_and_cols, RemoteDF
from forms.planner.plannode import PlanNode
from forms.executor.table import Table, DFTable
from forms.executor.executionnode import FunctionExecutionNode, create_intermediate_ref_node
from forms.executor.planexecutor import PlanExecutor
from forms.executor.dfexecutor.basicfuncexecutor import find_function_executor
from forms.utils.treenode import link_parent_to_children
from forms.utils.reference import axis_along_row
from forms.core.config import FormSConfig
from forms.executor.compiler import DFCompiler
from forms.core.globals import forms_global


def execute_one_subtree(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    new_children = []
    for child in physical_subtree.children:
        if isinstance(child, FunctionExecutionNode):
            df_table = execute_one_subtree(child)
            ref_node = create_intermediate_ref_node(DFTable(df=df_table), child)
            new_children.append(ref_node)
        else:
            new_children.append(child)
    link_parent_to_children(physical_subtree, new_children)

    function_executor = find_function_executor(physical_subtree.function)
    res_table = function_executor(physical_subtree)
    return res_table.get_table_content()


class DFPlanExecutor(PlanExecutor):
    def __init__(self, forms_config: FormSConfig):
        super().__init__(forms_config)
        self.compiler = DFCompiler()
        self.runtime = forms_global.get_runtime(forms_config.runtime, forms_config)
        self.execute_one_subtree = execute_one_subtree

    def df_execute_formula_plan(self, df: pd.DataFrame, formula_plan: PlanNode) -> pd.DataFrame:
        num_row_partitions = self.forms_config.partition_shape[0]
        num_col_partitions = self.forms_config.partition_shape[1]
        partitions = partition_df(df, num_row_partitions, num_col_partitions)
        rows, cols = find_rows_and_cols(partitions)

        start = time()
        remote_object_array = np.array(
            [
                [self.runtime.distribute_data(one_partition) for one_partition in one_row]
                for one_row in partitions
            ]
        )
        remote_df = RemoteDF(remote_object_array, rows, cols)
        df_table = DFTable(df, remote_df)
        print(f"Distributing data time: {time() - start}")
        start = time()
        res_table = super().execute_formula_plan(df_table, formula_plan)
        print(f"Execution time: {time() - start}")

        assert isinstance(res_table, DFTable)

        return res_table.get_table_content()

    def collect_results(self, remote_object_list, physical_subtree_list, axis: int) -> Table:
        shape = (len(remote_object_list), 1) if axis == axis_along_row else (1, len(remote_object_list))
        remote_object_array = np.array(remote_object_list).reshape(shape)

        exec_context_list = [physical_subtree.exec_context for physical_subtree in physical_subtree_list]
        num_formulae_array = np.array(
            [
                exec_context.end_formula_idx - exec_context.start_formula_idx
                for exec_context in exec_context_list
            ]
        ).reshape(shape)
        ones_array = np.ones(shape)

        rows, cols = (
            (num_formulae_array, ones_array)
            if axis == axis_along_row
            else (ones_array, num_formulae_array)
        )
        remote_df = RemoteDF(remote_object_array, rows, cols)
        return DFTable(remote_df=remote_df)

    def clean_up(self):
        pass
