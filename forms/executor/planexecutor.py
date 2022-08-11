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

from forms.executor.utils import row_axis, col_axis
from forms.executor.scheduler import *
from forms.planner.plannode import PlanNode
from forms.core.config import FormSConfig


class PlanExecutor(ABC):
    def __init__(self, forms_config: FormSConfig):
        self.forms_config = forms_config

    def build_exec_config(self, table: Table, ref_dir: RefDirection) -> ExecutionConfig:
        exec_config = ExecutionConfig()
        exec_config.cores = self.forms_config.cores
        if ref_dir == RefDirection.UP or ref_dir == RefDirection.DOWN:
            exec_config.num_of_formulae = table.get_num_of_rows()
            exec_config.axis = row_axis
        else:
            exec_config.num_of_formulae = table.get_num_of_columns()
            exec_config.axis = col_axis
        return exec_config

    def execute_formula_plan(self, table: Table, formula_plan: PlanNode) -> Table:
        exec_tree = from_plan_to_execution_tree(formula_plan, table)
        exec_config = self.build_exec_config(table, formula_plan.out_ref_dir)
        scheduler = create_scheduler_by_name(self.forms_config.scheduler, exec_config, exec_tree)

        future_result_dict = {}
        while not scheduler.is_finished():
            next_subtree, physical_subtree_list = scheduler.next_subtree()
            future_result_dict[next_subtree] = self.execute_one_subtree(
                physical_subtree_list, exec_config.axis
            )

            finished_exec_subtrees = [
                exec_subtree
                for exec_subtree in future_result_dict
                if not future_result_dict[exec_subtree].is_empty()
            ]
            for exec_subtree in finished_exec_subtrees:
                scheduler.finish_subtree(exec_subtree, future_result_dict[exec_subtree])
                del future_result_dict[exec_subtree]

        return scheduler.get_results()

    @abstractmethod
    def execute_one_subtree(self, physical_subtree_list: list, axis: int) -> Table:
        pass
