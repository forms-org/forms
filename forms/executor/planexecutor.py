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

from time import sleep

from forms.executor.executionnode import from_plan_to_execution_tree
from forms.executor.scheduler import *
from forms.planner.plannode import PlanNode
from forms.core.config import FormSConfig
from forms.utils.reference import axis_along_column


class PlanExecutor(ABC):
    def __init__(self, forms_config: FormSConfig):
        self.forms_config = forms_config
        self.schedule_interval = 0.01  # seconds
        self.runtime = None
        self.execute_one_subtree = None

    def build_exec_config(self, table: Table, axis: int) -> ExecutionConfig:
        if axis == axis_along_column:
            num_of_formulae = table.get_num_of_columns()
        else:  # axis_along_row
            num_of_formulae = table.get_num_of_rows()
        exec_config = ExecutionConfig(axis, num_of_formulae, cores=self.forms_config.cores)
        return exec_config

    def execute_formula_plan(self, table: Table, formula_plan: PlanNode) -> Table:
        exec_tree = from_plan_to_execution_tree(formula_plan, table)
        exec_config = self.build_exec_config(table, formula_plan.out_ref_axis)
        scheduler = create_scheduler_by_name(self.forms_config.scheduler, exec_config, exec_tree)

        remote_object_dict = {}
        while not scheduler.is_finished():
            next_subtree, physical_subtree_list = scheduler.next_subtree()
            if next_subtree is not None:
                remote_object_dict[next_subtree] = [
                    self.runtime.submit_one_func(self.execute_one_subtree, physical_subtree)
                    for physical_subtree in physical_subtree_list
                ]

            finished_exec_subtrees = [
                exec_subtree
                for exec_subtree in remote_object_dict
                if all(
                    [
                        remote_object.is_object_computed()
                        for remote_object in remote_object_dict[exec_subtree]
                    ]
                )
            ]
            for exec_subtree in finished_exec_subtrees:
                intermediate_result = self.collect_results(
                    remote_object_dict[exec_subtree], exec_config.axis
                )
                scheduler.finish_subtree(exec_subtree, intermediate_result)
                if not scheduler.is_finished():
                    self.distribute_results(intermediate_result)
                del remote_object_dict[exec_subtree]

            sleep(self.schedule_interval)

        return scheduler.get_results()

    @abstractmethod
    def collect_results(self, futures, axis: int) -> Table:
        pass

    @abstractmethod
    def distribute_results(self, table: Table):
        pass

    @abstractmethod
    def clean_up(self):
        pass
