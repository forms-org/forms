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
from abc import ABC, abstractmethod
from enum import Enum, auto

from pynverse import inversefunc

from forms.executor.executionnode import (
    ExecutionNode,
    FunctionExecutionNode,
    RefExecutionNode,
    LitExecutionNode,
)
from forms.executor.utils import ExecutionConfig
from forms.utils.exceptions import PartitionPlannerNotSupportedException
from forms.utils.reference import RefType


class BasePartitionPlanner(ABC):
    def __init__(self, subtree: ExecutionNode, config: ExecutionConfig):
        self.subtree = subtree
        self.cores = config.cores
        self.num_of_formulae = config.num_of_formulae

    @abstractmethod
    def partition_plan(self):
        pass


class EvenlyDividedPartitionPlanner(BasePartitionPlanner, ABC):
    def __init__(self, subtree: ExecutionNode, config: ExecutionConfig):
        super().__init__(subtree, config)

    def partition_plan(self):
        cores = self.cores
        num_of_formulae = self.num_of_formulae
        partitions = [int(i * num_of_formulae / cores) for i in range(cores)]
        partitions.append(num_of_formulae)
        return partitions


def rr_compute(k, w, h):
    return k * w * h


def fr_compute(k, w, h):
    return (2 * w * h + w * k) * k / 2


def rf_compute(k, w, h):
    end = max(0, h - k)
    return (w * h + w * end) * (h - end) / 2


def ff_compute(k, w, h):
    return k * w * h


class CostModelPartitionPlanner(BasePartitionPlanner, ABC):
    def __init__(self, subtree: ExecutionNode, config: ExecutionConfig):
        super().__init__(subtree, config)

    def f_compute(self, subtree, k):
        cost = 0
        for child in subtree.children:
            if isinstance(child, RefExecutionNode):
                ref = child.ref
                w = ref.last_col - ref.col + 1
                h = ref.last_row - ref.row + 1
                out_ref_type = child.out_ref_type
                if out_ref_type == RefType.RR:
                    cost += rr_compute(k, w, h)
                elif out_ref_type == RefType.FR:
                    cost += fr_compute(k, w, h)
                elif out_ref_type == RefType.RF:
                    cost += rf_compute(k, w, h)
                else:  # RefType.FF
                    cost += ff_compute(k, w, h)
            elif isinstance(child, FunctionExecutionNode):
                cost += self.f_compute(child, k)
        return cost

    def F(self, k, N):
        return self.f_compute(self.subtree, k) / self.f_compute(self.subtree, N)

    def cost(self):
        """
        Returns: the estimated cost for the whole subtree
        """
        return self.f_compute(self.subtree, self.num_of_formulae)

    def partition_plan(self):
        Q = inversefunc(lambda p: self.F(p, self.num_of_formulae))
        partitions = [0]
        for i in range(1, self.cores):
            p = i / self.cores
            res = Q(p)
            partitions.append(int(res))
        partitions.append(self.num_of_formulae)
        return partitions


class PartitionPlanners(Enum):
    EvenlyDivided = auto()
    CostModel = auto()


partition_planner_class_dict = {
    PartitionPlanners.EvenlyDivided.name.lower(): EvenlyDividedPartitionPlanner,
    PartitionPlanners.CostModel.name.lower(): CostModelPartitionPlanner,
}


def create_partition_planner_by_name(
    p_name: str, execution_tree: ExecutionNode, exec_config: ExecutionConfig
):
    if p_name.lower() in partition_planner_class_dict.keys():
        return partition_planner_class_dict[p_name](execution_tree, exec_config)
    raise PartitionPlannerNotSupportedException(f"Partition Planner {p_name} is not supported")
