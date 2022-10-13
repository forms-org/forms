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
from time import time

from pynverse import inversefunc

from forms.executor.executionnode import (
    ExecutionNode,
    FunctionExecutionNode,
    RefExecutionNode,
    LitExecutionNode,
)
from forms.executor.utils import ExecutionConfig
from forms.utils.exceptions import CostModelNotSupportedException
from forms.utils.reference import RefType


class BaseCostModel(ABC):
    def __init__(self, num_of_formulae: int):
        self.num_of_formulae = num_of_formulae
        self.time_cost = 0

    @abstractmethod
    def cost(self, subtree: ExecutionNode, cores: int):
        """
        Returns: the estimated cost for the whole subtree if run on cores number of cores
        """
        pass

    @abstractmethod
    def get_partition_plan(self, subtree: ExecutionNode, cores: int):
        """
        Returns: the partition plan (how formulae are partitioned across cores)
        """
        pass


class SimpleCostModel(BaseCostModel, ABC):
    def __init__(self, num_of_formulae: int):
        super().__init__(num_of_formulae)

    def cost(self, subtree: ExecutionNode, cores: int):
        return self.num_of_formulae

    def get_partition_plan(self, subtree: ExecutionNode, cores: int):
        start_time = time()
        num_of_formulae = self.num_of_formulae
        partitions = [int(i * num_of_formulae / cores) for i in range(cores)]
        partitions.append(num_of_formulae)
        end_time = time()
        self.time_cost += end_time - start_time
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


class LoadBalanceCostModel(BaseCostModel, ABC):
    def __init__(self, num_of_formulae: int):
        super().__init__(num_of_formulae)

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

    def F(self, subtree, k, N):
        return self.f_compute(subtree, k) / self.f_compute(subtree, N)

    def cost(self, subtree: ExecutionNode, cores: int):
        return self.f_compute(subtree, self.num_of_formulae)

    def get_partition_plan(self, subtree: ExecutionNode, cores: int):
        start_time = time()
        Q = inversefunc(lambda p: self.F(subtree, p, self.num_of_formulae))
        partitions = [0]
        for i in range(1, cores):
            p = i / cores
            res = Q(p)
            partitions.append(int(res))
        partitions.append(self.num_of_formulae)
        end_time = time()
        self.time_cost += end_time - start_time
        return partitions


class CostModels(Enum):
    Simple = auto()
    LoadBalance = auto()


partition_planner_class_dict = {
    CostModels.Simple.name.lower(): SimpleCostModel,
    CostModels.LoadBalance.name.lower(): LoadBalanceCostModel,
}


def create_cost_model_by_name(c_name: str, num_of_formulae: int):
    if c_name.lower() in partition_planner_class_dict.keys():
        return partition_planner_class_dict[c_name](num_of_formulae)
    raise CostModelNotSupportedException(f"Cost Model {c_name} is not supported")
