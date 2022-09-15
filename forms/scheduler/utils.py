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
from enum import Enum, auto

from forms.executor.compiler import BaseCompiler
from forms.executor.executionnode import ExecutionNode
from forms.executor.utils import ExecutionConfig
from forms.scheduler.dfscheduler.dfscheduler import DFSimpleScheduler, PrioritizedScheduler
from forms.utils.exceptions import SchedulerNotSupportedException


class Schedulers(Enum):
    SIMPLE = auto()
    PRIORITIZED = auto()


scheduler_class_dict = {
    Schedulers.SIMPLE.name.lower(): DFSimpleScheduler,
    Schedulers.PRIORITIZED.name.lower(): PrioritizedScheduler,
}


def create_scheduler_by_name(
    s_name: str, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
):
    if s_name.lower() in scheduler_class_dict.keys():
        return scheduler_class_dict[s_name](compiler, exec_config, execution_tree)
    raise SchedulerNotSupportedException(f"Scheduler {s_name} is not supported")
