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
from forms.executor.dfexecutor.planexecutor import DFPlanExecutor
from forms.core.config import FormSConfig
from forms.utils.exceptions import ExecutorNotSupportedException


class Executors(Enum):
    DFEXECUTOR = auto()


executor_class_dict = {Executors.DFEXECUTOR.name.lower(): DFPlanExecutor}


def create_executor_by_name(e_name: str, forms_config: FormSConfig):
    if e_name.lower() in executor_class_dict.keys():
        return executor_class_dict[e_name](forms_config)
    raise ExecutorNotSupportedException(f"Executor {e_name} is not supported")
