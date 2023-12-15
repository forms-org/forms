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

from forms.executor.dbexecutor.dbexecnode import DBExecNode


class Scheduler:
    def __init__(self, exec_tree: DBExecNode):
        pass

    def next_substree(self) -> DBExecNode:
        pass

    def has_next_subtree(self) -> bool:
        pass

    def finish_one_subtree(self, exec_subtree: DBExecNode):
        pass
