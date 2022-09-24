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

from forms.runtime.runtime import create_runtime_by_name


class FormSGlobal:
    def __init__(self):
        self.run_time = None

    def get_runtime(self, runtime_name, forms_config):
        if self.run_time is None:
            self.run_time = create_runtime_by_name(runtime_name, forms_config)
        return self.run_time


forms_global = FormSGlobal()
