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

from forms.executor.scheduler import Schedulers


class FormSConfig:
    def __init__(self):
        self.cores = 1
        self.scheduler = Schedulers.SIMPLE.name.lower()
        self.enable_logical_rewriting = False
        self.enable_physical_opt = False


forms_config = FormSConfig()
