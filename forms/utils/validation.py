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

from forms.core.config import FormSConfig
from forms.utils.functions import FunctionExecutor
from forms.utils.exceptions import FunctionExecutorNotSupportedException


def validate(forms_config: FormSConfig):
    if forms_config.function_executor == FunctionExecutor.df_mixed_executor.name.lower():
        raise FunctionExecutorNotSupportedException(f"{forms_config.function_executor} Not Supported")
