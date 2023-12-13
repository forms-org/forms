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

import pandas as pd
from forms.core.config import DBConfig, DBExecContext
from forms.planner.plannode import PlanNode
from forms.utils.metrics import MetricsTracker


class DBExecutor:
    def __init__(
        self, db_config: DBConfig, exec_context: DBExecContext, metrics_tracker: MetricsTracker
    ):
        self.db_config = db_config
        self.exec_context = exec_context
        self.metrics_tracker = metrics_tracker

    def execute_formula_plan(self, formula_plan: PlanNode) -> pd.DataFrame:
        pass

    def clean_up(self):
        pass
