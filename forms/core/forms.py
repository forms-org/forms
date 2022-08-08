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

from forms.core.config import forms_config

from forms.parser.parser import parse_formula
from forms.planner.planrewriter import PlanRewriter
from forms.executer.pandasexecutor.planexecutor import PlanExecutor


def compute_formula(df: pd.DataFrame, formula_str: str) -> pd.DataFrame:
    root = parse_formula(formula_str)

    plan_rewriter = PlanRewriter(forms_config)
    root = plan_rewriter.rewrite_plan(root)

    plan_executor = PlanExecutor(forms_config)
    return plan_executor.execute_formula_plan(df, root)


def config(parallelism=1):
    forms_config.parallelism = parallelism
