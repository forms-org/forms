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
import traceback
import sys

from forms.core.config import forms_config

from forms.parser.parser import parse_formula
from forms.planner.planrewriter import PlanRewriter
from forms.executor.pandasexecutor.planexecutor import DFPlanExecutor

from forms.utils.exceptions import FormSException


def compute_formula(df: pd.DataFrame, formula_str: str) -> pd.DataFrame:
    try:
        root = parse_formula(formula_str)
        root.validate()
        root.populate_ref_info()

        plan_rewriter = PlanRewriter(forms_config)
        root = plan_rewriter.rewrite_plan(root)

        plan_executor = DFPlanExecutor(forms_config)
        res = plan_executor.df_execute_formula_plan(df, root)
        plan_executor.clean_up()

        return res
    except FormSException:
        traceback.print_exception(*sys.exc_info())


def config(
    cores=forms_config.cores,
    scheduler=forms_config.scheduler,
    enable_logical_rewriting=forms_config.enable_logical_rewriting,
    enable_physical_opt=forms_config.enable_physical_opt,
    runtime=forms_config.runtime,
):
    forms_config.cores = cores
    forms_config.scheduler = scheduler
    forms_config.enable_logical_rewriting = enable_logical_rewriting
    forms_config.enable_physical_opt = enable_physical_opt
    forms_config.runtime = runtime
