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

from forms.utils.exceptions import FormSException
from forms.utils.reference import default_axis
from forms.utils.executors import create_executor_by_name
from forms.utils.validation import validate


def compute_formula(df: pd.DataFrame, formula_str: str, axis=default_axis) -> pd.DataFrame:
    try:
        validate(forms_config)

        root = parse_formula(formula_str, axis)
        root.validate(forms_config)
        root.populate_ref_info()

        plan_rewriter = PlanRewriter(forms_config)
        root = plan_rewriter.rewrite_plan(root)

        executor_name = "dfexecutor"
        plan_executor = create_executor_by_name(executor_name, forms_config)
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
    function_executor=forms_config.function_executor,
    cost_model=forms_config.cost_model,
    enable_communication_opt=forms_config.enable_communication_opt,
    partition_shape=forms_config.partition_shape,
):
    forms_config.cores = cores
    forms_config.scheduler = scheduler
    forms_config.enable_logical_rewriting = enable_logical_rewriting
    forms_config.enable_physical_opt = enable_physical_opt
    forms_config.runtime = runtime
    forms_config.function_executor = function_executor
    forms_config.cost_model = cost_model
    forms_config.enable_communication_opt = enable_communication_opt
    forms_config.partition_shape = partition_shape


def print_spreadsheet_view(df: pd.DataFrame, keep_original_labels=False):
    df_copy = df.copy(deep=True)
    try:
        # Flatten cols into tuple format if multi-index
        if isinstance(df_copy.columns, pd.MultiIndex):
            df_copy.columns = df_copy.columns.to_flat_index()
        for i, label in enumerate(df_copy.columns):
            res = ""
            # We want A = 1 instead of 0
            i += 1
            while i > 0:
                mod = (i - 1) % 26
                res += chr(mod + ord("A"))
                i = (i - 1) // 26
            res = res[::-1]
            # Preserve labels
            if keep_original_labels:
                res = res + " (" + str(label) + ")"
            df_copy.rename(columns={label: res}, inplace=True)

        # Preserve labels
        if keep_original_labels:
            # Flatten rows if multi-index
            if isinstance(df_copy.index, pd.MultiIndex):
                df_copy.index = [
                    str(i + 1) + " (" + ".".join(col) + ")" for i, col in enumerate(df_copy.index.values)
                ]
            else:
                df_copy.index = [
                    str(i + 1) + " (" + str(col) + ")" for i, col in enumerate(df_copy.index.values)
                ]
        else:
            df_copy.index = [str(i) for i in range(1, len(df_copy.index) + 1)]
        print(df.to_string())
    except FormSException:
        traceback.print_exception(*sys.exc_info())
