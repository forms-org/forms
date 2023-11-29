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

from forms.core.config import DBConfig, DFConfig, DFExecContext
from forms.executor.dfexecutor.dfexecutor import DFExecutor

from forms.parser.parser import parse_formula
from forms.planner.plannode import PlanNode
from forms.planner.planrewriter import PlanRewriter
from forms.utils.functions import FunctionExecutor
from forms.utils.metrics import MetricsTracker
from forms.utils.validator import validate

from forms.utils.exceptions import FormSException
from forms.utils.reference import default_axis
from abc import ABC, abstractmethod


class Workbook(ABC):
    def __init__(self):
        self.metrics_tracker = MetricsTracker()

    @abstractmethod
    def compute_formula(self, formula_str: str, num_formulas: int = -1, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def print_workbook(self, num_rows=10, keep_original_labels=False):
        pass

    def get_metrics(self):
        return self.metrics_tracker.get_metrics()

    @abstractmethod
    def close(self):
        pass


"""Public API"""


class DFWorkbook(Workbook):
    def __init__(self, df_config: DFConfig, df: pd.DataFrame):
        super().__init__()
        self.df_config = df_config
        self.df = df

    def compute_formula(self, formula_str: str, num_formulas: int = 0, **kwargs) -> pd.DataFrame:
        try:
            root = parse_formula_str(formula_str)
            validate(FunctionExecutor.df_executor, self.df.shape[0], self.df.shape[1], root)
            root = PlanRewriter(self.df_config).rewrite_plan(root)

            if num_formulas <= 0:
                num_formulas = self.df.shape[0]
            exec_context = DFExecContext(0, num_formulas, default_axis)
            executor = DFExecutor(self.df_config, exec_context, self.metrics_tracker)
            res = executor.execute_formula_plan(self.df, root)
            executor.clean_up()

            return res
        except FormSException:
            traceback.print_exception(*sys.exc_info())

    def print_workbook(self, num_rows=10, keep_original_labels=False):
        print_workbook_view(self.df.head(num_rows), keep_original_labels)

    def close(self):
        self.df = None


class DBWorkbook(Workbook):
    def __init__(self, db_config: DBConfig):
        super().__init__()
        self.db_config = db_config

    def compute_formula(self, formula_str: str, num_formulas: int = -1, **kwargs) -> pd.DataFrame:
        pass

    def print_workbook(self, num_rows=10, keep_original_labels=False):
        pass

    def close(self):
        pass


def open_workbook_from_df(df: pd.DataFrame, enable_rewriting=True) -> DFWorkbook:
    return DFWorkbook(DFConfig(enable_rewriting), df)


def open_workbook_from_db(db_conn: str, table_name: str, enable_rewriting=True) -> DBWorkbook:
    pass


"""Helper Functions"""


def parse_formula_str(formula_str: str) -> PlanNode:
    root = parse_formula(formula_str, default_axis)
    root.populate_ref_info()
    return root


def print_workbook_view(df: pd.DataFrame, keep_original_labels=False):
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
        print(df_copy.to_string())
    except FormSException:
        traceback.print_exception(*sys.exc_info())
