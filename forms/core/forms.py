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
import psycopg2

from forms.core.config import DBConfig, DFConfig, DFExecContext
from forms.executor.dfexecutor.dfexecutor import DFExecutor

from forms.parser.parser import parse_formula
from forms.planner.plannode import PlanNode
from forms.planner.planrewriter import PlanRewriter
from forms.utils.functions import FunctionExecutor
from forms.utils.metrics import MetricsTracker
from forms.utils.validator import validate

from forms.utils.exceptions import DBConfigException, DBRuntimeException, FormSException
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
        try:
            self.connection = psycopg2.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.username,
                password=db_config.password,
                dbname=db_config.db_name,
            )
        except psycopg2.Error as e:
            raise DBConfigException(f"Connection Error: {e}") 

        if not self.__check_primary_key():
            raise DBConfigException(f"The specified primary key does not match the table 
                                    {self.db_config.table_name}'s primary key")
        
        if not self.__check_order_key():
            raise DBConfigException(f"The specified order key is not part of the table 
                                    {self.db_config.table_name}'s columns")
        
        self.num_rows = self.__get_num_rows()
        
    def __check_primary_key(self) -> bool:
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc 
            JOIN information_schema.key_column_usage kcu 
              ON tc.constraint_name = kcu.constraint_name 
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
              AND tc.table_name = %s;
            """
            cursor.execute(query, (self.table_name,))
            primary_key_columns = [row[0] for row in cursor.fetchall()] 
            return set(primary_key_columns) == set(self.db_config.primary_key)

        except psycopg2.Error as e:
            raise DBRuntimeException(f"Cursor Error: {e}") 
        finally:
            cursor.close()       

    def __check_order_key(self) -> bool:
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s;
            """
            cursor.execute(query, (self.db_config.table_name,))
            table_columns = [row[0] for row in cursor.fetchall()]
            return all(column in table_columns for column in self.db_config.order_key)

        except psycopg2.Error as e:
            raise DBRuntimeException(f"Cursor Error: {e}") 
        finally:
            cursor.close()

    def __get_num_rows(self):
        try:
            cursor = self.conn.cursor()
            query = f"SELECT COUNT(*) FROM {self.db_config.table_name}"
            cursor.execute(query)
            return cursor.fetchone()[0]

        except psycopg2.Error as e:
            raise DBRuntimeException(f"Cursor Error: {e}") 
        finally:
            cursor.close()       

    def compute_formula(self, formula_str: str, num_formulas: int = -1, **kwargs) -> pd.DataFrame:
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
        order_by_clause = ', '.join(self.db_config.order_key)
        query = f"SELECT * FROM {self.db_config.db_name} ORDER BY {order_by_clause} LIMIT {num_rows}"
        try:
            df = pd.read_sql_query(query, self.connection)
            print_workbook_view(df, keep_original_labels)
        except psycopg2.Error as e:
            raise DBRuntimeException(f"Cursor Error: {e}")

    def close(self):
        self.connection.close()


def open_workbook_from_df(df: pd.DataFrame, enable_rewriting=True) -> DFWorkbook:
    return DFWorkbook(DFConfig(enable_rewriting), df)


def open_workbook_from_db(host: str, port: int, 
                 username: str, password: str, 
                 db_name: str, table_name: str,
                 primary_key: list, order_key: list,
                 enable_rewriting=True) -> DBWorkbook:
    return DBWorkbook(DBConfig(host, port, username, password,
                               db_name, table_name, primary_key,
                               order_key, enable_rewriting))


"""Helper Functions"""


def parse_formula_str(formula_str: str) -> PlanNode:
    root = parse_formula(formula_str, default_axis)
    root.populate_ref_info()
    return root


def print_workbook_view(df: pd.DataFrame, keep_original_labels=False):
    df_copy = df.copy(deep=True)
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