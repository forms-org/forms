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

from psycopg2 import sql
from forms.core.catalog import START_ROW_ID, TableCatalog, BASE_TABLE, AUX_TABLE, ROW_ID

from forms.core.config import DBConfig, DBExecContext, DFConfig, DFExecContext
from forms.executor.dbexecutor.dbexecutor import DBExecutor
from forms.executor.dfexecutor.dfexecutor import DFExecutor

from forms.parser.parser import parse_formula
from forms.planner.plannode import PlanNode
from forms.planner.planrewriter import PlanRewriter
from forms.utils.functions import FunctionExecutor
from forms.utils.generic import get_columns_and_types
from forms.utils.metrics import MetricsTracker
from forms.utils.validator import validate

from forms.utils.exceptions import DBConfigException, DBRuntimeException, FormSException
from forms.utils.reference import DEFAULT_AXIS
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
            validate(FunctionExecutor.DF_EXECUTOR, self.df.shape[0], self.df.shape[1], root)
            root = PlanRewriter(self.df_config).rewrite_plan(root)

            if num_formulas <= 0:
                num_formulas = self.df.shape[0]
            exec_context = DFExecContext(0, num_formulas, DEFAULT_AXIS)
            executor = DFExecutor(self.df_config, exec_context, self.metrics_tracker)
            res = executor.execute_formula_plan(self.df, root)
            executor.clean_up()

            return res
        except FormSException as e:
            print(f"An error occurred: {e}")
            traceback.print_exception(*sys.exc_info())

    def print_workbook(self, num_rows=10, keep_original_labels=False):
        print_workbook_view(self.df.head(num_rows), keep_original_labels)

    def close(self):
        self.df = None


class DBWorkbook(Workbook):
    def __init__(self, db_config: DBConfig):
        super().__init__()
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        try:
            self.connection = psycopg2.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.username,
                password=db_config.password,
                dbname=db_config.db_name,
            )
            self.cursor = self.connection.cursor()

            if not self.__check_primary_key():
                raise DBConfigException(
                    f"The specified primary key does not match the table {self.db_config.table_name}'s primary key"
                )

            if not self.__check_order_key():
                raise DBConfigException(
                    f"The specified order key is not part of the table {self.db_config.table_name}'s columns"
                )

            column_names, column_types = self.__get_columns_and_types()
            self.num_columns = len(column_names)
            self.num_rows = self.__get_num_rows()

            self.base_table = TableCatalog(BASE_TABLE, column_names, column_types)
            self.__build_auxiliary_and_base_tables()

            self.connection.commit()
        except psycopg2.Error as e:
            self.__clean_up()
            raise DBRuntimeException(f"DB Runtime Error: {e}")
        except DBConfigException as e:
            self.__clean_up()
            raise e

    def __clean_up(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.rollback()
            self.connection.close()

    def __check_primary_key(self) -> bool:
        cursor = self.cursor
        query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc 
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name 
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY' 
          AND tc.table_name = %s;
        """
        cursor.execute(query, (self.db_config.table_name,))
        primary_key_columns = [row[0] for row in cursor.fetchall()]
        return set(primary_key_columns) == set(self.db_config.primary_key)

    def __check_order_key(self) -> bool:
        cursor = self.cursor
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s;
        """
        cursor.execute(query, (self.db_config.table_name,))
        table_columns = [row[0] for row in cursor.fetchall()]
        return all(column in table_columns for column in self.db_config.order_key)

    def __get_num_rows(self):
        cursor = self.cursor
        query = f"SELECT COUNT(*) FROM {self.db_config.table_name}"
        cursor.execute(query)
        num_rows = cursor.fetchone()[0]
        return num_rows

    def __get_columns_and_types(self) -> tuple[list, list]:
        return get_columns_and_types(self.cursor, self.db_config.table_name)

    def __build_auxiliary_and_base_tables(self):
        cur = self.cursor
        pk_cols_defs = sql.SQL(", ").join(
            sql.SQL("{} {}").format(
                sql.Identifier(col), sql.SQL(self.base_table.get_column_type_by_name(col))
            )
            for col in self.db_config.primary_key
        )
        pk_cols_sql = sql.SQL(", ").join(sql.Identifier(col) for col in self.db_config.primary_key)
        order_cols_sql = sql.SQL(", ").join(sql.Identifier(col) for col in self.db_config.order_key)

        # Create Auxiliary Table (A)
        cur.execute(
            sql.SQL(
                """
            CREATE TABLE IF NOT EXISTS {auxiliary_table_name} (
                {row_id} SERIAL PRIMARY KEY,
                {pk_cols_defs}
            );
        """
            ).format(
                auxiliary_table_name=sql.Identifier(AUX_TABLE),
                row_id=sql.Identifier(ROW_ID),
                pk_cols_defs=pk_cols_defs,
            )
        )

        cur.execute(
            sql.SQL(
                """
                            SELECT EXISTS (SELECT 1 FROM {auxiliary_table_name}
                            LIMIT 1);
        """
            ).format(auxiliary_table_name=sql.Identifier(AUX_TABLE))
        )
        is_empty = not cur.fetchone()[0]

        # Populate Auxiliary Table with data from the Input Table
        if is_empty:
            cur.execute(
                sql.SQL(
                    """
                INSERT INTO {auxiliary_table_name} ({pk_cols})
                SELECT {pk_cols}
                FROM {input_table_name}
                ORDER BY {order_cols};
            """
                ).format(
                    auxiliary_table_name=sql.Identifier(AUX_TABLE),
                    pk_cols=pk_cols_sql,
                    input_table_name=sql.Identifier(self.db_config.table_name),
                    order_cols=order_cols_sql,
                )
            )

        # Create the Base View
        cur.execute(
            sql.SQL(
                """
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT {auxiliary_table_name}.{row_id}, {input_table_name}.*
            FROM {input_table_name}
            JOIN {auxiliary_table_name} ON {join_condition}
            """
            ).format(
                view_name=sql.Identifier(BASE_TABLE),
                row_id=sql.Identifier(ROW_ID),
                auxiliary_table_name=sql.Identifier(AUX_TABLE),
                input_table_name=sql.Identifier(self.db_config.table_name),
                join_condition=sql.SQL(" AND ").join(
                    sql.SQL("{input_table_name}.{} = {auxiliary_table_name}.{}").format(
                        sql.Identifier(col), sql.Identifier(col)
                    )
                    for col in self.db_config.primary_key
                ),
            )
        )

    def compute_formula(self, formula_str: str, num_formulas: int = -1, **kwargs) -> pd.DataFrame:
        try:
            root = parse_formula_str(formula_str)
            validate(FunctionExecutor.DB_EXECUTOR, self.num_rows, self.num_columns, root)
            root = PlanRewriter(self.db_config).rewrite_plan(root)

            if num_formulas <= 0:
                num_formulas = self.num_rows
            exec_context = DBExecContext(
                self.connection, self.cursor, self.base_table, START_ROW_ID, START_ROW_ID + num_formulas
            )
            executor = DBExecutor(self.db_config, exec_context, self.metrics_tracker)
            res = executor.execute_formula_plan(root)
            executor.clean_up()

            return res
        except FormSException as e:
            print(f"An error occurred: {e}")
            traceback.print_exception(*sys.exc_info())

    def print_workbook(self, num_rows=10, keep_original_labels=False):
        order_by_clause = ", ".join(self.db_config.order_key)
        query = f"SELECT * FROM {self.db_config.db_name} ORDER BY {order_by_clause} LIMIT {num_rows}"
        try:
            df = pd.read_sql_query(query, self.connection)
            print_workbook_view(df, keep_original_labels)
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")
            traceback.print_exception(*sys.exc_info())

    def close(self):
        try:
            cur = self.cursor
            cur.execute(
                sql.SQL("DROP VIEW IF EXISTS {view_name}").format(view_name=sql.Identifier(BASE_TABLE))
            )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {table_name}").format(table_name=sql.Identifier(AUX_TABLE))
            )
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"An error occurred: {e}")
            traceback.print_exception(*sys.exc_info())
        finally:
            cur.close()
            self.connection.close()


def from_df(df: pd.DataFrame, enable_rewriting=True) -> DFWorkbook:
    return DFWorkbook(DFConfig(enable_rewriting), df)


def from_db(
    host: str,
    port: int,
    username: str,
    password: str,
    db_name: str,
    table_name: str,
    primary_key: list,
    order_key: list,
    enable_rewriting=True,
) -> DBWorkbook:
    try:
        return DBWorkbook(
            DBConfig(
                host,
                port,
                username,
                password,
                db_name,
                table_name,
                primary_key,
                order_key,
                enable_rewriting,
            )
        )
    except FormSException as e:
        print(f"An error occurred: {e}")
        traceback.print_exception(*sys.exc_info())


"""Helper Functions"""


def parse_formula_str(formula_str: str) -> PlanNode:
    root = parse_formula(formula_str, DEFAULT_AXIS)
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
