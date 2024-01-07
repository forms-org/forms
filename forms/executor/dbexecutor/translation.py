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

from forms.core.catalog import BASE_TABLE, ROW_ID
from forms.core.config import DBExecContext
from forms.executor.dbexecutor.dbexecnode import DBExecNode, DBFuncExecNode, DBRefExecNode

from psycopg2 import sql

from forms.utils.functions import Function


def translate(subtree: DBExecNode, exec_context: DBExecContext) -> sql.SQL:
    ret_sql = None
    if isinstance(subtree, DBRefExecNode):
        ret_sql = sql.SQL(
            """
            SELECT {col_name}
            FROM {table_name}
            WHERE {row_id} >= {formula_index_start}
            AND {row_id} < {formula_index_end}
            """
        ).format(
            col_name=sql.Identifier(subtree.cols[0]),
            table_name=sql.Identifier(BASE_TABLE),
            row_id=sql.Identifier(ROW_ID),
            formula_idx_start=sql.Identifier(exec_context.formula_idx_start),
            formula_idx_end=sql.Identifier(exec_context.formula_idx_end),
        )
    elif isinstance(subtree, DBFuncExecNode):
        if subtree.translatable_to_window:
            ret_sql = translate_to_one_window_query(subtree, exec_context)
        elif subtree.function == Function.LOOKUP:
            ret_sql = translate_lookup_function(subtree, exec_context)
        elif subtree.function == Function.MATCH:
            ret_sql = translate_match_function(subtree, exec_context)
        else:
            ret_sql = translate_using_udf(subtree, exec_context)
    else:
        assert False
    return ret_sql


def translate_to_one_window_query(subtree: DBFuncExecNode, exec_context: DBExecContext) -> sql.SQL:
    pass


def translate_lookup_function(subtree: DBFuncExecNode, exec_context: DBExecContext) -> sql.SQL:
    pass


def translate_match_function(subtree: DBFuncExecNode, exec_context: DBExecContext) -> sql.SQL:
    pass


def translate_using_udf(subtree: DBFuncExecNode, exec_context: DBExecContext) -> sql.SQL:
    pass
