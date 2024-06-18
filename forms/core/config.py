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


from forms.core.catalog import TableCatalog


class DFConfig:
    def __init__(self, enable_rewriting):
        self.df_enable_rewriting = enable_rewriting


class DBConfig:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        db_name: str,
        table_name: str,
        primary_key: list,
        order_key: list,
        enable_rewriting: bool,
        enable_pipelining: bool,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.primary_key = primary_key
        self.order_key = order_key
        self.enable_pipelining = enable_pipelining
        self.db_enable_rewriting = enable_rewriting


class DFExecContext:
    def __init__(self, formula_idx_start: int, formula_idx_end: int, axis: int):
        self.formula_idx_start = formula_idx_start
        self.formula_idx_end = formula_idx_end
        self.axis = axis


class DBExecContext:
    def __init__(
        self, conn, cursor, base_table: TableCatalog, formula_id_start: int, formula_id_end: int
    ):
        self.conn = conn
        self.cursor = cursor
        self.base_table = base_table
        self.formula_id_start = formula_id_start
        self.formula_id_end = formula_id_end
        self.tmp_table_index = 0
