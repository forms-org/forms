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

AUX_TABLE = "FormS_A"
BASE_TABLE = "FormS_T"
TRANSLATE_TEMP_TABLE = "FormS_Trans_Temp"
TEMP_TABLE_PREFIX = "FormS_Temp_"
ROW_ID = "row_id"
START_ROW_ID = 1


class TableCatalog:
    def __init__(self, table_name: str, table_columns: list, table_column_types: list) -> None:
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_column_types = table_column_types
        self.table_column_types_dict = dict(zip(table_columns, table_column_types))

    def get_table_column(self, col_index: int) -> str:
        return self.table_columns[col_index]

    def get_column_type(self, col_index: int) -> str:
        return self.table_column_types[col_index]

    def get_column_type_by_name(self, col_name: str) -> str:
        return self.table_column_types_dict[col_name]
