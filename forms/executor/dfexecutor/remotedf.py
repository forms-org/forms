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
import numpy as np
import pandas as pd
import math

from forms.executor.dfexecutor.remotepartition import RemotePartition

max_num_row_partitions = 128
max_num_col_partitions = 16


def partition_df(df: pd.DataFrame, num_row_partitions=1, num_col_partitions=1):
    num_rows, num_cols = df.shape
    real_row_partitions = min(num_row_partitions, num_rows, max_num_row_partitions)
    real_col_partitions = min(num_col_partitions, num_cols, max_num_col_partitions)
    partitions = np.empty(shape=(real_row_partitions, real_col_partitions), dtype=pd.DataFrame)
    for i in range(real_row_partitions):
        for j in range(real_col_partitions):
            first_row = math.floor((i * num_rows) / real_row_partitions)
            last_row = math.floor(((i + 1) * num_rows) / real_row_partitions)
            first_col = math.floor((j * num_cols) / real_col_partitions)
            last_col = math.floor(((j + 1) * num_cols) / real_col_partitions)
            partitions[i][j] = df.iloc[first_row:last_row, first_col:last_col]
    return partitions


def find_rows_and_cols(partitions: np.ndarray):
    rows = np.array([[one_partition.shape[0] for one_partition in one_row] for one_row in partitions])
    cols = np.array([[one_partition.shape[1] for one_partition in one_row] for one_row in partitions])
    return rows, cols


class RemoteDF:
    def __init__(self, remote_obj_array: np.ndarray, rows: np.ndarray, cols: np.ndarray):
        shape = remote_obj_array.shape
        remote_partitions = np.empty(shape=shape, dtype=RemotePartition)
        total_rows = 0
        total_cols = 0
        for i in range(shape[0]):
            for j in range(shape[1]):
                num_row = rows[i][j]
                num_col = cols[i][j]
                remote_obj = remote_obj_array[i][j]
                remote_partitions[i][j] = RemotePartition(num_row, num_col, remote_obj)
                if i == 0:
                    total_cols += num_col
                if j == 0:
                    total_rows += num_row
        self.remote_partitions = remote_partitions
        self.num_rows = total_rows
        self.num_cols = total_cols

    def get_df_content(self) -> pd.DataFrame:
        return pd.concat(
            [
                pd.concat(
                    [one_partition.remote_obj.get_computed_result() for one_partition in one_row],
                    axis=1,
                    ignore_index=True,
                )
                for one_row in self.remote_partitions
            ],
            axis=0,
            ignore_index=True,
        )

    def get_num_of_rows(self) -> int:
        return self.num_rows

    def get_num_of_cols(self) -> int:
        return self.num_cols
