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

from forms.executor.executionnode import (
    RefExecutionNode,
    ExecutionNode,
)
from forms.executor.dfexecutor.utils import get_single_value


# Splits the df into bins for range partitioning.
def get_df_bins(df, num_cores):
    search_keys = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
    idx_bins = []
    bins = []
    for i in range(num_cores):
        start_idx = (i * df.shape[0]) // num_cores
        end_idx = ((i + 1) * df.shape[0]) // num_cores
        if start_idx == 0:
            idx_bins.append(0)
        idx_bins.append(end_idx - 1)
        bins.append(search_keys[end_idx - 1])
    bins.pop(-1)
    return bins, idx_bins


# Gets bins for df based on the quantiles of values. Only works with numerical inputs.
def get_value_bins(values, df, num_cores):
    search_keys = (df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df).to_numpy()
    percentiles = [i / num_cores for i in range(num_cores + 1)]
    quantiles = np.quantile(values, percentiles)
    idx_bins = np.searchsorted(search_keys, quantiles)
    bins = np.take(search_keys, idx_bins)
    return bins, idx_bins


# Helper to infer the data type of a lookup result.
def set_dtype(res, nan_idxes=None):
    if nan_idxes is None:
        nan_idxes = []
    if np.float64 > res.dtype:
        res = res.astype(np.float64)
    if len(nan_idxes) > 0:
        np.put(res, nan_idxes, np.nan)
    res_type = res.dtype
    if np.issubdtype(res_type, np.integer):
        res_type = np.float64
    return pd.DataFrame(res).astype(res_type)


# Creates a random dataframe with string values for benchmarking and testing.
def create_alpha_df(rows, print_df=False):
    import string
    import numpy as np
    from time import time

    np.random.seed(1)
    start_time = time()
    test_strings = [i + j + k + x + y
                    for i in string.ascii_lowercase
                    for j in string.ascii_lowercase
                    for k in string.ascii_lowercase
                    for x in string.ascii_lowercase
                    for y in string.ascii_lowercase
                    ]
    search_keys = np.random.choice(test_strings, rows, replace=False)
    search_keys.sort()
    result1 = np.arange(rows)
    result2 = np.arange(rows, 0, -1)
    # result1 = np.random.choice(test_strings, rows, replace=True)
    # result2 = np.random.choice(test_strings, rows, replace=True)
    df = pd.DataFrame({0: search_keys, 1: result1, 2: result2})
    values = pd.Series(np.random.choice(test_strings, rows, replace=False))
    # col_idxes = pd.concat([pd.Series(np.full(rows // 2, 2)), pd.Series(np.full(rows // 2, 3))])
    col_idxes = pd.Series(np.full(rows, 3))
    print(f"Generated input in {time() - start_time} seconds.")
    if print_df:
        print(pd.DataFrame({0: search_keys, 1: result1, 2: result2, "values": values, "col_idxes": col_idxes}))
    return values, df, col_idxes


# Performs binary search for value VALUE in array ARR. Assumes the array is sorted.
# If the value is found, returns the index of the value.
# If the value is not found, returns the index of the value before the sorted value.
# If the value is less than the first value, return -1.
def approx_binary_search(value: any, arr: list) -> int:
    assert len(arr) > 0
    if value < arr[0]:
        return -1
    low, high = 0, len(arr) - 1
    while low <= high:
        mid = (high + low) // 2
        if arr[mid] < value:
            low = mid + 1
        elif arr[mid] > value:
            high = mid - 1
        else:
            return mid
    return (high + low) // 2


# Gets a literal value from a child and puts it in a dataframe with SIZE rows.
def get_literal_value(child: ExecutionNode, size: int) -> pd.DataFrame:
    value = get_single_value(child)
    result = value
    if not isinstance(value, pd.DataFrame):
        result = pd.DataFrame([value] * size)
    elif value.shape[0] == 1:
        result = pd.DataFrame([value.iloc[0, 0]] * size)
    return result


# Obtains the full dataframe for this lookup.
def get_df(child: RefExecutionNode) -> pd.DataFrame:
    ref = child.ref
    df: pd.DataFrame = child.table.get_table_content()
    return df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1]


# Clean string values by removing quotations.
def clean_string_values(values: pd.DataFrame):
    return values.applymap(lambda x: x.strip('"').strip("'") if isinstance(x, str) else x)


# Clean column indices by casting floats to integers.
def clean_col_idxes(col_idxes: pd.DataFrame):
    return col_idxes.applymap(lambda x: int(x))
