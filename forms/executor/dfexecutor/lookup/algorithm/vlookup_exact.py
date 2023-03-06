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

from forms.executor.dfexecutor.lookup.utils.utils import set_dtype


def vlookup_exact_loops(values, df, col_idxes) -> pd.DataFrame:
    # Performs a scan of array ARR to find value VALUE.
    # If the value is not found, returns -1.
    def exact_scan_search(value: any, arr: list) -> int:
        for i in range(len(arr)):
            if arr[i] == value:
                return i
        return -1

    df_arr: list = []
    for i in range(len(values)):
        value, col_idx = values[i], col_idxes[i]
        value_idx = exact_scan_search(value, list(df.iloc[:, 0]))
        result = np.nan
        if value_idx != -1:
            result = df.iloc[value_idx, col_idx - 1]
        df_arr.append(result)
    return pd.DataFrame(df_arr)


def vlookup_exact_hash(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    cache = {}
    for i in range(df.shape[0]):
        value = search_range.iloc[i]
        if value not in cache:
            cache[value] = i
    result_arr = [np.nan] * len(values)
    for i in range(len(values)):
        value, col_idx = values.iloc[i], col_idxes.iloc[i]
        if value in cache:
            value_idx = cache[value]
            result = df.iloc[value_idx, col_idx - 1]
            result_arr[i] = result
    return pd.DataFrame(result_arr)


def vlookup_exact_hash_vector(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    idxes = list(enumerate(search_range))
    idxes.reverse()
    cache = {v: i for i, v in idxes}
    nan_mask = values.isin(set(cache.keys())) ^ True
    nan_idxes = nan_mask.to_numpy().nonzero()
    result_idxes = values.map(cache.get)
    np.put(result_idxes.to_numpy(), nan_idxes, 1)
    row_res = np.take(df.to_numpy(), result_idxes, axis=0, mode='clip')
    res = row_res[np.arange(len(col_idxes)), col_idxes.astype(int) - 1]
    return set_dtype(res, nan_idxes)


def vlookup_exact_pd_merge(values, df, col_idxes) -> pd.DataFrame:
    left = pd.DataFrame({"join_col": values})
    right = df.rename(columns={df.columns[0]: "join_col"}).drop_duplicates(subset="join_col")
    merged = pd.merge(left, right, how="left", on="join_col").to_numpy()
    chosen = merged[np.arange(len(col_idxes)), col_idxes.astype(int) - 1]
    res = pd.Series(chosen, index=values.index).sort_index().to_numpy()
    return set_dtype(res)
