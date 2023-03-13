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

from forms.executor.dfexecutor.lookup.utils import approx_binary_search, set_dtype


def lookup_binary_search(values, search_range, result_range) -> pd.DataFrame:
    df_arr: list = [np.nan] * len(values)
    for i in range(len(values)):
        value_idx = approx_binary_search(values[i], list(search_range))
        if value_idx != -1:
            df_arr[i] = result_range[value_idx]
    return pd.DataFrame(df_arr)


def lookup_sort_merge(values, search_range, result_range) -> pd.DataFrame:
    # Sort values and preserve index for later
    sorted_values = list(enumerate(values))
    sorted_values.sort(key=lambda x: x[1])

    # Use sorted_values as the left and search_range as the right
    left_idx, right_idx = 0, 0
    df_arr: list = [np.nan] * len(values)
    while left_idx < len(values):
        searching_val = sorted_values[left_idx][1]
        while right_idx < len(search_range) and search_range[right_idx] <= searching_val:
            right_idx += 1
        stop_val = search_range[right_idx] if right_idx < len(search_range) else np.infty
        while left_idx < len(values) and sorted_values[left_idx][1] < stop_val:
            if right_idx != 0:
                df_arr[sorted_values[left_idx][0]] = result_range[right_idx - 1]
            left_idx += 1

    return pd.DataFrame(df_arr)


def lookup_np(values, search_range, result_range) -> pd.DataFrame:
    value_idxes = np.searchsorted(search_range.to_numpy(), values.to_numpy(), side="left")
    result_arr = [np.nan] * len(values)
    for i in range(len(values)):
        value, value_idx = values.iloc[i], value_idxes[i]
        if value_idx >= len(search_range) or value != search_range.iloc[value_idx]:
            value_idx -= 1
        if value_idx != -1:
            result_arr[i] = result_range.iloc[value_idx]
    return pd.DataFrame(result_arr)


def lookup_np_vector(values: pd.Series, search_range: pd.Series, result_range: pd.Series) -> pd.DataFrame:
    value_idxes = np.searchsorted(search_range.to_numpy(), values.to_numpy(), side="left")
    greater_than_length = np.greater_equal(value_idxes, len(search_range))
    value_idxes_no_oob = np.minimum(value_idxes, len(search_range) - 1)
    search_range_values = np.take(search_range, value_idxes_no_oob)
    approximate_matches = values.to_numpy() != search_range_values.to_numpy()
    combined = np.logical_or(greater_than_length, approximate_matches).astype(int)
    adjusted_idxes = value_idxes - combined
    res = np.take(result_range, adjusted_idxes).to_numpy()
    nan_idxes = (adjusted_idxes == -1).nonzero()
    return set_dtype(res, nan_idxes)


def lookup_pd_merge(values: pd.Series, search_range: pd.Series, result_range: pd.Series) -> pd.DataFrame:
    values = values.sort_values()
    left = pd.DataFrame({"join_col": values.reset_index(drop=True)})
    right = pd.DataFrame({"join_col": search_range, "results": result_range})
    res = pd.merge_asof(left, right, on="join_col")
    return pd.DataFrame(res['results'].to_numpy(), index=values.index).sort_index()
