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

from forms.executor.dfexecutor.lookup.algorithm.lookup_approx import lookup_pd_merge, lookup_np_vector
from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_pd_merge, vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_pd_merge

from forms.executor.dfexecutor.lookup.distributed.lookup_approx import lookup_approx_distributed
from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import vlookup_approx_distributed
from forms.executor.dfexecutor.lookup.distributed.vlookup_exact import vlookup_exact_distributed


# Arbitrary constant for number of rows to switch to distributed
LOCAL_RECORD_LIMIT = 500_000


def lookup(values: pd.Series,
           search_range: pd.Series,
           result_range: pd.Series,
           dask_client=None) -> pd.DataFrame:

    distributed_enabled = len(values) > LOCAL_RECORD_LIMIT and dask_client is not None
    numerical = search_range.dtype <= np.float64 and values.dtype <= np.float64

    if search_range.dtype > values.dtype:
        values = values.astype(search_range.dtype)
    else:
        search_range = search_range.astype(values.dtype)

    if numerical and distributed_enabled:
        return lookup_approx_distributed(dask_client, values, search_range, result_range, lookup_pd_merge)
    elif numerical:
        return lookup_pd_merge(values, search_range, result_range)
    elif distributed_enabled:
        return lookup_approx_distributed(dask_client, values, search_range, result_range, lookup_np_vector)
    else:
        return lookup_np_vector(values, search_range, result_range)


def vlookup(values: pd.Series,
            df: pd.DataFrame,
            col_idxes: pd.Series,
            approx=True,
            dask_client=None) -> pd.DataFrame:

    search_range = df.iloc[:, 0]
    distributed_enabled = len(values) > LOCAL_RECORD_LIMIT and dask_client is not None
    numerical = values.dtype <= np.float64 and search_range.dtype <= np.float64

    df, col_idxes = compact_input(df, col_idxes)

    if search_range.dtype > values.dtype:
        values = values.astype(search_range.dtype)
    else:
        df = df.astype({df.columns[0]: values.dtype})

    if approx:
        if df.shape[1] == 2:
            return lookup(values, df.iloc[:, 0], df.iloc[:, 1], dask_client)
        elif numerical and distributed_enabled:
            return vlookup_approx_distributed(dask_client, values, df, col_idxes, vlookup_approx_pd_merge)
        elif numerical:
            return vlookup_approx_pd_merge(values, df, col_idxes)
        elif distributed_enabled:
            return vlookup_approx_distributed(dask_client, values, df, col_idxes, vlookup_approx_np_vector)
        else:
            return vlookup_approx_np_vector(values, df, col_idxes)

    if distributed_enabled:
        return vlookup_exact_distributed(dask_client, values, df, col_idxes, vlookup_exact_pd_merge)
    else:
        return vlookup_exact_pd_merge(values, df, col_idxes)


# Remove unneeded columns from DF based on COL_IDXES.
# If most columns are needed, skip the optimization.
def compact_input(df, col_idxes) -> tuple[pd.DataFrame, pd.Series]:
    unique_vals = set(col_idxes.unique())
    if len(unique_vals) / df.shape[1] > 0.8:
        return df, col_idxes
    unique_vals.add(1)
    unique_vals = np.sort(list(unique_vals))
    compact_df = pd.concat([df.iloc[:, i - 1] for i in unique_vals], axis=1)
    if len(unique_vals) > 1:
        compact_col_idxes = np.empty(len(col_idxes))
        for i, j in enumerate(unique_vals):
            compact_col_idxes[col_idxes == j] = i + 1
    else:
        compact_col_idxes = np.full(len(col_idxes), 2)
    compact_col_idxes = pd.Series(compact_col_idxes)
    return compact_df, compact_col_idxes

