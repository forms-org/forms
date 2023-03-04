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
from collections.abc import Callable
from dask.distributed import Client
from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import (
    vlookup_approx_distributed,
    range_partition_df_distributed
)
from forms.executor.dfexecutor.lookup.utils import get_df_bins


# A version of LOOKUP that reduces the problem to VLOOKUP.
def lookup_approx_distributed_reduction(client: Client,
                              values: pd.Series,
                              search_range: pd.Series,
                              result_range: pd.Series,
                              lookup_func: Callable) -> pd.DataFrame:
    df = pd.concat([search_range, result_range], axis=1)
    col_idxes = pd.Series(np.full(len(search_range), 2))
    return vlookup_approx_distributed(client, values, df, col_idxes, lookup_func)


# Local numpy binary search to find the values
def lookup_approx_local(values_partitions: list[pd.DataFrame],
                        search_range: pd.Series,
                        result_range: pd.Series,
                        lookup_func: Callable) -> pd.DataFrame:
    values = pd.concat(values_partitions)
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    res = lookup_func(values, search_range, result_range)
    return res.set_index(values.index)


# Performs a distributed LOOKUP on the given values with a Dask client.
def lookup_approx_distributed(client: Client,
                                     values: pd.Series,
                                     search_range: pd.Series,
                                     result_range: pd.Series,
                                     lookup_func: Callable) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    bins, idx_bins = get_df_bins(search_range, num_cores)

    binned_values = range_partition_df_distributed(client, values, bins)

    result_futures = []
    for i in range(num_cores):
        worker_id = workers[i]
        values_partitions = [binned_values[j][i] for j in range(num_cores)]
        start_idx, end_idx = idx_bins[i], idx_bins[i + 1] + 1
        scattered_search_range = client.scatter(search_range[start_idx:end_idx], workers=worker_id, direct=True)
        scattered_result_range = client.scatter(result_range[start_idx:end_idx], workers=worker_id, direct=True)
        future = client.submit(lookup_approx_local,
                               values_partitions,
                               scattered_search_range,
                               scattered_result_range,
                               lookup_func,
                               workers=worker_id)
        result_futures.append(future)

    results = client.gather(result_futures)
    result = np.empty(len(values))
    for r in results:
        np.put(result, r.index, r)
    return pd.DataFrame(result)
