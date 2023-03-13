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
from typing import Callable
from dask.distributed import Client, get_client

from forms.executor.dfexecutor.lookup.utils import get_df_bins


# Partitions a dataframe based on bins and groups by the bin id.
def range_partition_df(df: pd.DataFrame or pd.Series, bins: list[any]):
    client = get_client()
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    data = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
    binned_df = pd.Series(np.searchsorted(bins, data), index=df.index)
    return [client.scatter(df[binned_df == i], workers=workers[i], direct=True) for i in range(num_cores)]


# Chunks range partitions a table based on a set of given bins.
def range_partition_df_distributed(client: Client, df: pd.DataFrame or pd.Series, bins: list[any]):
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    chunk_partitions = []
    for i in range(num_cores):
        worker_id = workers[i]
        start_idx = (i * df.shape[0]) // num_cores
        end_idx = ((i + 1) * df.shape[0]) // num_cores
        data = df[start_idx: end_idx]
        scattered_data = client.scatter(data, workers=worker_id, direct=True)
        future = client.submit(range_partition_df, scattered_data, bins, workers=worker_id)
        chunk_partitions.append(future)
    return client.gather(chunk_partitions)


# Local numpy binary search to find the values.
def vlookup_approx_local(data_partitions: list[pd.DataFrame],
                         df: pd.DataFrame,
                         lookup_func: Callable) -> pd.DataFrame:
    data = pd.concat(data_partitions)
    if len(data) == 0:
        return pd.DataFrame(dtype=object)
    values, col_idxes = data['values'], data['col_idxes']
    res = lookup_func(values, df, col_idxes)
    return res.set_index(values.index)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_approx_distributed(client: Client,
                               values: pd.Series,
                               df: pd.DataFrame,
                               col_idxes: pd.Series,
                               lookup_func: Callable) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    bins, idx_bins = get_df_bins(df, num_cores)
    data = pd.DataFrame({'values': values, 'col_idxes': col_idxes})

    binned_values = range_partition_df_distributed(client, data, bins)

    result_futures = []
    for i in range(num_cores):
        worker_id = workers[i]
        data_partitions = [binned_values[j][i] for j in range(num_cores)]
        start_idx, end_idx = idx_bins[i], idx_bins[i + 1] + 1
        scattered_df = client.scatter(df[start_idx:end_idx], workers=worker_id, direct=True)
        future = client.submit(vlookup_approx_local, data_partitions, scattered_df, lookup_func, workers=worker_id)
        result_futures.append(future)

    results = client.gather(result_futures)
    result = np.empty(len(values), dtype=results[0].dtypes[0])
    for r in results:
        np.put(result, r.index, r)
    return pd.DataFrame(result)
