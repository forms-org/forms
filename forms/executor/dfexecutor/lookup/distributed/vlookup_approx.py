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
from dask.distributed import Client, get_client

from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.utils import get_df_bins


# Partitions a dataframe based on bins and groups by the bin id.
def range_partition_df(df: pd.DataFrame or pd.Series, bins, workers):
    client = get_client()
    data = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
    binned_df = pd.Series(np.searchsorted(bins, data), index=df.index)
    df['bin_DO_NOT_USE'] = binned_df
    grouped = df.groupby('bin_DO_NOT_USE')
    scattered_groups = []
    for i in range(len(workers)):
        df = grouped.get_group(i) if i in grouped.groups else None
        res = client.scatter(df, workers=workers[i], direct=True)
        scattered_groups.append(res)
    return scattered_groups


# Chunks range partitions a table based on a set of given bins.
def range_partition_df_distributed(client: Client, df: pd.DataFrame or pd.Series, bins):
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    chunk_partitions = []
    for i in range(num_cores):
        worker_id = workers[i]
        start_idx = (i * df.shape[0]) // num_cores
        end_idx = ((i + 1) * df.shape[0]) // num_cores
        data = df[start_idx: end_idx]
        scattered_data = client.scatter(data, workers=worker_id, direct=True)
        chunk_partitions.append(client.submit(range_partition_df, scattered_data, bins, workers))
    res = client.gather(chunk_partitions)
    return res


# Local numpy binary search to find the values.
def vlookup_approx_local(values_partitions, df) -> pd.DataFrame:
    values = pd.concat(values_partitions)
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    values, col_idxes = values.iloc[:, 0], values.loc[:, 'col_idxes_DO_NOT_USE']
    res = vlookup_approx_np_vector(values, df, col_idxes)
    return res.set_index(values.index)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_approx_distributed(client: Client,
                               values: pd.Series,
                               df: pd.DataFrame,
                               col_idxes: pd.Series) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)

    bins, idx_bins = get_df_bins(df, num_cores)
    values = values.to_frame()
    values['col_idxes_DO_NOT_USE'] = col_idxes

    binned_values = range_partition_df_distributed(client, values, bins)

    result_futures = []
    for i in range(num_cores):
        worker_id = workers[i]
        values_partitions = [binned_values[j][i] for j in range(num_cores)]
        start_idx, end_idx = idx_bins[i], idx_bins[i + 1] + 1
        scattered_df = client.scatter(df[start_idx:end_idx], workers=worker_id, direct=True)
        result_futures.append(client.submit(vlookup_approx_local, values_partitions, scattered_df))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()
