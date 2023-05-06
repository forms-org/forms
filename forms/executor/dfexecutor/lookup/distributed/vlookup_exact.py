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

from time import time

from collections.abc import Callable
from dask.distributed import Client, get_client

from forms.executor.dfexecutor.lookup.utils import combine_results


# Locally hashes a dataframe with 1 column and groups it by hash.
def hash_partition_df(df: pd.DataFrame):
    client = get_client()
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    hashed_df = pd.util.hash_array(df.iloc[:, 0].to_numpy()) % num_cores
    return [client.scatter(df[hashed_df == i], workers=workers[i], direct=True) for i in range(num_cores)]


# Chunks and hashes dataframes in df_list with a Dask client.
def hash_chunk_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]):
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    chunk_partitions = []
    for df in df_list:
        df_partitions = []
        for i in range(num_cores):
            worker_id = workers[i]
            start_idx = (i * df.shape[0]) // num_cores
            end_idx = ((i + 1) * df.shape[0]) // num_cores
            data = df[start_idx: end_idx]
            scattered_data = client.scatter(data, workers=worker_id, direct=True)
            future = client.submit(hash_partition_df, scattered_data, workers=worker_id)
            df_partitions.append(future)
        chunk_partitions.append(df_partitions)
    for i in range(len(df_list)):
        chunk_partitions[i] = client.gather(chunk_partitions[i])
    return chunk_partitions


# Local hash join to get the result.
def vlookup_exact_local(data_partitions: list[pd.DataFrame],
                        df_partitions: list[pd.DataFrame],
                        lookup_func: Callable):
    data = pd.concat(data_partitions)
    if len(data) == 0:
        return pd.DataFrame(dtype=object)
    df = pd.concat(df_partitions)
    values, col_idxes = data['values'], data['col_idxes']
    res = lookup_func(values, df, col_idxes)
    return res.set_index(values.index)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_exact_distributed(client: Client,
                              values: pd.Series,
                              df: pd.DataFrame,
                              col_idxes: pd.Series,
                              lookup_func: Callable) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    data = pd.DataFrame({'values': values, 'col_idxes': col_idxes})

    start_time = time()
    data_hash_partitions, df_hash_partitions = hash_chunk_k_tables_distributed(client, [data, df])

    result_futures = []
    for i in range(num_cores):
        data_partitions = [data_hash_partitions[j][i] for j in range(num_cores)]
        df_partitions = [df_hash_partitions[j][i] for j in range(num_cores)]
        future = client.submit(vlookup_exact_local, data_partitions, df_partitions, lookup_func, workers=workers[i])
        result_futures.append(future)

    dist_time = time() - start_time
    print(f'Distributing data time: {dist_time}')

    start_time = time()
    results = client.gather(result_futures)
    exec_time = time() - start_time
    print(f'Execution time: {exec_time}')
    return combine_results(results, len(values))
