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
import pandas as pd
from dask.distributed import Client, get_client
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_hash_vector


# Locally hashes a dataframe with 1 column and groups it by hash.
def hash_partition_df(df: pd.DataFrame, workers):
    client = get_client()
    hashed_df = pd.util.hash_array(df.iloc[:, 0].to_numpy()) % len(workers)
    df['hash_DO_NOT_USE'] = hashed_df
    grouped = df.groupby('hash_DO_NOT_USE')
    scattered_groups = []
    for i in range(len(workers)):
        df = grouped.get_group(i) if i in grouped.groups else pd.Series(dtype=object)
        res = client.scatter(df, workers=workers[i], direct=True)
        scattered_groups.append(res)
    return scattered_groups


# Chunks and hashes dataframes in df_list with a Dask client.
def hash_chunk_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]):
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)
    chunk_partitions = {}
    for df_idx in range(len(df_list)):
        chunk_partitions[df_idx] = []
        df = df_list[df_idx]
        for i in range(num_cores):
            worker_id = workers[i]
            start_idx = (i * df.shape[0]) // num_cores
            end_idx = ((i + 1) * df.shape[0]) // num_cores
            data = df[start_idx: end_idx]
            scattered_data = client.scatter(data, workers=worker_id, direct=True)
            chunk_partitions[df_idx].append(client.submit(hash_partition_df, scattered_data, workers))
    for df_idx in range(len(df_list)):
        chunk_partitions[df_idx] = client.gather(chunk_partitions[df_idx])
    return chunk_partitions


# Local hash join to get the result.
def vlookup_exact_hash_local(values_partitions, df_partitions):
    values = pd.concat(values_partitions)
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    df = pd.concat(df_partitions)
    values, col_idxes = values.iloc[:, 0], values.loc[:, 'col_idxes_DO_NOT_USE']
    res = vlookup_exact_hash_vector(values, df, col_idxes)
    return res.set_index(values.index)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_exact_hash_distributed(client: Client,
                                   values: pd.Series,
                                   df: pd.DataFrame,
                                   col_idxes: pd.Series) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)

    values = values.to_frame()
    values['col_idxes_DO_NOT_USE'] = col_idxes

    chunk_partitions = hash_chunk_k_tables_distributed(client, [values, df])

    result_futures = []
    for i in range(num_cores):
        values_partitions = [chunk_partitions[0][j][i] for j in range(num_cores)]
        df_partitions = [chunk_partitions[1][j][i] for j in range(num_cores)]
        result_futures.append(client.submit(vlookup_exact_hash_local, values_partitions, df_partitions))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()
