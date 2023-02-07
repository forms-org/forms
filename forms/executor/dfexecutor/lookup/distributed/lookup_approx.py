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
from dask.distributed import Client
from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import (
    vlookup_approx_distributed,
    range_partition_df_distributed
)
from forms.executor.dfexecutor.lookup.lookupfuncexecutor import (
    lookup_binary_search_np,
    lookup_binary_search_np_vector
)


def lookup_approx_distributed_reduction(client, values, search_range, result_range) -> pd.DataFrame:
    df = pd.concat([search_range, result_range], axis=1)
    col_idxes = pd.Series([2] * len(search_range))
    return vlookup_approx_distributed(client, values, df, col_idxes)


# Local numpy binary search to find the values
def lookup_approx_local(values, df) -> pd.DataFrame:
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    search_range, result_range = df.iloc[:, 0], df.iloc[:, 1]
    res = lookup_binary_search_np(values, search_range, result_range)
    return res.set_index(values.index)


# Local numpy binary search to find the values
def lookup_approx_local_vector(values, df) -> pd.DataFrame:
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    search_range, result_range = df.iloc[:, 0], df.iloc[:, 1]
    res = lookup_binary_search_np_vector(values, search_range, result_range)
    return res.set_index(values.index)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def lookup_approx_distributed(client: Client,
                              values: pd.Series,
                              search_range: pd.Series,
                              result_range: pd.Series) -> pd.DataFrame:
    workers = list(client.scheduler_info()['workers'].keys())
    num_cores = len(workers)

    df = pd.concat([search_range, result_range], axis=1)
    idx_bins = []
    bins = []
    for i in range(num_cores):
        start_idx = (i * df.shape[0]) // num_cores
        end_idx = ((i + 1) * df.shape[0]) // num_cores
        if start_idx == 0:
            idx_bins.append(0)
            bins.append(-float('inf'))
        idx_bins.append(end_idx)
        bins.append(df.iloc[end_idx - 1, 0])
    bins[-1] = float('inf')

    binned_values = range_partition_df_distributed(client, values, bins)

    result_futures = []
    for i in range(num_cores):
        worker_id = workers[i]
        values_partitions = []
        for j in range(num_cores):
            if i in binned_values[j].groups:
                group = binned_values[j].get_group(i)
                values_partitions.append(group)
        if len(values_partitions) > 0:
            scattered_values = client.scatter(pd.concat(values_partitions), workers=worker_id)
        else:
            scattered_values = pd.Series(dtype=object)

        start_idx = idx_bins[i]
        end_idx = idx_bins[i + 1]
        scattered_df = client.scatter(df[start_idx:end_idx], workers=worker_id)

        result_futures.append(client.submit(lookup_approx_local_vector, scattered_values, scattered_df))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()


def run_test():
    CORES = 4
    DF_ROWS = 1000000
    np.random.seed(1)
    test_df = pd.DataFrame(np.random.randint(0, DF_ROWS, size=(DF_ROWS, 3)))
    values, search_range, result_range = test_df.iloc[:, 0], pd.Series(range(DF_ROWS)), test_df.iloc[:, 2]

    start_time = time()
    table1 = lookup_binary_search_np_vector(values, search_range, result_range)
    print(f"Finished local LOOKUP in {time() - start_time} seconds.")

    dask_client = Client(processes=True, n_workers=CORES)
    start_time = time()
    table2 = lookup_approx_distributed(dask_client, values, search_range, result_range)
    print(f"Finished distributed LOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!")


if __name__ == '__main__':
    run_test()
