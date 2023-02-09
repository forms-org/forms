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
from forms.executor.dfexecutor.lookup.vlookupfuncexecutor import (
    vlookup_approx_np,
    vlookup_approx_np_vector
)
from forms.executor.dfexecutor.lookup.utils import get_df_bins


# Partitions a dataframe based on bins and groups by the bin id.
def range_partition_df(df: pd.DataFrame or pd.Series, bins):
    if isinstance(df, pd.DataFrame):
        data = df.iloc[:, 0]
    else:
        data = df
    binned_df = pd.cut(data, bins, labels=False)
    df['bin_DO_NOT_USE'] = binned_df
    return df.groupby('bin_DO_NOT_USE')


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
        scattered_data = client.scatter(data, workers=worker_id)
        chunk_partitions.append(client.submit(range_partition_df, scattered_data, bins))
    return client.gather(chunk_partitions)


# Local numpy binary search to find the values
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

    """
    # It might be better to get the bins with qcut
    start_time_test = time()
    res, bins = pd.qcut(values, num_cores, retbins=True, labels=False)
    print(f"qcut time: {time() - start_time_test}")
    print(res)
    """

    values = values.to_frame()
    values['col_idxes_DO_NOT_USE'] = col_idxes
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
            scattered_values = client.scatter(values_partitions, workers=worker_id)
        else:
            scattered_values = pd.Series(dtype=object)

        start_idx = idx_bins[i]
        end_idx = idx_bins[i + 1]
        scattered_df = client.scatter(df[start_idx:end_idx], workers=worker_id)

        result_futures.append(client.submit(vlookup_approx_local, scattered_values, scattered_df))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()


def run_test():
    CORES = 4
    DF_ROWS = 1000000
    np.random.seed(1)
    test_df = pd.DataFrame(np.random.randint(0, DF_ROWS, size=(DF_ROWS, 10)))
    values, df, col_idxes = test_df.iloc[:, 0], test_df.iloc[:, 1:], pd.Series([3] * DF_ROWS)
    df = pd.concat([pd.Series(range(DF_ROWS)), df], axis=1)

    start_time = time()
    table1 = vlookup_approx_np_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    dask_client = Client(processes=True, n_workers=CORES)
    start_time = time()
    table2 = vlookup_approx_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!")


if __name__ == '__main__':
    run_test()
