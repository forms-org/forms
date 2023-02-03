import numpy as np
import pandas as pd
from time import time
from dask.distributed import Client
from forms.executor.dfexecutor.lookup.vlookupfuncexecutor import vlookup_approx_np


# Partitions a dataframe based on bins and groups by the bin id.
def range_partition_df(df: pd.DataFrame, bins):
    binned_df = pd.cut(df.iloc[:, 0], bins, labels=False)
    df['bin_DO_NOT_USE'] = binned_df
    return df.groupby('bin_DO_NOT_USE')


# Chunks range partitions a table based on a set of given bins.
def range_partition_df_distributed(client: Client, df: pd.DataFrame, bins):
    workers = list(client.scheduler_info()['workers'].keys())
    chunk_partitions = []
    for i in range(CORES):
        worker_id = workers[i]
        start_idx = (i * df.shape[0]) // CORES
        end_idx = ((i + 1) * df.shape[0]) // CORES
        data = df[start_idx: end_idx]
        scattered_data = client.scatter(data, workers=worker_id)
        chunk_partitions.append(client.submit(range_partition_df, scattered_data, bins))
    return client.gather(chunk_partitions)


# Local numpy binary search to find the values
def vlookup_approx_local(values, df) -> pd.DataFrame:
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    values, col_idxes = values.iloc[:, 0], values.loc[:, 'col_idxes_DO_NOT_USE']
    search_range = df.iloc[:, 0]
    result_df = pd.Series(index=values.index, dtype=object)
    value_idxes = np.searchsorted(list(search_range), list(values), side="left")
    for i in range(len(values)):
        value, value_idx, col_idx = values.iloc[i], value_idxes[i], col_idxes.iloc[i]
        if value_idx >= len(search_range) or value != search_range.iloc[value_idx]:
            value_idx -= 1
        if value_idx != -1:
            result_df.iloc[i] = df.iloc[value_idx, col_idx - 1]
    return pd.DataFrame(result_df)


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_approx_distributed(client: Client,
                               values: pd.Series,
                               df: pd.DataFrame,
                               col_idxes: pd.Series) -> pd.DataFrame:
    search_keys = df.iloc[:, 0]
    idx_bins = []
    bins = []
    for i in range(CORES):
        start_idx = (i * df.shape[0]) // CORES
        end_idx = ((i + 1) * df.shape[0]) // CORES
        if start_idx == 0:
            idx_bins.append(0)
            bins.append(-float('inf'))
        idx_bins.append(end_idx)
        bins.append(search_keys[end_idx - 1])
    bins[-1] = float('inf')

    """
    # It might be better to get the bins with qcut
    start_time_test = time()
    res, bins = pd.qcut(values, CORES, retbins=True, labels=False)
    print(f"qcut time: {time() - start_time_test}")
    print(res)
    """

    values = values.to_frame()
    values['col_idxes_DO_NOT_USE'] = col_idxes
    binned_values = range_partition_df_distributed(dask_client, values, bins)

    workers = list(client.scheduler_info()['workers'].keys())
    result_futures = []
    for i in range(CORES):
        worker_id = workers[i]
        values_partitions = []
        for j in range(CORES):
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

        result_futures.append(client.submit(vlookup_approx_local, scattered_values, scattered_df))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()


if __name__ == '__main__':
    CORES = 4
    DF_ROWS = 100000
    df = pd.DataFrame(np.random.randint(0, DF_ROWS, size=(DF_ROWS, 10)))
    values, df, col_idxes = df.iloc[:, 0], df.iloc[:, 1:], pd.Series([3] * DF_ROWS)
    df.iloc[:, 0] = range(DF_ROWS)

    start_time = time()
    table1 = vlookup_approx_np(values, df, col_idxes)
    print(f"Finished local hash in {time() - start_time} seconds.")

    dask_client = Client(processes=True, n_workers=CORES)
    start_time = time()
    table2 = vlookup_approx_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed hash in {time() - start_time} seconds.")

    table1 = table1.astype('object')
    assert table1.equals(table2)
    print("Dataframes are equal!")