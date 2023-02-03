import numpy as np
import pandas as pd
from time import time
from dask.distributed import Client
from forms.executor.dfexecutor.lookup.vlookupfuncexecutor import vlookup_exact_hash


# Locally hashes a dataframe with 1 column and groups it by hash.
def hash_partition_df(df: pd.DataFrame):
    hashed_df = df.iloc[:, 0].apply(lambda x: hash(x) % CORES)
    df['hash_DO_NOT_USE'] = hashed_df
    return df.groupby('hash_DO_NOT_USE')


# Chunks and hashes dataframes in df_list with a Dask client.
def hash_chunk_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]):
    workers = list(client.scheduler_info()['workers'].keys())
    chunk_partitions = {}
    for df_idx in range(len(df_list)):
        chunk_partitions[df_idx] = []
        df = df_list[df_idx]
        for i in range(CORES):
            worker_id = workers[i]
            start_idx = (i * df.shape[0]) // CORES
            end_idx = ((i + 1) * df.shape[0]) // CORES
            data = df[start_idx: end_idx]
            scattered_data = client.scatter(data, workers=worker_id)
            chunk_partitions[df_idx].append(client.submit(hash_partition_df, scattered_data))
    for df_idx in range(len(df_list)):
        chunk_partitions[df_idx] = client.gather(chunk_partitions[df_idx])
    return chunk_partitions


# Locally hash and probe.
def vlookup_exact_hash_local(values, df):
    if len(values) == 0:
        return pd.DataFrame(dtype=object)
    values, col_idxes = values.iloc[:, 0], values.loc[:, 'col_idxes_DO_NOT_USE']
    cache = {}
    for i in range(df.shape[0]):
        value = df.iloc[i, 0]
        if value not in cache:
            cache[value] = i
    result_df = pd.Series(index=values.index, dtype=object)
    for i in range(len(values)):
        value, col_idx = values.iloc[i], col_idxes.iloc[i]
        result = np.nan
        if value in cache:
            value_idx = cache[value]
            result = df.iloc[value_idx, col_idx - 1]
        result_df.iloc[i] = result
    return result_df.to_frame()


# Performs a distributed VLOOKUP on the given values with a Dask client.
def vlookup_exact_hash_distributed(client: Client,
                                   values: pd.Series,
                                   df: pd.DataFrame,
                                   col_idxes: pd.Series) -> pd.DataFrame:
    values = values.to_frame()
    values['col_idxes_DO_NOT_USE'] = col_idxes
    chunk_partitions = hash_chunk_k_tables_distributed(client, [values, df.iloc[:, 0].to_frame()])
    workers = list(client.scheduler_info()['workers'].keys())
    result_futures = []
    for i in range(CORES):
        worker_id = workers[i]
        values_partitions, df_partitions = [], []
        for j in range(CORES):
            if i in chunk_partitions[0][j].groups:
                group = chunk_partitions[0][j].get_group(i)
                values_partitions.append(group)
            if i in chunk_partitions[1][j].groups:
                group = chunk_partitions[1][j].get_group(i)
                df_partitions.append(group)

        if len(values_partitions) > 0:
            scattered_values = client.scatter(pd.concat(values_partitions), workers=worker_id)
        else:
            scattered_values = pd.DataFrame(dtype=object)

        if len(df_partitions) > 0:
            # This is an optimization to avoid sending unnecessary data to nodes to be hashed
            # Instead, use the index to reconstruct the data locally to send to each node
            # I don't think this really does anything, it's easier to just hash df above and set
            # partition to pd.concat(df_partitions)
            partition = df[df.index.isin(pd.concat(df_partitions).index)]
            scattered_df = client.scatter(partition, workers=worker_id)
        else:
            scattered_df = pd.DataFrame(dtype=object)

        result_futures.append(client.submit(vlookup_exact_hash_local, scattered_values, scattered_df))

    results = client.gather(result_futures)
    return pd.concat(results).sort_index()


if __name__ == '__main__':
    CORES = 4
    DF_ROWS = 100000
    np.random.seed(1)
    df = pd.DataFrame(np.random.randint(0, 1000, size=(DF_ROWS, 10)))
    values, df, col_idxes = df.iloc[:, 0], df.iloc[:, 1:], pd.Series([3] * DF_ROWS)

    start_time = time()
    table1 = vlookup_exact_hash(values, df, col_idxes)
    print(f"Finished local hash in {time() - start_time} seconds.")

    dask_client = Client(processes=True, n_workers=CORES)
    start_time = time()
    table2 = vlookup_exact_hash_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed hash in {time() - start_time} seconds.")

    table1 = table1.astype('object')
    assert table1.equals(table2)
    print("Dataframes are equal!")
