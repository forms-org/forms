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
    chunk_partitions = {}
    for df_idx in range(len(df_list)):
        chunk_partitions[df_idx] = []
        df = df_list[df_idx]
        for i in range(CORES):
            start_idx = (i * df.shape[0]) // CORES
            end_idx = ((i + 1) * df.shape[0]) // CORES
            data = df[start_idx: end_idx]
            scattered_data = client.scatter(data)
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
    result_futures = []
    for i in range(CORES):
        values_partitions, df_partitions = [], []
        for j in range(CORES):
            if i in chunk_partitions[0][j].groups:
                group = chunk_partitions[0][j].get_group(i)
                values_partitions.append(group)
            if i in chunk_partitions[1][j].groups:
                group = chunk_partitions[1][j].get_group(i)
                df_partitions.append(group)

        if len(values_partitions) > 0:
            scattered_values = client.scatter(pd.concat(values_partitions))
        else:
            scattered_values = pd.Series(dtype=object)

        if len(df_partitions) > 0:
            # This is an optimization to avoid sending unnecessary data to nodes to be hashed
            # Instead, use the index to reconstruct the data locally to send to each node
            # I don't think this really does anything, it's easier to just hash df above and set
            # partition to pd.concat(df_partitions)
            partition = df[df.index.isin(pd.concat(df_partitions).index)]
            scattered_df = client.scatter(partition)
        else:
            scattered_df = pd.Series(dtype=object)

        result_futures.append(client.submit(vlookup_exact_hash_local, scattered_values, scattered_df))

    # Combining the results uses another optimization (currently commented out)
    # Since we know that the indexes of the subresults exactly match to the final result
    # we can pre-construct the final dataframe and fill in the values at the indices
    # This improves the complexity from O(nlogn) time to O(n) time since we don't need sorting
    # I'm not sure if this actually speeds anything up, but it helps the complexity
    # results = pd.Series(index=values.index, dtype=object)
    # result_arr = client.gather(result_futures)
    # for i in range(CORES):
    #     results[result_arr[i].index] = result_arr[i].iloc[:, 0]
    # return results.to_frame()

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


""" OLD CODE FOR REFERENCE -------------------------------------------- """

"""
def hash_single_table(df: pd.Series):
    table = {}
    for i in df.index:
        val = df[i]
        if val not in table:
            table[val] = []
        table[val].append(i)
    return table


def hash_first_index(df: pd.Series):
    table = {}
    for i in df.index:
        val = df[i]
        if val not in table:
            table[val] = i
    return table


def partition_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]):
    chunk_partitions = hash_chunk_k_tables_distributed(client, df_list)
    hash_partitions = {}
    for df_idx in range(len(df_list)):
        hash_partitions[df_idx] = []
        for i in range(CORES):
            subpartitions = []
            for j in range(CORES):
                if i in chunk_partitions[df_idx][j].groups:
                    group = chunk_partitions[df_idx][j].get_group(i).iloc[:, 0]
                    subpartitions.append(group)
            if len(subpartitions) > 0:
                partition = pd.concat(subpartitions)
                scattered_partition = client.scatter(partition)
            else:
                scattered_partition = pd.Series(dtype=object)
            hash_partitions[df_idx].append(client.submit(hash_first_index, scattered_partition))
    return hash_partitions


def hash_single_table_distributed(client, df):
    return hash_k_tables_distributed(client, [df])[0]


def hash_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]) -> list[dict[any, int]]:
    hash_partitions = partition_k_tables_distributed(client, df_list)
    tables = []
    for df_idx in range(len(df_list)):
        result = {}
        for i in range(CORES):
            result.update(hash_partitions[df_idx][i].result())
        tables.append(result)
    return tables


def check_dict_equality(t1: dict, t2: dict, is_iterable=True):
    if is_iterable:
        for k, v1 in t1.items():
            assert set(v1) == set(t2[k])
    else:
        for k, v1 in t1.items():
            assert v1 == t2[k]
"""

"""          
if __name__ == '__main__':
    CORES = 8
    DF_ROWS = 100000
    df1 = pd.DataFrame(np.random.randint(0, 1000, size=(DF_ROWS, 1)))
    df2 = pd.DataFrame(np.random.randint(0, 1000, size=(DF_ROWS, 1)))

    start_time = time()
    table1_1 = hash_first_index(df1.iloc[:, 0])
    table2_1 = hash_first_index(df2.iloc[:, 0])
    print(f"Finished local hash in {time() - start_time} seconds.")

    dask_client = Client(processes=True, n_workers=CORES)
    start_time = time()
    table1_2, table2_2 = hash_k_tables_distributed(dask_client, [df1, df2])
    print(f"Finished distributed hash in {time() - start_time} seconds.")

    check_dict_equality(table1_1, table1_2, is_iterable=False)
    check_dict_equality(table2_1, table2_2, is_iterable=False)
    print("Tables are equal!")
"""

