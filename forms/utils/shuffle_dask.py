import numpy as np
import pandas as pd
from time import time
from dask.distributed import Client


def hash_partition_df(df: pd.DataFrame):
    hashed_df = df.iloc[:, 0].apply(lambda x: hash(x) % CORES)
    df['hash_DO_NOT_USE'] = hashed_df
    return df.groupby('hash_DO_NOT_USE')


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


def hash_single_table_distributed(client, df):
    chunk_partitions = []
    for i in range(CORES):
        start_idx = (i * df.shape[0]) // CORES
        end_idx = ((i + 1) * df.shape[0]) // CORES
        data = df[start_idx: end_idx]
        scattered_data = client.scatter(data)
        chunk_partitions.append(client.submit(hash_partition_df, scattered_data))

    for i in range(CORES):
        chunk_partitions[i] = chunk_partitions[i].result()

    hash_partitions = []
    for i in range(CORES):
        subpartitions = []
        for j in range(CORES):
            if i in chunk_partitions[j].groups:
                group = chunk_partitions[j].get_group(i).iloc[:, 0]
                subpartitions.append(group)
        if len(subpartitions) > 0:
            partition = pd.concat(subpartitions)
            scattered_partition = client.scatter(partition)
        else:
            scattered_partition = pd.Series(dtype=object)
        hash_partitions.append(client.submit(hash_first_index, scattered_partition))

    tables = []
    for i in range(CORES):
        tables.append(hash_partitions[i].result())

    result = {}
    for table in tables:
        result.update(table)

    return result


def hash_k_tables_distributed(client: Client, df_list: list[pd.DataFrame]) -> list[dict[any, int]]:
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
        for i in range(CORES):
            chunk_partitions[df_idx][i] = chunk_partitions[df_idx][i].result()

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

    tables = []
    for df_idx in range(len(df_list)):
        result = {}
        for i in range(CORES):
            result.update(hash_partitions[df_idx][i].result())
        tables.append(result)

    return tables


def check_table_equality(t1: dict, t2: dict, is_iterable=True):
    if is_iterable:
        for k, v1 in t1.items():
            assert set(v1) == set(t2[k])
    else:
        for k, v1 in t1.items():
            assert v1 == t2[k]


if __name__ == '__main__':
    CORES = 8
    DF_ROWS = 1000000
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

    check_table_equality(table1_1, table1_2, is_iterable=False)
    check_table_equality(table2_1, table2_2, is_iterable=False)
    print("Tables are equal!")
