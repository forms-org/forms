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

import sys
sys.path.append("/home/ec2-user/forms")

import pandas as pd

from time import time
from dask.distributed import Client

from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_pd_merge

from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import vlookup_approx_distributed
from forms.executor.dfexecutor.lookup.distributed.vlookup_exact import vlookup_exact_distributed

from forms.executor.dfexecutor.lookup.utils import create_alpha_df


def vlookup_approx_string_test(dask_client):
    print("TESTING VLOOKUP APPROX WITH STRINGS")
    values, df, col_idxes = alpha_df

    start_time = time()
    table1 = vlookup_approx_np_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2, dist_time, exec_time = vlookup_approx_distributed(dask_client, values, df, col_idxes, vlookup_approx_np_vector)
    execution_time = time() - start_time
    print(f"Finished distributed VLOOKUP in {execution_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")
    return execution_time, dist_time, exec_time


def vlookup_exact_string_test(dask_client):
    print("TESTING VLOOKUP EXACT WITH STRINGS")
    values, df, col_idxes = alpha_df

    start_time = time()
    table1 = vlookup_exact_pd_merge(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2, dist_time, exec_time = vlookup_exact_distributed(dask_client, values, df, col_idxes, vlookup_exact_pd_merge)
    execution_time = time() - start_time
    print(f"Finished distributed VLOOKUP in {execution_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")
    return execution_time, dist_time, exec_time


if __name__ == '__main__':
    row_counts = [100_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000]
    core_counts = [1, 2, 4, 8, 16]
    df = create_alpha_df(max(row_counts))

    approx_times = pd.DataFrame(columns=row_counts)
    exact_times = approx_times.copy()
    approx_dist_times = approx_times.copy()
    exact_dist_times = approx_times.copy()
    approx_exec_times = approx_times.copy()
    exact_exec_times = approx_times.copy()

    for rows in row_counts:
        alpha_df = df[0][:rows], df[1][:rows], df[2][:rows]
        for cores in core_counts:
            print(f"RUNNING: {rows} rows, {cores} cores")
            client = Client(processes=True, n_workers=cores)

            # total_time, dist_time, exec_time = vlookup_approx_string_test(client)
            # approx_times.loc[cores, rows] = total_time
            # approx_dist_times.loc[cores, rows] = dist_time
            # approx_exec_times.loc[cores, rows] = exec_time

            total_time, dist_time, exec_time = vlookup_exact_string_test(client)
            exact_times.loc[cores, rows] = total_time
            exact_dist_times.loc[cores, rows] = dist_time
            exact_exec_times.loc[cores, rows] = exec_time

            client.close()

    # print(approx_times)
    # print(approx_dist_times)
    # print(approx_exec_times)
    print(exact_times)
    print(exact_dist_times)
    print(exact_exec_times)

    # approx_times.to_csv('./lookup_bench_data/approx_times.csv')
    # approx_dist_times.to_csv('./lookup_bench_data/approx_dist_times.csv')
    # approx_exec_times.to_csv('./lookup_bench_data/approx_exec_times.csv')
    exact_times.to_csv('./lookup_bench_data/exact_times.csv')
    exact_dist_times.to_csv('./lookup_bench_data/exact_dist_times.csv')
    exact_exec_times.to_csv('./lookup_bench_data/exact_exec_times.csv')
