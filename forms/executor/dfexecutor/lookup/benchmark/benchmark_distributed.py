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

from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import vlookup_approx_distributed
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_hash_vector
from forms.executor.dfexecutor.lookup.distributed.vlookup_exact import vlookup_exact_hash_distributed
from forms.executor.dfexecutor.lookup.algorithm.lookup_approx import lookup_np_vector
from forms.executor.dfexecutor.lookup.distributed.lookup_approx import lookup_approx_distributed
from forms.executor.dfexecutor.lookup.utils import create_alpha_df


CORES = 4
DF_ROWS = 1000000
np.random.seed(1)
dask_client = None
num_df = None
alpha_df = None


def vlookup_approx_num_test():
    print("TESTING VLOOKUP APPROX WITH NUMBERS")
    test_df = num_df
    values, df, col_idxes = test_df.iloc[:, 0], test_df.iloc[:, 1:], pd.Series([3] * DF_ROWS)
    df = pd.concat([pd.Series(range(DF_ROWS)), df], axis=1)

    start_time = time()
    table1 = vlookup_approx_np_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = vlookup_approx_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


def vlookup_approx_string_test():
    print("TESTING VLOOKUP APPROX WITH STRINGS")
    values, df, col_idxes = alpha_df

    start_time = time()
    table1 = vlookup_approx_np_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = vlookup_approx_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


def vlookup_exact_num_test():
    print("TESTING VLOOKUP EXACT WITH NUMBERS")
    test_df = num_df
    values, df, col_idxes = test_df.iloc[:, 0], test_df.iloc[:, 1:], pd.Series([3] * DF_ROWS)

    start_time = time()
    table1 = vlookup_exact_hash_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = vlookup_exact_hash_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


def vlookup_exact_string_test():
    print("TESTING VLOOKUP EXACT WITH STRINGS")
    values, df, col_idxes = alpha_df

    start_time = time()
    table1 = vlookup_exact_hash_vector(values, df, col_idxes)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = vlookup_exact_hash_distributed(dask_client, values, df, col_idxes)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


def lookup_approx_num_test():
    print("TESTING LOOKUP WITH NUMBERS")
    test_df = num_df
    values, search_range, result_range = test_df.iloc[:, 0], pd.Series(range(DF_ROWS)), test_df.iloc[:, 2]

    start_time = time()
    table1 = lookup_np_vector(values, search_range, result_range)
    print(f"Finished local LOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = lookup_approx_distributed(dask_client, values, search_range, result_range)
    print(f"Finished distributed LOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


def lookup_approx_string_test():
    print("TESTING LOOKUP WITH STRINGS")
    values, df, col_idxes = alpha_df
    search_range, result_range = df.iloc[:, 0], df.iloc[:, 1]

    start_time = time()
    table1 = lookup_np_vector(values, search_range, result_range)
    print(f"Finished local VLOOKUP in {time() - start_time} seconds.")

    start_time = time()
    table2 = lookup_approx_distributed(dask_client, values, search_range, result_range)
    print(f"Finished distributed VLOOKUP in {time() - start_time} seconds.")

    assert table1.astype('object').equals(table2.astype('object'))
    print("Dataframes are equal!\n")


if __name__ == '__main__':
    num_df = pd.DataFrame(np.random.randint(0, DF_ROWS, size=(DF_ROWS, 10)))
    alpha_df = create_alpha_df(DF_ROWS, print_df=True)
    dask_client = Client(processes=True, n_workers=CORES)
    vlookup_approx_num_test()
    vlookup_approx_string_test()
    vlookup_exact_num_test()
    vlookup_exact_string_test()
    lookup_approx_num_test()
    lookup_approx_string_test()
