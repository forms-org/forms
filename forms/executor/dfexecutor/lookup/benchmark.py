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
import time
import random

from forms.executor.dfexecutor.lookup.lookupfuncexecutor import lookup_binary_search, lookup_sort_merge


def create_df(size=(1000, 10), df_type="constant", start_val: float = 0, first_col_idx=True, seed=2):
    rows, cols = size
    assert rows >= 1 and cols >= 1
    lst = [([0.0] * cols) for _ in range(rows)]
    random.seed(seed)
    for i in range(rows):
        col_start = 0
        if first_col_idx:
            col_start = 1
            lst[i][0] = i
        for j in range(col_start, cols):
            if df_type == "constant":
                val = start_val
            elif df_type == "range":
                val = start_val + i
            elif df_type == "random":
                val = random.random() * rows
            else:
                raise IOError(f"Df type {df_type} is not supported!")
            lst[i][j] = val
    return pd.DataFrame(lst)


def run_lookup_trials(df: pd.DataFrame):
    values, search_range, result_range = df.iloc[:, 1], df.iloc[:, 0], df.iloc[:, 2]

    # Warm up cache
    for i in range(5):
        lookup_binary_search(values, search_range, result_range)
        lookup_sort_merge(values, search_range, result_range)

    # Run trials
    binary_search_time, sort_merge_time = 0, 0
    for i in range(5):
        start_time = time.time()
        result1 = lookup_binary_search(values, search_range, result_range)
        binary_search_time += time.time() - start_time
        start_time = time.time()
        result2 = lookup_sort_merge(values, search_range, result_range)
        sort_merge_time += time.time() - start_time
        assert result1.equals(result2)
    for i in range(5):
        start_time = time.time()
        result1 = lookup_sort_merge(values, search_range, result_range)
        sort_merge_time += time.time() - start_time
        start_time = time.time()
        result2 = lookup_binary_search(values, search_range, result_range)
        binary_search_time += time.time() - start_time
        assert result1.equals(result2)

    return binary_search_time, sort_merge_time


def benchmark_lookup():
    print("\nBENCHMARKING LOOKUP", "-" * 20, "\n")

    df = create_df(df_type="constant", start_val=50)
    binary_search_time, sort_merge_time = run_lookup_trials(df)
    print("Constant DataFrame")
    print(f"Binary search time: {binary_search_time}")
    print(f"Sort merge time: {sort_merge_time}")
    print()

    df = create_df(df_type="range", start_val=0)
    binary_search_time, sort_merge_time = run_lookup_trials(df)
    print("Range DataFrame Exact")
    print(f"Binary search time: {binary_search_time}")
    print(f"Sort merge time: {sort_merge_time}")
    print()

    df = create_df(df_type="range", start_val=0.5)
    binary_search_time, sort_merge_time = run_lookup_trials(df)
    print("Range DataFrame Approximate")
    print(f"Binary search time: {binary_search_time}")
    print(f"Sort merge time: {sort_merge_time}")
    print()

    df = create_df(df_type="random")
    binary_search_time, sort_merge_time = run_lookup_trials(df)
    print("Random DataFrame Approximate")
    print(f"Binary search time: {binary_search_time}")
    print(f"Sort merge time: {sort_merge_time}")


benchmark_lookup()
