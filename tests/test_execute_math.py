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
import pytest
import numpy as np
import pandas as pd
from typing import Callable

from forms.executor.executionnode import (
    RefExecutionNode,
    FunctionExecutionNode,
)
from forms.executor.dfexecutor.mathfuncexecutor import is_odd_df_executor, sin_df_executor
from forms.executor.table import DFTable
from forms.executor.utils import ExecutionContext
from forms.utils.reference import Ref, RefType, axis_along_row
from forms.utils.functions import Function
from forms.utils.treenode import link_parent_to_children

table = None


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    m = 100
    n = 5
    df = pd.DataFrame(np.ones((m, n)))
    global table
    table = DFTable(df)

    yield


def compute_one_formula(ref: Ref, ref_type: RefType, function: Function, executor: Callable) -> DFTable:
    global table
    root = FunctionExecutionNode(function, Ref(0, 0), RefType.RR, axis_along_row)
    child = RefExecutionNode(ref, table, ref_type, axis_along_row)
    link_parent_to_children(root, [child])
    child.set_exec_context(ExecutionContext(50, 100, axis_along_row))
    return executor(root)


# try to mock forms.compute_formula(df, "=ISODD(A1)")
def test_execute_is_odd_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ISODD, is_odd_df_executor)
    real_result = pd.DataFrame(np.full(50, True))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=SIN(A1)")
def test_execute_sin_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SIN, sin_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.84))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-02)
