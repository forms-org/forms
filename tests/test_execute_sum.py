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

from forms.executor.executionnode import (
    RefExecutionNode,
    FunctionExecutionNode,
    create_intermediate_ref_node,
)
from forms.executor.dfexecutor.basicfuncexecutor import sum_df_executor
from forms.executor.table import DFTable
from forms.executor.utils import ExecutionContext
from forms.utils.reference import Ref, RefType, axis_along_row, origin_ref
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


def compute_one_formula(ref: Ref, ref_type: RefType) -> DFTable:
    global table
    root = FunctionExecutionNode(Function.SUM, Ref(0, 0), RefType.RR, axis_along_row)
    child = RefExecutionNode(ref, table, ref_type, axis_along_row)
    link_parent_to_children(root, [child])
    child.set_exec_context(ExecutionContext(50, 100, axis_along_row))
    return sum_df_executor(root)


# try to mock forms.compute_formula(df, "=SUM(A1:B3)")
def test_execute_sum_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 2, 1), RefType.RR)
    real_result = pd.DataFrame(np.full(48, 6.0))
    assert np.array_equal(result.df.iloc[0:48].values, real_result.values)


# try to mock forms.compute_formula(df, "=SUM(A$1:B$3)")
def test_execute_sum_simple_formula_ff():
    result = compute_one_formula(Ref(0, 0, 2, 1), RefType.FF)
    # according to current semantics, ff will only return one single value for all column
    real_result = pd.DataFrame([6.0])
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=SUM(A$1:B3)")
def test_execute_sum_simple_formula_fr():
    result = compute_one_formula(Ref(0, 0, 2, 1), RefType.FR)
    real_result = pd.DataFrame(np.arange(106, 202, 2))
    assert np.array_equal(result.df.iloc[0:48].values, real_result.values)


# try to mock forms.compute_formula(df, "=SUM(A1:B$100)")
def test_execute_sum_simple_formula_rf():
    result = compute_one_formula(Ref(0, 0, 99, 1), RefType.RF)
    real_result = pd.DataFrame(np.arange(100, 0, -2))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=SUM($A$1, SUM(A1:B3))")
def test_execute_sum_complex_formula():
    global table
    root = FunctionExecutionNode(Function.SUM, origin_ref, RefType.RR, axis_along_row)
    parent = FunctionExecutionNode(Function.SUM, origin_ref, RefType.RR, axis_along_row)
    child1 = RefExecutionNode(Ref(0, 0), table, RefType.FF, axis_along_row)
    child2 = RefExecutionNode(Ref(0, 0, 2, 1), table, RefType.RR, axis_along_row)
    link_parent_to_children(root, [parent, child1])
    link_parent_to_children(parent, [child2])
    root.set_exec_context(ExecutionContext(50, 100, axis_along_row))
    sub_result = sum_df_executor(parent)
    real_result = pd.DataFrame(np.full(48, 6.0))
    assert np.array_equal(sub_result.df.iloc[0:48].values, real_result.values)

    ref_node = create_intermediate_ref_node(sub_result, parent)
    link_parent_to_children(root, [ref_node, child1])
    result = sum_df_executor(root)

    real_result = pd.DataFrame(np.full(48, 7.0))
    assert np.array_equal(result.df.iloc[0:48].values, real_result.values)
