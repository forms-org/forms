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

from forms.executor.dfexecutor.dfexecnode import (
    DFRefExecNode,
    DFFuncExecNode,
    DFLitExecNode,
)
from forms.executor.dfexecutor.textfunctionexecutor import (
    concat_executor,
    concatenate_executor,
    exact_executor,
    find_executor,
    left_executor,
    len_executor,
    lower_executor,
    mid_executor,
    replace_executor,
    right_executor,
    trim_executor,
    upper_executor,
    value_executor,
)
from forms.executor.dfexecutor.dftable import DFTable
from forms.core.config import DFExecContext
from forms.utils.reference import Ref, RefType, AXIS_ALONG_ROW
from forms.utils.functions import Function
from forms.utils.treenode import link_parent_to_children

table = None


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    m = 100
    n = 5
    df = pd.DataFrame(np.full((m, n), "  TeSt Case  "))
    global table
    table = DFTable(df)

    yield


def compute_one_formula(ref: Ref, ref_type: RefType, function: Function, executor: Callable) -> DFTable:
    global table
    root = DFFuncExecNode(function, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child = DFRefExecNode(ref, table, ref_type, AXIS_ALONG_ROW)
    link_parent_to_children(root, [child])
    child.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    return executor(root)


def test_execute_len_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.LEN, len_executor)
    real_result = pd.DataFrame(np.full(50, fill_value=13))
    assert np.array_equal(result.df.values, real_result.values)


def test_execute_lower_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.LOWER, lower_executor)
    real_result = pd.DataFrame(np.full(50, fill_value="  test case  "))
    assert np.array_equal(result.df.values, real_result.values)


def test_execute_upper_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.UPPER, upper_executor)
    real_result = pd.DataFrame(np.full(50, fill_value="  TEST CASE  "))
    assert np.array_equal(result.df.values, real_result.values)


def test_execute_trim_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.TRIM, trim_executor)
    real_result = pd.DataFrame(np.full(50, fill_value="TeSt Case"))
    assert np.array_equal(result.df.values, real_result.values)


def test_execute_exact_rr():
    parent = DFFuncExecNode(Function.EXACT, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode("  TeSt Case  ", RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = exact_executor(parent)
    real_result = pd.DataFrame(np.full(50, True))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_concat_rr():
    parent = DFFuncExecNode(Function.EXACT, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode(" ", RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = concat_executor(parent)
    real_result = pd.DataFrame(np.full(50, "  TeSt Case   "))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_concatenate_rr():
    parent = DFFuncExecNode(Function.EXACT, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode("hello world", RefType.RR, AXIS_ALONG_ROW)
    child3 = DFLitExecNode(" RISE Lab", RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2, child3])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = concatenate_executor(parent)
    real_result = pd.DataFrame(np.full(50, "  TeSt Case  hello world RISE Lab"))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_find_rr():
    parent = DFFuncExecNode(Function.FIND, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFLitExecNode("Case", RefType.RR, AXIS_ALONG_ROW)
    child2 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child3 = DFLitExecNode(2, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2, child3])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = find_executor(parent)
    real_result = pd.DataFrame(np.full(50, 7))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_left_rr():
    parent = DFFuncExecNode(Function.FIND, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode(3, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = left_executor(parent)
    real_result = pd.DataFrame(np.full(50, "  T"))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_right_rr():
    parent = DFFuncExecNode(Function.FIND, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode(3, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = right_executor(parent)
    real_result = pd.DataFrame(np.full(50, "e  "))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_mid_rr():
    parent = DFFuncExecNode(Function.FIND, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode(2, RefType.RR, AXIS_ALONG_ROW)
    child3 = DFLitExecNode(4, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2, child3])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = mid_executor(parent)
    real_result = pd.DataFrame(np.full(50, "TeSt"))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_replace_rr():
    parent = DFFuncExecNode(Function.FIND, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child1 = DFRefExecNode(Ref(0, 0, 0, 0), table, RefType.RR, AXIS_ALONG_ROW)
    child2 = DFLitExecNode(7, RefType.RR, AXIS_ALONG_ROW)
    child3 = DFLitExecNode(6, RefType.RR, AXIS_ALONG_ROW)
    child4 = DFLitExecNode("Suites", RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(parent, [child1, child2, child3, child4])
    parent.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = replace_executor(parent)
    real_result = pd.DataFrame(np.full(50, "  TeSt Suites"))
    assert np.array_equal(sub_result.df.iloc[0:50].values, real_result.values)


def test_execute_time_value():
    m = 100
    n = 5
    df = pd.DataFrame(np.full((m, n), "1:22:33 PM"))
    table2 = DFTable(df)
    root = DFFuncExecNode(Function.VALUE, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child = DFRefExecNode(Ref(0, 0, 0, 0), table2, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(root, [child])
    child.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = value_executor(root)
    real_result = pd.DataFrame(np.full(50, fill_value=0.557326389))
    assert np.allclose(sub_result.df.values, real_result.values, rtol=1e-03)


def test_execute_numerical_value():
    m = 100
    n = 5
    df = pd.DataFrame(np.full((m, n), "67.5%"))
    table2 = DFTable(df)
    root = DFFuncExecNode(Function.VALUE, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child = DFRefExecNode(Ref(0, 0, 0, 0), table2, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(root, [child])
    child.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = value_executor(root)
    real_result = pd.DataFrame(np.full(50, fill_value=0.675))
    assert np.array_equal(sub_result.df.values, real_result.values)


def test_execute_date_value():
    m = 100
    n = 5
    df = pd.DataFrame(np.full((m, n), "September 6, 2001"))
    table2 = DFTable(df)
    root = DFFuncExecNode(Function.VALUE, Ref(0, 0), RefType.RR, AXIS_ALONG_ROW)
    child = DFRefExecNode(Ref(0, 0, 0, 0), table2, RefType.RR, AXIS_ALONG_ROW)
    link_parent_to_children(root, [child])
    child.set_exec_context(DFExecContext(50, 100, AXIS_ALONG_ROW))
    sub_result = value_executor(root)
    real_result = pd.DataFrame(np.full(50, fill_value=37140))
    assert np.array_equal(sub_result.df.values, real_result.values)
