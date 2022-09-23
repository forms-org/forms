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
from forms.executor.dfexecutor.mathfuncexecutor import (
    abs_df_executor,
    acos_df_executor,
    acosh_df_executor,
    asin_df_executor,
    asinh_df_executor,
    atan_df_executor,
    atanh_df_executor,
    cos_df_executor,
    cosh_df_executor,
    degrees_df_executor,
    exp_df_executor,
    fact_df_executor,
    int_df_executor,
    is_even_df_executor,
    is_odd_df_executor,
    ln_df_executor,
    log10_df_executor,
    radians_df_executor,
    sign_df_executor,
    sin_df_executor,
    sinh_df_executor,
    sqrt_df_executor,
    sqrt_pi_df_executor,
    tan_df_executor,
    tanh_df_executor,
)
from forms.executor.table import DFTable
from forms.executor.utils import ExecutionContext
from forms.utils.reference import Ref, RefType, axis_along_row
from forms.utils.functions import Function
from forms.utils.treenode import link_parent_to_children

table = DFTable(pd.DataFrame([]))


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


# try to mock forms.compute_formula(df, "=ABS(A1)")
def test_execute_abs_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ABS, abs_df_executor)
    real_result = pd.DataFrame(np.full(50, 1))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=ACOS(A1)")
def test_execute_acos_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ACOS, acos_df_executor)
    real_result = pd.DataFrame(np.full(50, 0))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=ACOSH(A1)")
def test_execute_acosh_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ACOSH, acosh_df_executor)
    real_result = pd.DataFrame(np.full(50, 0))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=ASIN(A1)")
def test_execute_asin_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ASIN, asin_df_executor)
    real_result = pd.DataFrame(np.full(50, 1.571))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=ASINH(A1)")
def test_execute_asinh_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ASINH, asinh_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.881))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=ATAN(A1)")
def test_execute_atan_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ATAN, atan_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.785))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=ATANH(A1)")
def test_execute_atanh_simple_formula_rr():
    try:
        compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ATANH, atanh_df_executor)
        assert False
    except ValueError:
        pass
    except Exception:
        assert False


# try to mock forms.compute_formula(df, "=COS(A1)")
def test_execute_cos_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.COS, cos_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.540))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=COSH(A1)")
def test_execute_cosh_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.COSH, cosh_df_executor)
    real_result = pd.DataFrame(np.full(50, 1.543))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=DEGREES(A1)")
def test_execute_degrees_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.DEGREES, degrees_df_executor)
    real_result = pd.DataFrame(np.full(50, 57.296))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=EXP(A1)")
def test_execute_exp_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.EXP, exp_df_executor)
    real_result = pd.DataFrame(np.full(50, 2.718))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=FACT(A1)")
def test_execute_fact_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.FACT, fact_df_executor)
    real_result = pd.DataFrame(np.full(50, 1))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=INT(A1)")
def test_execute_int_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.INT, int_df_executor)
    real_result = pd.DataFrame(np.full(50, 1))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=ISEVEN(A1)")
def test_execute_is_even_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ISEVEN, is_even_df_executor)
    real_result = pd.DataFrame(np.full(50, False))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=ISODD(A1)")
def test_execute_is_odd_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.ISODD, is_odd_df_executor)
    real_result = pd.DataFrame(np.full(50, True))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=LN(A1)")
def test_execute_ln_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.LN, ln_df_executor)
    real_result = pd.DataFrame(np.full(50, 0))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=LOG10(A1)")
def test_execute_log10_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.LOG10, log10_df_executor)
    real_result = pd.DataFrame(np.full(50, 0))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=RADIANS(A1)")
def test_execute_radians_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.RADIANS, radians_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.0174533))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=SIGN(A1)")
def test_execute_sign_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SIGN, sign_df_executor)
    real_result = pd.DataFrame(np.full(50, 1))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=SIN(A1)")
def test_execute_sin_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SIN, sin_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.841))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=SINH(A1)")
def test_execute_sinh_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SINH, sinh_df_executor)
    real_result = pd.DataFrame(np.full(50, 1.175))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=SQRT(A1)")
def test_execute_sqrt_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SQRT, sqrt_df_executor)
    real_result = pd.DataFrame(np.full(50, 1))
    assert np.array_equal(result.df.values, real_result.values)


# try to mock forms.compute_formula(df, "=SQRTPI(A1)")
def test_execute_sqrt_pi_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.SQRTPI, sqrt_pi_df_executor)
    real_result = pd.DataFrame(np.full(50, 1.772))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=TAN(A1)")
def test_execute_tan_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.TAN, tan_df_executor)
    real_result = pd.DataFrame(np.full(50, 1.557))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)


# try to mock forms.compute_formula(df, "=TANH(A1)")
def test_execute_tanh_simple_formula_rr():
    result = compute_one_formula(Ref(0, 0, 0, 0), RefType.RR, Function.TANH, tanh_df_executor)
    real_result = pd.DataFrame(np.full(50, 0.762))
    assert np.allclose(result.df.values, real_result.values, rtol=1e-03)
