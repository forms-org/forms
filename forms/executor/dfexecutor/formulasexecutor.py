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
import numpy as np

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode
from forms.utils.optimizations import FRRFOptimization
from forms.utils.reference import axis_along_row, RefType
from forms.utils.exceptions import NonScalarNotSupportedException


def formulas_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    results = []
    exec_context = physical_subtree.exec_context
    func = exec_context.compiled_formula_func
    start_formula_idx = exec_context.start_formula_idx
    end_formula_idx = exec_context.end_formula_idx
    assert physical_subtree.fr_rf_optimization == FRRFOptimization.NOOPT
    for idx in range(start_formula_idx, end_formula_idx):
        values = []
        for child in physical_subtree.children:
            if isinstance(child, RefExecutionNode):
                ref = child.ref
                df = child.table.get_table_content()
                out_ref_type = child.out_ref_type
                idx = (
                    idx - start_formula_idx if child.exec_context.start_formula_idx == 0 else idx
                )  # check intermediate node
                window = None
                axis = child.exec_context.axis
                # TODO: add support for axis_along_column
                if axis == axis_along_row:
                    df = df.iloc[:, ref.col : ref.last_col + 1]
                    if out_ref_type == RefType.RR and idx + ref.last_row + 1 <= len(df):
                        window = df.iloc[idx + ref.row : idx + ref.last_row + 1].to_numpy()
                    elif out_ref_type == RefType.FF:
                        window = df.iloc[ref.row : ref.last_row + 1].to_numpy()
                    elif out_ref_type == RefType.FR and idx + ref.last_row + 1 <= len(df):
                        window = df.iloc[ref.row : idx + ref.last_row + 1].to_numpy()
                    elif out_ref_type == RefType.RF and ref.row + idx < ref.last_row + 1:
                        window = df.iloc[ref.row + idx : ref.last_row + 1].to_numpy()
                values.append(window)
        res = np.nan if any(value is None for value in values) else func(*values)
        if isinstance(res, np.ndarray):
            if res.size != 1:
                raise NonScalarNotSupportedException("A formula should only compute a scalar")
            results.append(res.item())
        else:
            results.append(res)
    return DFTable(df=pd.DataFrame(results))
