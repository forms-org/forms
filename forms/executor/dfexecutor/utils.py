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

from forms.core.config import forms_config
from forms.executor.dfexecutor.remotedf import RemoteDF
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode, LitExecutionNode
from forms.executor.table import DFTable
from forms.utils.optimizations import FRRFOptimization
from forms.utils.reference import RefType, axis_along_row


class Range:
    def __init__(
        self,
        row: int,
        col: int,
        last_row: int,
        last_col: int,
        ref_node: RefExecutionNode = None,
        remote_df: RemoteDF = None,
    ):
        self.row = row
        self.col = col
        self.last_row = last_row
        self.last_col = last_col
        self.ref_node = None
        self.row_lengths = None
        self.col_widths = None
        self.cum_row_lengths = None
        self.cum_col_widths = None
        self.row_part_idx = None
        self.row_offset = None
        self.last_row_part_idx = None
        self.col_part_idx = None
        self.col_offset = None
        self.last_col_part_idx = None
        assert ref_node is not None or remote_df is not None
        if ref_node:
            self.ref_node = ref_node
            self.remote_df = ref_node.table.remote_df
        else:
            self.remote_df = remote_df
        self.generate_idx_and_offset()
        self.block_num = self.get_block_num()

    def partition(self, i: int, col=True):
        widths = self.col_widths if col else self.row_lengths
        cum_widths = np.array(widths).cumsum()
        if i >= cum_widths[-1]:
            idx = len(cum_widths) - 1
        else:
            idx = next(x for x, val in enumerate(cum_widths) if val > i)
        offset = i - cum_widths[idx - 1] if idx != 0 else i
        return idx, offset

    def generate_idx_and_offset(self):
        self.row_lengths = [partition.rows for partition in self.remote_df.remote_partitions[:, 0]]
        self.col_widths = [partition.cols for partition in self.remote_df.remote_partitions[0]]
        self.cum_row_lengths = np.array(self.row_lengths).cumsum()
        self.cum_col_widths = np.array(self.col_widths).cumsum()
        self.row_part_idx, self.row_offset = self.partition(self.row, col=False)
        self.last_row_part_idx, _ = self.partition(self.last_row, col=False)
        self.col_part_idx, self.col_offset = self.partition(self.col, col=True)
        self.last_col_part_idx, _ = self.partition(self.last_col, col=True)
        if self.ref_node:
            self.ref_node.set_offset(self.row_offset, self.col_offset)

    def get_block_num(self):
        return (self.last_row_part_idx - self.row_part_idx + 1) * (
            self.last_col_part_idx - self.col_part_idx + 1
        )

    def reset(self, row_part_idx, col_part_idx):
        if row_part_idx == 0:
            self.row_offset = self.row
        else:
            if self.row > self.cum_row_lengths[row_part_idx - 1]:
                self.row_offset = self.row - self.cum_row_lengths[row_part_idx - 1]
        if col_part_idx == 0:
            self.col_offset = self.col
        else:
            if self.col > self.cum_col_widths[row_part_idx - 1]:
                self.col_offset = self.col - self.cum_col_widths[col_part_idx - 1]
        self.ref_node.set_offset(self.row_offset, self.col_offset)

    def __add__(self, other):
        row = min(self.row, other.row)
        end_row = max(self.last_row, other.last_row)
        col = min(self.col, other.col)
        end_col = max(self.last_col, other.last_col)
        # we can only cluster two node if they have the same df
        if self.remote_df == other.remote_df:
            return Range(row, col, end_row, end_col, remote_df=self.remote_df)
        return None


def get_refs(exec_subtree):
    refs = []
    if isinstance(exec_subtree, RefExecutionNode):
        refs.append(exec_subtree)
    elif exec_subtree.children:
        for child in exec_subtree.children:
            refs.extend(get_refs(child))
    return refs


def get_range(ref_node: RefExecutionNode, start_idx: int, end_idx: int):
    out_ref_type = ref_node.out_ref_type
    ref = ref_node.ref
    num_of_rows = ref_node.table.get_num_of_rows()
    if out_ref_type == RefType.RR:
        start = ref.row + start_idx
        end = ref.last_row + end_idx
    elif out_ref_type == RefType.FR:
        start = ref.row
        end = ref.last_row + end_idx
    elif out_ref_type == RefType.RF:
        start = ref.row + start_idx
        end = ref.last_row + 1
    else:  # out_ref_type == RefType.FF
        start = ref.row
        end = ref.last_row + 1
    return Range(max(0, start), ref.col, min(end, num_of_rows), ref.last_col, ref_node=ref_node)


class RangeCluster:
    def __init__(self, range: Range, cluster: set):
        self.range = range
        self.cluster = cluster

    def __add__(self, other):
        range = self.range + other.range
        cluster = self.cluster.union(other.cluster)
        for r in cluster:
            r.reset(range.row_part_idx, range.col_part_idx)
        return RangeCluster(range, cluster)


class RangeClusterGrid:
    def __init__(self):
        self.range_clusters = None
        self.cost_mat = None
        self.max_benefit = None

    def get_benefit(self, range1: Range, range2: Range):
        new_range = range1 + range2
        if new_range:
            return range1.block_num + range2.block_num - new_range.block_num
        else:
            return -1

    def gain_benefit(self, range_clusters):
        self.range_clusters = range_clusters
        range_clusters = self.range_clusters
        cost_mat = np.zeros((len(range_clusters), len(range_clusters)))
        for i in range(len(range_clusters)):
            for j in range(i + 1, len(range_clusters)):
                cost_mat[i][j] = self.get_benefit(range_clusters[i].range, range_clusters[j].range)
        self.cost_mat = cost_mat
        self.max_benefit = cost_mat.max()
        return self.max_benefit > 0

    def get_max_range_clusters(self):
        return np.argwhere(self.cost_mat == self.max_benefit)[0]


def min_cost(ranges: list):
    range_clusters = [RangeCluster(r, {r}) for r in ranges]
    grid = RangeClusterGrid()
    while grid.gain_benefit(range_clusters):
        max_clusters = grid.get_max_range_clusters()
        r1 = range_clusters[max_clusters[0]]
        r2 = range_clusters[max_clusters[1]]
        range_clusters.remove(r1)
        range_clusters.remove(r2)
        new_range_cluster = r1 + r2
        range_clusters.append(new_range_cluster)

    for rc in range_clusters:
        r = rc.range
        old_remote_df = r.remote_df
        partitions = old_remote_df.remote_partitions[
            r.row_part_idx : r.last_row_part_idx + 1, r.col_part_idx : r.last_col_part_idx + 1
        ]
        remote_df = RemoteDF(
            remote_partitions=partitions,
            num_rows=old_remote_df.num_rows,
            num_cols=old_remote_df.num_cols,
        )
        table = DFTable(remote_df=remote_df)
        for r in rc.cluster:
            r.ref_node.table = table


def remote_access_planning(exec_subtree: FunctionExecutionNode) -> FunctionExecutionNode:
    if forms_config.enable_communication_opt:
        start_idx = exec_subtree.exec_context.start_formula_idx
        end_idx = exec_subtree.exec_context.end_formula_idx
        refs = get_refs(exec_subtree)
        ranges = [get_range(ref, start_idx, end_idx) for ref in refs]
        min_cost(ranges)
    return exec_subtree


def construct_df_table(array):
    return DFTable(df=pd.DataFrame(array))


def get_value_rr(df: pd.DataFrame, window_size: int, func1, func2) -> pd.DataFrame:
    return df.agg(func1, axis=1).rolling(window_size, min_periods=window_size).agg(func2).dropna()


def get_value_ff(single_value, n_formula: int) -> pd.DataFrame:
    return pd.DataFrame(np.full(n_formula, single_value))


def get_value_fr(df: pd.DataFrame, min_window_size: int, func1, func2) -> pd.DataFrame:
    return df.agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna()


def get_value_rf(df: pd.DataFrame, min_window_size: int, func1, func2) -> pd.DataFrame:
    return df.iloc[::-1].agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna().iloc[::-1]


def fill_in_nan(value, n_formula: int) -> pd.DataFrame:
    if n_formula > len(value):
        extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
        value = pd.concat([value, extra], ignore_index=True)
    return value


def get_reference_indices(ref_node: RefExecutionNode):
    start_idx = ref_node.exec_context.start_formula_idx
    end_idx = ref_node.exec_context.end_formula_idx
    n_formula = end_idx - start_idx
    out_ref_type = ref_node.out_ref_type
    ref = ref_node.ref
    row_length = ref.last_row + 1 - ref.row
    col_width = ref.last_col + 1 - ref.col
    if ref_node.exec_context.enable_communication_opt:
        row = ref_node.row_offset
        col = ref_node.col_offset
        if out_ref_type == RefType.RR:
            return row, col, row + n_formula - 1 + row_length, col + col_width
        elif out_ref_type == RefType.FF:
            return row, col, row + row_length, col + col_width
        elif out_ref_type == RefType.FR:
            return row, col, row + row_length - 1 + end_idx, col + col_width
        elif out_ref_type == RefType.RF:
            return row, col, row + row_length - start_idx, col + col_width
    else:
        row = ref.row
        col = ref.col
        if out_ref_type == RefType.RR:
            return start_idx + row, col, end_idx + ref.last_row, col + col_width
        elif out_ref_type == RefType.FF:
            return row, col, row + row_length, col + col_width
        elif out_ref_type == RefType.FR:
            return row, col, ref.last_row + end_idx, col + col_width
        elif out_ref_type == RefType.RF:
            return row + start_idx, col, ref.last_row + 1, col + col_width


def get_reference_indices_for_single_index(ref_node: RefExecutionNode, idx: int):
    ref = ref_node.ref
    table = ref_node.table
    row_length = ref.last_row + 1 - ref.row
    col_width = ref.last_col + 1 - ref.col
    row = ref_node.row_offset if ref_node.exec_context.enable_communication_opt else ref.row
    col = ref_node.col_offset if ref_node.exec_context.enable_communication_opt else ref.col
    out_ref_type = ref_node.out_ref_type
    if out_ref_type == RefType.RR and idx + ref.last_row + 1 <= table.get_num_of_rows():
        return row + idx, col, row + idx + row_length, col + col_width
    elif out_ref_type == RefType.FF:
        return row, col, row + row_length, col + col_width
    elif out_ref_type == RefType.FR and idx + ref.last_row + 1 <= table.get_num_of_rows():
        return row, col, row + row_length + idx, col + col_width
    elif out_ref_type == RefType.RF and ref.row + idx < ref.last_row + 1:
        return row + idx, col, row + row_length, col + col_width
    return None


def get_reference_indices_2_phase_rf_fr(ref_node: RefExecutionNode):
    ref = ref_node.ref
    row_length = ref.last_row + 1 - ref.row
    col_width = ref.last_col + 1 - ref.col
    row = ref_node.row_offset if ref_node.exec_context.enable_communication_opt else ref.row
    col = ref_node.col_offset if ref_node.exec_context.enable_communication_opt else ref.col
    out_ref_type = ref_node.out_ref_type
    start_idx = ref_node.exec_context.start_formula_idx
    end_idx = ref_node.exec_context.end_formula_idx
    all_formula_idx = ref_node.exec_context.all_formula_idx
    if ref_node.exec_context.enable_communication_opt:
        if out_ref_type == RefType.FR:
            return (
                row if start_idx == 0 else start_idx + row + row_length - 1,
                col,
                end_idx + row + row_length - 1,
                col + col_width,
            )
        elif out_ref_type == RefType.RF:
            return (
                row,
                col,
                row + row_length - start_idx
                if end_idx == all_formula_idx[-1]
                else row + end_idx - start_idx,
                col + col_width,
            )
    else:
        if out_ref_type == RefType.FR:
            return (
                row if start_idx == 0 else start_idx + row + row_length - 1,
                col,
                end_idx + row + row_length - 1,
                col + col_width,
            )
        elif out_ref_type == RefType.RF:
            return (
                row + start_idx,
                col,
                row + row_length if end_idx == all_formula_idx[-1] else row + end_idx,
                col + col_width,
            )
    return None


def get_single_value(child):
    value = pd.DataFrame([])
    if isinstance(child, RefExecutionNode):
        df = child.table.get_table_content()
        out_ref_type = child.out_ref_type
        start_idx = child.exec_context.start_formula_idx
        end_idx = child.exec_context.end_formula_idx
        n_formula = end_idx - start_idx
        axis = child.exec_context.axis
        if axis == axis_along_row:
            start_row, start_column, end_row, end_column = get_reference_indices(child)
            df = df.iloc[start_row:end_row, start_column:end_column]
            if out_ref_type == RefType.RR:
                value = df
                value.index, value.columns = [range(value.index.size), range(value.columns.size)]
                value = fill_in_nan(value, n_formula)
            elif out_ref_type == RefType.FF:
                value = get_value_ff(df, n_formula)
    elif isinstance(child, LitExecutionNode):
        value = child.literal
    return value
