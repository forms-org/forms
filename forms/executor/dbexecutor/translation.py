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

from forms.core.catalog import BASE_TABLE, ROW_ID, TRANSLATE_TEMP_TABLE, TEMP_TABLE_PREFIX
from forms.core.config import DBExecContext
from forms.executor.dbexecutor.dbexecnode import DBExecNode, DBFuncExecNode, DBLitExecNode, DBRefExecNode

from psycopg2 import sql

from forms.utils.functions import (
    ARITHMETIC_FUNCTIONS,
    COMPARISON_FUNCTIONS,
    DB_AGGREGATE_FUNCTIONS,
    DB_AGGREGATE_IF_FUNCTIONS,
    Function,
)
from forms.utils.reference import RefType

WINDOW_PRECEDING = "PRECEDING"
WINDOW_FOLLOWING = "FOLLOWING"


def translate(
    subtree: DBExecNode, exec_context: DBExecContext, subtree_index: int, is_root_subtree: bool
) -> tuple[sql.SQL, str]:
    ret_sql = None
    if isinstance(subtree, DBRefExecNode):
        ret_sql = sql.SQL(
            """
            SELECT {row_id}, {col_name}
            FROM {table_name}
            WHERE {row_id} >= {formula_index_start}
            AND {row_id} < {formula_index_end}
            """
        ).format(
            row_id=sql.Identifier(ROW_ID),
            col_name=sql.Identifier(subtree.cols[0]),
            table_name=sql.Identifier(BASE_TABLE),
            formula_idx_start=sql.Literal(exec_context.formula_id_start),
            formula_idx_end=sql.Literal(exec_context.formula_id_end),
        )
    elif isinstance(subtree, DBFuncExecNode):
        base_table = find_base_table(subtree, DBFuncExecNode)
        if subtree.translatable_to_window:
            ret_sql = translate_to_one_window_query(subtree, exec_context, base_table)
        elif subtree.function == Function.LOOKUP:
            ret_sql = translate_lookup_function(subtree, exec_context, base_table)
        elif subtree.function == Function.MATCH:
            ret_sql = translate_match_function(subtree, exec_context, base_table)
        elif subtree.function == Function.INDEX:
            ret_sql = translate_index_function_to_join(subtree, exec_context, base_table)
        else:
            ret_sql = translate_using_udf(subtree, exec_context, base_table)
        ret_sql = get_required_formula_results(ret_sql, exec_context)
    else:
        assert False
    if not is_root_subtree:
        ret_sql = add_create_temp_table(ret_sql, subtree_index)
    return ret_sql


def get_required_formula_results(subquery: sql.SQL, exec_context: DBExecContext) -> sql.SQL:
    return sql.SQL(
        """SELECT * 
                   FROM ({subquery}) AS {temp_table} 
                   WHERE {row_id} >= {formula_id_start}
                   AND {row_id} < {formula_id_end}"""
    ).format(
        subquery=subquery,
        temp_table=sql.Identifier(TRANSLATE_TEMP_TABLE),
        row_id=sql.Identifier(ROW_ID),
        formula_id_start=sql.Literal(exec_context.formula_id_start),
        formula_id_end=sql.Literal(exec_context.formula_id_end),
    )


def add_create_temp_table(selquery: sql.SQL, subtree_index: int) -> sql.SQL:
    return sql.SQL(
        """CREATE TEMP {table_name} AS
                   {selquery}"""
    ).format(table_name=sql.Identifier(TEMP_TABLE_PREFIX + subtree_index), selquery=selquery)


def find_base_table(subtree: DBExecNode, exec_context: DBExecContext) -> str:
    ref_node_list = subtree.collect_ref_nodes_in_order()
    if all(ref_node.table.table_name == BASE_TABLE for ref_node in ref_node_list):
        return BASE_TABLE
    else:
        return sql.SQL("""(SELECT {first_table}{row_id}, {column_list}
                           FROM {table_list})
                       """)


def translate_to_one_window_query(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    ret_sql = sql.SQL("""SELECT {row_id}, """).format(row_id=sql.Identifier(ROW_ID))
    ret_sql = ret_sql + translate_window_clause(subtree, exec_context, base_table)
    ret_sql = ret_sql + sql.SQL(""" FROM {table_name} t2""").format(
        table_name=create_quantified_table_for_window_query(subtree, base_table)
    )
    return ret_sql


def create_quantified_table_for_window_query(subtree: DBFuncExecNode, base_table: str) -> sql.SQL:
    if (
        subtree.function in DB_AGGREGATE_FUNCTIONS
        or subtree.function in DB_AGGREGATE_IF_FUNCTIONS
        or subtree.function == Function.INDEX
    ):
        childRef = subtree.children[0]
        if isinstance(childRef, DBRefExecNode):
            if childRef.out_ref_type == RefType.RF:
                return sql.SQL(
                    """(SELECT *
                                   FROM {table_name} t2
                                   WHERE t2.{row_id} <= {last_row}
                                  )
                               """
                ).format(
                    table_name=sql.Identifier(base_table),
                    row_id=sql.Identifier(ROW_ID),
                    last_row=sql.Identifier(childRef.ref.last_row),
                )
            elif childRef.out_ref_type == RefType.FR:
                return sql.SQL(
                    """(SELECT *
                                   FROM {table_name} t2
                                   WHERE t2.{row_id} >= {first_row}
                                  )
                               """
                ).format(
                    table_name=sql.Identifier(base_table),
                    row_id=sql.Identifier(ROW_ID),
                    first_row=sql.Identifier(childRef.ref.row),
                )

    return sql.SQL("""{table_name}""").format(table_name=sql.Identifier(base_table))


def translate_window_clause(subtree: DBExecNode, exec_context: DBExecContext, base_table: sql):
    if isinstance(subtree, DBFuncExecNode):
        if subtree.function == Function.IF:
            return translate_if_function(subtree, exec_context, base_table)
        elif subtree.function in COMPARISON_FUNCTIONS or subtree.function in ARITHMETIC_FUNCTIONS:
            return translate_comparison_and_arithmetic_functions(subtree, exec_context, base_table)
        elif subtree.function in DB_AGGREGATE_FUNCTIONS:
            return translate_aggregate_functions(subtree, exec_context, base_table)
        elif subtree.function in DB_AGGREGATE_IF_FUNCTIONS:
            return translate_aggregate_if_functions(subtree, exec_context, base_table)
        elif subtree.function == Function.INDEX:
            return translate_index_function_to_window(subtree, exec_context)
        else:
            assert False
    elif isinstance(subtree, DBRefExecNode):
        return translate_reference(subtree, exec_context, base_table)
    elif isinstance(subtree, DBLitExecNode):
        return translate_literal(subtree, exec_context)
    else:
        assert False


def translate_if_function(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    children = subtree.children
    return sql.SQL(
        """CASE WHEN {condition}
                      THEN {true_result}
                      ELSE {false_result}"""
    ).format(
        condition=translate_window_clause(children[0], exec_context, base_table),
        true_result=translate_window_clause(children[1], exec_context, base_table),
        false_result=translate_window_clause(children[2], exec_context, base_table),
    )


def translate_comparison_and_arithmetic_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    children = subtree.children
    return sql.SQL("""{left_clause} {function} {right_clause}""").format(
        left_clause=translate_window_clause(children[0], exec_context, base_table),
        function=subtree.function.value,
        right_clause=translate_window_clause(children[1], exec_context, base_table),
    )


def translate_aggregate_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    child = subtree.children[0]
    if isinstance(child, DBRefExecNode):
        agg_sql = None
        if subtree.function == Function.SUM:
            agg_sql = sql.SQL("""{function}({agg_column})""").format(
                function=subtree.function.value,
                agg_column=sql.SQL("+").join(sql.Identifier(col) for col in child.cols),
            )
        elif subtree.function == Function.COUNT:
            agg_sql = sql.SQL("""{num_columns} * {function}({agg_column})""").format(
                num_columns=len(child.cols),
                function=subtree.function.value,
                agg_column=sql.Identifier(child.cols[0]),
            )
        else:
            assert False
        if child.out_ref_type != RefType.FF:
            window_size_sql = compute_window_size_expression(child, exec_context)
            return agg_sql + window_size_sql
        else:
            return sql.SQL(
                """(SELECT {agg_sql}
                               FROM {table_name})
                           """
            ).format(agg_sql=agg_sql, table_name=base_table)
    else:
        assert False


def translate_aggregate_if_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    input_child = subtree.children[0]
    if isinstance(input_child, DBRefExecNode):
        output_child = input_child
        if len(subtree.children) == 3:
            output_child = subtree.children[2]
        condition = subtree.children[1].lieteral
        input_cols = input_child.cols
        output_cols = output_child.cols
        agg_sql = None
        if subtree.function == Function.SUMIF:
            agg_sql = sql.SQL("""SUM({agg_expression})""").format(
                agg_expression=sql.SQL("+").join(
                    sql.SQL(
                        """CASE WHEN {input_col}{condition}
                                THEN {ouput_col}
                                ELSE 0 END
                        """
                    ).format(input_col=input_cols[i], condition=condition, output_col=output_cols[i])
                    for i in range(len(input_child.cols))
                )
            )
        elif subtree.function == Function.COUNTIF:
            agg_sql = sql.SQL("""SUM({agg_expression})""").format(
                agg_expression=sql.SQL("+").join(
                    sql.SQL(
                        """CASE WHEN {input_col}{condition}
                           THEN 1 
                           ELSE 0 END
                        """
                    ).format(
                        input_col=input_cols[i],
                        condition=condition,
                    )
                    for i in range(len(input_child.cols))
                )
            )
        else:
            assert False
        if input_child.out_ref_type != RefType.FF:
            window_size_sql = compute_window_size_expression(input_child, exec_context)
            return agg_sql + window_size_sql
        else:
            return sql.SQL(
                """(SELECT {agg_sql}
                               FROM {table_name})
                           """
            ).format(agg_sql=agg_sql, table_name=base_table)
    else:
        assert False


def translate_index_function_to_window(subtree: DBFuncExecNode, exec_context: DBExecContext) -> sql.SQL:
    children = subtree.children
    col_index = 0
    if len(children) == 3:
        col_index = children[2].literal
    agg_col = children[0].cols[col_index]
    row_idx_col = children[1].cols[0]
    agg_sql = sql.SQL("""nth_value({agg_col},{idx_col})""").format(agg_col=agg_col, idx_col=row_idx_col)
    window_size_sql = compute_window_size_expression(subtree.children[0], exec_context)
    return agg_sql + window_size_sql


def translate_reference(refNode: DBRefExecNode, exec_context: DBExecContext, base_table: str) -> sql.SQL:
    ref_col = refNode.cols[0]
    if refNode.out_ref_type == RefType.RR:
        agg_sql = sql.SQL("""SUM({ref_col})""").format(ref_col=ref_col)
        window_size_sql = compute_window_size_expression(ref_col, exec_context)
        return agg_sql + window_size_sql
    else:  # FF
        return sql.SQL(
            """(SELECT {ref_col}
                           FROM {table_name}
                           WHERE {row_id} = {row_offset} + 1
                           )
                       """
        ).format(
            ref_col=sql.Identifier(ref_col),
            table_name=sql.Identifier(base_table),
            row_id=sql.Identifier(ROW_ID),
            row_offset=sql.Identifier(refNode.ref.row),
        )


def translate_literal(litNode: DBLitExecNode, exec_context: DBExecContext) -> sql.SQL:
    return sql.SQL("""{literal}""").format(literal=litNode.literal)


def translate_lookup_function(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    ref_children = subtree.children[:3]
    lit_child = subtree.children[3]
    if (
        all(isinstance(child, DBRefExecNode) for child in ref_children)
        and isinstance(lit_child, DBLitExecNode)
        and ref_children[0].out_ref_type == RefType.RR
        and all(child.out_ref_type == RefType for child in ref_children[1:])
    ):
        source_col = ref_children[0].cols[0]
        search_col = ref_children[1].cols[0]
        target_col = ref_children[2].cols[0]
        if lit_child.literal == 0:
            return sql.SQL(
                """SELECT search_id, {target_col}
                           FROM (
                           SELECT t1.{row_id} as search_id, MIN(t2.{row_id}) as target_id
                           FROM {base_table} t1
                           LEFT JOIN T t2 ON t1.{source_col} = t2.{search_col}
                           GROUP BY search_id) Temp
                           LEFT JOIN {base_table} t3 on Temp.target_id = t3.{row_id}"""
            ).format(
                source_col=sql.Identifier(source_col),
                search_col=sql.Identifier(search_col),
                target_col=sql.Identifier(target_col),
                base_table=sql.Identifier(base_table),
                row_id=sql.Identifier(ROW_ID),
            )
        else:
            if lit_child.literal == 1:
                agg_op = "max"
                comp_op = ">="
            elif lit_child.literal == -1:
                agg_op = "min"
                comp_op = "<="
            else:
                assert False
            return sql.SQL(
                """SELECT search_id, {target_col}
                           FROM (
                           SELECT t1.{row_id} as search_id, {agg_op}(t2.{row_id}) as target_id
                           FROM {base_table} t1
                           LEFT JOIN {base_table} t2 ON t1.{source_col} {comp_op} t2.{search_col}
                           GROUP BY t1.row_id) Temp
                           LEFT JOIN {base_table} t3 ON Temp.target_id = t3.{row_id}
                           """
            ).format(
                source_col=sql.Identifier(source_col),
                search_col=sql.Identifier(search_col),
                target_col=sql.Identifier(target_col),
                base_table=sql.Identifier(base_table),
                row_id=sql.Identifier(ROW_ID),
                agg_op=sql.Identifier(agg_op),
                comp_op=sql.Identifier(comp_op),
            )
    else:
        return translate_using_udf(subtree, exec_context)


def translate_match_function(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    pass


def translate_index_function_to_join(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    children = subtree.children
    col_index = 0
    if len(children) == 3:
        col_index = children[2].literal
    target_col = children[0].cols[col_index]
    row_idx_col = children[1].cols[0]
    table_name = base_table
    offset = 0
    return sql.SQL(
        """SELECT t1.{row_id}, {target_col})
                   FROM {table_name} t1
                   LEFT JOIN {table_name} t2 ON t1.{row_idx_col} + {offset} = t2.{row_id}
                   """
    ).format(
        row_id=sql.Identifier(ROW_ID),
        target_col=sql.Identifier(target_col),
        table_name=sql.Identifier(table_name),
        row_idx_col=sql.Identifier(row_idx_col),
        offset=sql.Identifier(offset),
    )


def translate_using_udf(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    assert False


def compute_window_size_expression(refNode: DBRefExecNode, exec_context: DBExecContext) -> sql.SQL:
    row_offset_start, start_dir = compute_offset_and_direction(
        exec_context.formula_id_start, refNode.ref.row
    )
    row_offset_end, end_dir = compute_offset_and_direction(
        exec_context.formula_id_start, refNode.ref.last_row
    )
    window_size_sql = None
    if refNode.out_ref_type == RefType.RR:
        window_size_sql = sql.SQL(
            """OVER (ORDER BY {row_id}
                             ROWS BETWEEN {row_offset_start} {start_dir}
                             AND {row_offset_end} {end_dir})"""
        ).format(
            row_id=ROW_ID,
            row_offset_start=row_offset_start,
            start_dir=start_dir,
            row_offset_end=row_offset_end,
            end_dir=end_dir,
        )
    elif refNode.out_ref_type == RefType.RF:
        window_size_sql = sql.SQL(
            """OVER (ORDER BY {row_id}
                             ROWS BETWEEN {row_offset_start} {start_dir}
                             AND UNBOUNDED FOLLOWING)"""
        ).format(row_id=ROW_ID, row_offset_start=row_offset_start, start_dir=start_dir)
    elif refNode.out_ref_type == RefType.FR:
        window_size_sql = sql.SQL(
            """OVER (ORDER BY {row_id}
                             ROWS BETWEEN UNBOUNDED PRECEDING
                             AND {row_offset_end} {end_dir})"""
        ).format(
            row_id=ROW_ID,
            row_offset_end=row_offset_end,
            end_dir=end_dir,
        )
    else:
        assert False
    return window_size_sql


def compute_offset_and_direction(row_id: int, ref_row_idx: int) -> tuple[int, str]:
    ref_row_id = ref_row_idx + 1
    if row_id < ref_row_id:
        return ((ref_row_id - row_id), WINDOW_PRECEDING)
    else:
        return ((row_id - ref_row_id), WINDOW_FOLLOWING)
