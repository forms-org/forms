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

from forms.core.catalog import BASE_TABLE, ROW_ID, TRANSLATE_TEMP_TABLE, TEMP_TABLE_COL_SUFFIX
from forms.core.config import DBExecContext
from forms.executor.dbexecutor.dbexecnode import DBExecNode, DBFuncExecNode, DBLitExecNode, DBRefExecNode

from psycopg2 import sql
from psycopg2.sql import Composable

from forms.utils.functions import (
    ARITHMETIC_FUNCTIONS,
    COMPARISON_FUNCTIONS,
    COLUMN_FUNCTIONS,
    DB_AGGREGATE_FUNCTIONS,
    DB_AGGREGATE_IF_FUNCTIONS,
    Function,
)
from forms.utils.reference import RefType

WINDOW_PRECEDING = sql.SQL("PRECEDING")
WINDOW_FOLLOWING = sql.SQL("FOLLOWING")

LOCAL_TEMP_TABLE_PREFIX = "FormS_Local_Temp_"
local_temp_table_number: int = 0


def translate(
    subtree: DBExecNode, exec_context: DBExecContext, intermediate_table_name, is_root_subtree: bool
) -> Composable:
    global local_temp_table_number
    local_temp_table_number = 0

    if isinstance(subtree, DBRefExecNode):
        ret_sql = translate_cell_reference(subtree, exec_context, BASE_TABLE)
        # sql.SQL(
        #     """
        #     SELECT {row_id}, {col_name}
        #     FROM {table_name}
        #     WHERE {row_id} >= {formula_index_start}
        #     AND {row_id} < {formula_index_end}
        #     """
        # ).format(
        #     row_id=sql.Identifier(ROW_ID),
        #     col_name=sql.Identifier(subtree.cols[0]),
        #     table_name=sql.Identifier(BASE_TABLE),
        #     formula_idx_start=sql.Literal(exec_context.formula_id_start),
        #     formula_idx_end=sql.Literal(exec_context.formula_id_end),
        # ))
    elif isinstance(subtree, DBFuncExecNode):
        base_table = find_or_generate_base_table(subtree)
        if subtree.translatable_to_window:
            ret_sql = translate_to_one_window_query(
                subtree, exec_context, base_table, intermediate_table_name
            )
        elif subtree.function == Function.LOOKUP:
            ret_sql = translate_lookup_function(
                subtree, exec_context, base_table, intermediate_table_name
            )
        elif subtree.function == Function.MATCH:
            ret_sql = translate_match_function(
                subtree, exec_context, base_table, intermediate_table_name
            )
        elif subtree.function == Function.INDEX:
            ret_sql = translate_index_function_to_join(
                subtree, exec_context, base_table, intermediate_table_name
            )
        else:
            ret_sql = translate_using_udf(subtree, exec_context, base_table, intermediate_table_name)
        # ret_sql = get_required_formula_results(ret_sql, exec_context)
    else:
        assert False
    if not is_root_subtree:
        ret_sql = create_temp_table(ret_sql, intermediate_table_name)
    return ret_sql


def get_required_formula_results(subquery_str: sql.SQL, exec_context: DBExecContext) -> Composable:
    return sql.SQL(
        """SELECT * FROM ({subquery}) AS {temp_table} 
           WHERE {row_id} >= {formula_id_start}
           AND {row_id} < {formula_id_end}"""
    ).format(
        subquery=sql.Identifier(subquery_str),
        temp_table=sql.Identifier(TRANSLATE_TEMP_TABLE),
        row_id=sql.Identifier(ROW_ID),
        formula_id_start=sql.Literal(exec_context.formula_id_start),
        formula_id_end=sql.Literal(exec_context.formula_id_end),
    )


def next_local_temp_table_name() -> str:
    global local_temp_table_number
    local_temp_table_number = local_temp_table_number + 1
    return f"{LOCAL_TEMP_TABLE_PREFIX}{local_temp_table_number}"


def create_temp_table(sel_query: Composable, subtree_temp_table_name: str) -> Composable:
    return sql.SQL("""CREATE TEMP TABLE {table_name} AS {sel_query}""").format(
        sel_query=sel_query, table_name=sql.Identifier(subtree_temp_table_name)
    )


def find_or_generate_base_table(subtree: DBExecNode) -> Composable:
    collected_ref_node_list = subtree.collect_ref_nodes_in_order()
    # deduplicate
    seen = False
    ref_node_list = []
    for ref_node in collected_ref_node_list:
        if ref_node.table.table_name == BASE_TABLE and not seen:
            seen = True
            ref_node_list.append(ref_node)
        elif ref_node.table.table_name != BASE_TABLE:
            ref_node_list.append(ref_node)
    if len(ref_node_list) == 1:
        return sql.SQL("""{}""").format(sql.Identifier(ref_node_list[0].table.table_name))
    else:
        table_list = [sql.Identifier(ref_node.table.table_name) for ref_node in ref_node_list]
        join_condition_list = [
            sql.SQL("""{table_one}.{row_id} = {table_two}.{row_id}""").format(
                table_one=sql.Identifier(ref_node_list[i - 1].table.table_name),
                table_two=sql.Identifier(ref_node_list[i].table.table_name),
                row_id=sql.Identifier(ROW_ID),
            )
            for i in range(1, len(ref_node_list))
        ]
        column_list = []
        for ref_node in collected_ref_node_list:
            for column_str in ref_node.cols:
                if column_str != ROW_ID:
                    column_list.append(sql.Identifier(column_str))

        return sql.SQL(
            """(SELECT {first_table}.{row_id}, {columns}
                           FROM {tables}
                           WHERE {join_conditions}) {table_name}
                        """
        ).format(
            first_table=sql.Identifier(ref_node_list[0].table.table_name),
            row_id=sql.Identifier(ROW_ID),
            columns=sql.SQL(",").join(column_list),
            tables=sql.SQL(",").join(table_list),
            join_conditions=sql.SQL("AND").join(join_condition_list),
            table_name=sql.Identifier(next_local_temp_table_name()),
        )


def translate_cell_reference(
    ref_node: DBRefExecNode, exec_context: DBExecContext, base_table: str
) -> sql.SQL:
    pass


def translate_to_one_window_query(
    subtree: DBFuncExecNode,
    exec_context: DBExecContext,
    base_table: Composable,
    subtree_temp_table_name: str,
) -> Composable:
    # ret_sql = sql.SQL("""SELECT {row_id}, """).format(row_id=sql.Identifier(ROW_ID))
    # ret_sql = ret_sql + translate_window_clause(subtree, exec_context, base_table)
    # ret_sql = ret_sql + sql.SQL(""" FROM {table_name} t2""").format(
    #    table_name=create_quantified_table_for_window_query(subtree, base_table)
    # )
    return sql.SQL(
        """
            SELECT {row_id}, {window_clause} AS {new_column_name}
            FROM {base_table}
        """
    ).format(
        row_id=sql.Identifier(ROW_ID),
        window_clause=translate_window_clause(subtree, exec_context, base_table),
        new_column_name=sql.Identifier(subtree_temp_table_name + TEMP_TABLE_COL_SUFFIX),
        base_table=base_table,
    )


def create_quantified_table_for_window_query(
    subtree: DBFuncExecNode, base_table: Composable
) -> Composable:
    if (
        subtree.function in DB_AGGREGATE_FUNCTIONS
        or subtree.function in DB_AGGREGATE_IF_FUNCTIONS
        or subtree.function == Function.INDEX
    ):
        child_ref = subtree.children[0]
        if isinstance(child_ref, DBRefExecNode):
            if child_ref.out_ref_type == RefType.RF:
                return sql.SQL(
                    """(SELECT *
                                   FROM {table_name} t2
                                   WHERE t2.{row_id} <= {last_row}
                                  )
                               """
                ).format(
                    table_name=base_table,
                    row_id=sql.Identifier(ROW_ID),
                    last_row=sql.Literal(child_ref.ref.last_row),
                )
            elif child_ref.out_ref_type == RefType.FR:
                return sql.SQL(
                    """(SELECT *
                                   FROM {table_name} t2
                                   WHERE t2.{row_id} >= {first_row}
                                  )
                               """
                ).format(
                    table_name=base_table,
                    row_id=sql.Identifier(ROW_ID),
                    first_row=sql.Literal(child_ref.ref.row),
                )

    return sql.SQL("""{table_name}""").format(table_name=base_table)


def translate_window_clause(
    subtree: DBExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    if isinstance(subtree, DBFuncExecNode):
        if subtree.function == Function.IF:
            return translate_if_function(subtree, exec_context, base_table)
        elif subtree.function in COMPARISON_FUNCTIONS or subtree.function in ARITHMETIC_FUNCTIONS:
            return translate_comparison_and_arithmetic_functions(subtree, exec_context, base_table)
        elif subtree.function in COLUMN_FUNCTIONS:
            return translate_column_functions(subtree, exec_context, base_table)
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
        return translate_literal(subtree)
    else:
        assert False


def translate_if_function(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    children = subtree.children
    if isinstance(children[1], DBLitExecNode) and children[1].literal == "NULL":
        return sql.SQL(
            """CASE
                WHEN {condition}
                THEN NULL 
                ELSE {false_result}
           END"""
        ).format(
            condition=translate_window_clause(children[0], exec_context, base_table),
            false_result=translate_window_clause(children[2], exec_context, base_table),
        )

    return sql.SQL(
        """CASE
                WHEN {condition}
                THEN {true_result}
                ELSE {false_result}
           END"""
    ).format(
        condition=translate_window_clause(children[0], exec_context, base_table),
        true_result=translate_window_clause(children[1], exec_context, base_table),
        false_result=translate_window_clause(children[2], exec_context, base_table),
    )


def translate_comparison_and_arithmetic_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    children = subtree.children
    return sql.SQL("""({left_clause} {function} {right_clause})""").format(
        left_clause=translate_window_clause(children[0], exec_context, base_table),
        function=sql.SQL(subtree.function.value),
        right_clause=translate_window_clause(children[1], exec_context, base_table),
    )


def translate_column_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    return sql.SQL("""{col_func}({agg_column})""").format(
        col_func=sql.SQL(subtree.function.value),
        agg_column=sql.SQL(",").join(
            translate_window_clause(child, exec_context, base_table) for child in subtree.children
        ),
    )


def translate_aggregate_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    child = subtree.children[0]
    if isinstance(child, DBRefExecNode):
        if subtree.function == Function.SUM:
            agg_sql = sql.SQL("""{function}({agg_column})""").format(
                function=sql.SQL(subtree.function.value),
                agg_column=sql.SQL("+").join(sql.Identifier(col) for col in child.cols),
            )
        elif subtree.function == Function.AVG:
            col_count = float(len(child.cols))
            agg_sql = sql.SQL("""AVG(({agg_column})/{col_count})""").format(
                agg_column=sql.SQL("+").join(sql.Identifier(col) for col in child.cols),
                col_count=sql.Literal(col_count),
            )
        elif subtree.function == Function.MAX or subtree.function == Function.MIN:
            row_func = "MAX" if subtree.function == Function.MAX else "MIN"
            col_func = "GREATEST" if subtree.function == Function.MAX else "LEAST"
            agg_sql = sql.SQL("""{row_func}({col_func}({agg_column}))""").format(
                row_func=sql.SQL(row_func),
                col_func=sql.SQL(col_func),
                agg_column=sql.SQL(",").join(sql.Identifier(col) for col in child.cols),
            )
        elif subtree.function == Function.COUNT:
            agg_sql = sql.SQL("""{num_columns} * {function}({agg_column})""").format(
                num_columns=sql.Literal(len(child.cols)),
                function=sql.SQL(subtree.function.value),
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
            ).format(agg_sql=sql.Identifier(agg_sql), table_name=base_table)
    else:
        assert False


def translate_aggregate_if_functions(
    subtree: DBFuncExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    input_child = subtree.children[0]
    if isinstance(input_child, DBRefExecNode):
        output_child = input_child
        if len(subtree.children) == 3:
            output_child = subtree.children[2]
        condition = subtree.children[1].literal.replace('"', "")
        input_cols = input_child.cols
        output_cols = output_child.cols
        if subtree.function == Function.SUMIF:
            agg_sql = sql.SQL("""SUM({agg_expression})""").format(
                agg_expression=sql.SQL("+").join(
                    sql.SQL(
                        """ 
                            CASE
                                WHEN {input_col}{condition}
                                THEN {output_col}
                                ELSE 0
                            END
                        """
                    ).format(
                        input_col=sql.Identifier(input_cols[i]),
                        condition=sql.SQL(condition),
                        output_col=sql.Identifier(output_cols[i]),
                    )
                    for i in range(len(input_child.cols))
                )
            )
        elif subtree.function == Function.MAXIF or subtree.function == Function.MINIF:
            row_func = "MAX" if subtree.function == Function.MAXIF else "MIN"
            col_func = "GREATEST" if subtree.function == Function.MAXIF else "LEAST"
            agg_sql = sql.SQL("""{row_func}({col_func}({agg_expression}))""").format(
                row_func=sql.SQL(row_func),
                col_func=sql.SQL(col_func),
                agg_expression=sql.SQL(",").join(
                    sql.SQL(
                        """ 
                            CASE
                                WHEN {input_col}{condition}
                                THEN {output_col}
                                ELSE NULL
                            END
                        """
                    ).format(
                        input_col=sql.Identifier(input_cols[i]),
                        condition=sql.SQL(condition),
                        output_col=sql.Identifier(output_cols[i]),
                    )
                    for i in range(len(input_child.cols))
                ),
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
                        input_col=sql.Identifier(input_cols[i]),
                        condition=sql.SQL(condition),
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
            ).format(agg_sql=sql.Identifier(agg_sql), table_name=base_table)
    else:
        assert False


def translate_index_function_to_window(
    subtree: DBFuncExecNode, exec_context: DBExecContext
) -> Composable:
    children = subtree.children
    col_index = 0
    if len(children) == 3:
        col_index = int(children[2].literal)
    agg_col = children[0].cols[col_index]
    row_idx_col = children[1].cols[0]
    agg_sql = sql.SQL("""nth_value({agg_col},{idx_col})""").format(
        agg_col=sql.Identifier(agg_col), idx_col=sql.Identifier(row_idx_col)
    )
    window_size_sql = compute_window_size_expression(subtree.children[0], exec_context)
    return agg_sql + window_size_sql


def translate_reference(
    ref_node: DBRefExecNode, exec_context: DBExecContext, base_table: Composable
) -> Composable:
    ref_col = ref_node.cols[0]
    ref_col_type = ref_node.table.get_column_type(0)
    if ref_node.out_ref_type == RefType.RR and ref_col_type == "boolean":
        return sql.SQL("""{ref_col}""").format(ref_col=sql.Identifier(ref_col))
    elif ref_node.out_ref_type == RefType.RR:
        agg_sql = sql.SQL("""SUM({ref_col})""").format(ref_col=sql.Identifier(ref_col))
        window_size_sql = compute_window_size_expression(ref_node, exec_context)
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
            table_name=base_table,
            row_id=sql.Identifier(ROW_ID),
            row_offset=sql.Literal(ref_node.ref.row),
        )


def translate_literal(lit_node: DBLitExecNode) -> Composable:
    return sql.SQL("""{literal}""").format(literal=sql.Literal(lit_node.literal))


def translate_lookup_function(
    subtree: DBFuncExecNode,
    exec_context: DBExecContext,
    base_table: Composable,
    subtree_temp_table_name: str,
) -> Composable:
    ref_children = subtree.children[:3]
    lit_child = subtree.children[3]
    if (
        all(isinstance(child, DBRefExecNode) for child in ref_children)
        and isinstance(lit_child, DBLitExecNode)
        and ref_children[0].out_ref_type == RefType.RR
        and all(child.out_ref_type == RefType.FF for child in ref_children[1:])
    ):
        source_col = ref_children[0].cols[0]
        search_col = ref_children[1].cols[0]
        target_col = ref_children[2].cols[0]

        table_one = next_local_temp_table_name()
        table_two = next_local_temp_table_name()
        table_three = next_local_temp_table_name()
        temp_table = next_local_temp_table_name()
        if lit_child.literal == 0:
            return sql.SQL(
                """
                    SELECT search_id AS {row_id}, {target_col} AS {new_column_name}
                    FROM (
                           SELECT {table_one}.{row_id} as search_id, MIN({table_two}.{row_id}) as target_id
                           FROM {base_table} {table_one}
                           LEFT JOIN {base_table} {table_two} ON {table_one}.{source_col} = {table_two}.{search_col}
                           GROUP BY search_id
                         ) {temp_table}
                    LEFT JOIN {base_table} {table_three} on {temp_table}.target_id = {table_three}.{row_id}
                    ORDER BY search_id
                """
            ).format(
                source_col=sql.Identifier(source_col),
                search_col=sql.Identifier(search_col),
                target_col=sql.Identifier(target_col),
                base_table=base_table,
                temp_table=sql.Identifier(temp_table),
                row_id=sql.Identifier(ROW_ID),
                table_one=sql.Identifier(table_one),
                table_two=sql.Identifier(table_two),
                table_three=sql.Identifier(table_three),
                new_column_name=sql.Identifier(subtree_temp_table_name + TEMP_TABLE_COL_SUFFIX),
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
                """
                        SELECT search_id, {target_col}
                        FROM (
                           SELECT {table_one}.{row_id} as search_id, {agg_op}({table_two}.{row_id}) as target_id
                           FROM {base_table} {table_one}
                           LEFT JOIN {base_table} {table_two} ON {table_one}.{source_col} {comp_op} {table_two}.{search_col}
                           GROUP BY {table_one}.row_id) {temp_table}
                        LEFT JOIN {base_table} {table_three} ON {temp_table}.target_id = {table_three}.{row_id}
                        ORDER BY search_id
                """
            ).format(
                source_col=sql.Identifier(source_col),
                search_col=sql.Identifier(search_col),
                target_col=sql.Identifier(target_col),
                base_table=base_table,
                temp_table=sql.Identifier(temp_table),
                row_id=sql.Identifier(ROW_ID),
                table_one=sql.Identifier(table_one),
                table_two=sql.Identifier(table_two),
                table_three=sql.Identifier(table_three),
                agg_op=sql.SQL(agg_op),
                comp_op=sql.SQL(comp_op),
            )
    else:
        return translate_using_udf(subtree, exec_context)


def translate_match_function(
    subtree: DBFuncExecNode,
    exec_context: DBExecContext,
    base_table: Composable,
    subtree_temp_table_name: str,
) -> Composable:
    pass


def translate_index_function_to_join(
    subtree: DBFuncExecNode,
    exec_context: DBExecContext,
    base_table: Composable,
    subtree_temp_table_name: str,
) -> Composable:
    children = subtree.children
    col_index = 0
    if len(children) == 3:
        col_index = int(children[2].literal)
    target_col = children[0].cols[col_index]
    row_idx_col = children[1].cols[0]
    alias_one = next_local_temp_table_name()
    alias_two = next_local_temp_table_name()
    offset = 1

    return sql.SQL(
        """
            SELECT {table_one}.{row_id} AS {row_id}, {table_two}.{target_col} AS {new_column_name}
            FROM {table_name} {table_one}
            LEFT JOIN {table_name} {table_two} 
            ON {table_one}.{row_idx_col} + {offset} = {table_two}.{row_id}
            ORDER BY {table_one}.{row_id}
        """
    ).format(
        row_id=sql.Identifier(ROW_ID),
        target_col=sql.Identifier(target_col),
        table_name=base_table,
        row_idx_col=sql.Identifier(row_idx_col),
        offset=sql.Literal(offset),
        table_one=sql.SQL(alias_one),
        table_two=sql.SQL(alias_two),
        new_column_name=sql.Identifier(subtree_temp_table_name + TEMP_TABLE_COL_SUFFIX),
    )


def translate_using_udf(
    subtree: DBFuncExecNode,
    exec_context: DBExecContext,
    base_table: Composable,
    subtree_temp_table_name: str,
) -> Composable:
    pass


def compute_window_size_expression(ref_node: DBRefExecNode, exec_context: DBExecContext) -> Composable:
    row_offset_start, start_dir = compute_offset_and_direction(
        exec_context.formula_id_start, ref_node.ref.row
    )
    row_offset_end, end_dir = compute_offset_and_direction(
        exec_context.formula_id_start, ref_node.ref.last_row
    )
    if ref_node.out_ref_type == RefType.RR:
        window_size_sql = sql.SQL(
            """ OVER (ORDER BY {row_id}
                             ROWS BETWEEN {row_offset_start} {start_dir}
                             AND {row_offset_end} {end_dir})"""
        ).format(
            row_id=sql.Identifier(ROW_ID),
            row_offset_start=sql.Literal(row_offset_start),
            start_dir=start_dir,
            row_offset_end=sql.Literal(row_offset_end),
            end_dir=end_dir,
        )
    elif ref_node.out_ref_type == RefType.RF:
        window_size_sql = sql.SQL(
            """OVER (ORDER BY {row_id}
                             ROWS BETWEEN {row_offset_start} {start_dir}
                             AND UNBOUNDED FOLLOWING)"""
        ).format(
            row_id=sql.Identifier(ROW_ID),
            row_offset_start=sql.Literal(row_offset_start),
            start_dir=start_dir,
        )
    elif ref_node.out_ref_type == RefType.FR:
        window_size_sql = sql.SQL(
            """OVER (ORDER BY {row_id}
                             ROWS BETWEEN UNBOUNDED PRECEDING
                             AND {row_offset_end} {end_dir})"""
        ).format(
            row_id=sql.Identifier(ROW_ID),
            row_offset_end=sql.Literal(row_offset_end),
            end_dir=end_dir,
        )
    else:
        assert False
    return window_size_sql


def compute_offset_and_direction(row_id: int, ref_row_idx: int) -> tuple[int, sql.SQL]:
    ref_row_id = ref_row_idx + 1
    if row_id > ref_row_id:
        return (row_id - ref_row_id), WINDOW_PRECEDING
    else:
        return (ref_row_id - row_id), WINDOW_FOLLOWING
