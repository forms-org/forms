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
import psycopg2

from forms.core.catalog import TableCatalog
from forms.core.config import DBConfig, DBExecContext
from forms.executor.dbexecutor.dbexecnode import (
    from_plan_to_execution_tree,
    DBFuncExecNode,
    create_intermediate_ref_node,
)
from forms.executor.dbexecutor.scheduler import Scheduler
from forms.executor.dbexecutor.translation import translate
from forms.planner.plannode import PlanNode
from forms.utils.exceptions import DBRuntimeException
from forms.utils.generic import get_columns_and_types
from forms.utils.metrics import MetricsTracker
from forms.utils.treenode import link_parent_to_children


def finish_one_subtree(intermediate_table: TableCatalog, exec_subtree: DBFuncExecNode):
    intermediate_ref_node = create_intermediate_ref_node(intermediate_table, exec_subtree)

    parent = exec_subtree.parent
    children = parent.children
    children[children.index(exec_subtree)] = intermediate_ref_node

    link_parent_to_children(parent, children)


class DBExecutor:
    def __init__(
        self, db_config: DBConfig, exec_context: DBExecContext, metrics_tracker: MetricsTracker
    ):
        self.db_config = db_config
        self.exec_context = exec_context
        self.metrics_tracker = metrics_tracker

    def execute_formula_plan(self, formula_plan: PlanNode) -> pd.DataFrame:
        exec_tree = from_plan_to_execution_tree(formula_plan, self.exec_context.base_table)
        scheduler = Scheduler(exec_tree)
        df = None
        try:
            while scheduler.has_next_subtree():
                exec_subtree = scheduler.next_subtree()
                is_root_subtree = not scheduler.has_next_subtree()
                intermediate_table_name = (
                    exec_tree.intermediate_table_name if isinstance(exec_tree, DBFuncExecNode) else ""
                )
                sql_composable = translate(
                    exec_subtree, self.exec_context, intermediate_table_name, is_root_subtree
                )
                if scheduler.has_next_subtree():
                    self.exec_context.cursor.execute(sql_composable)
                    col_names, col_types = get_columns_and_types(
                        self.exec_context.cursor, intermediate_table_name
                    )
                    intermediate_table = TableCatalog(intermediate_table_name, col_names, col_types)
                    finish_one_subtree(intermediate_table, exec_subtree)
                else:
                    sql_str = sql_composable.as_string(self.exec_context.conn)
                    df = pd.read_sql_query(sql_str, self.exec_context.conn)
            self.exec_context.conn.commit()
        except psycopg2.Error as e:
            self.exec_context.conn.rollback()
            raise DBRuntimeException(e)

        return df

    def clean_up(self):
        pass
