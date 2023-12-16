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
from forms.core.config import DBConfig, DBExecContext
from forms.executor.dbexecutor.dbexecnode import from_plan_to_execution_tree
from forms.executor.dbexecutor.scheduler import Scheduler
from forms.executor.dbexecutor.translation import translate
from forms.planner.plannode import PlanNode
from forms.utils.exceptions import DBRuntimeException
from forms.utils.generic import get_columns_and_types
from forms.utils.metrics import MetricsTracker


class DBExecutor:
    def __init__(
        self, db_config: DBConfig, exec_context: DBExecContext, metrics_tracker: MetricsTracker
    ):
        self.db_config = db_config
        self.exec_context = exec_context
        self.metrics_tracker = metrics_tracker

    def execute_formula_plan(self, formula_plan: PlanNode) -> pd.DataFrame:
        exec_tree = from_plan_to_execution_tree(formula_plan)
        scheduler = Scheduler(exec_tree)
        df = None
        try:
            while scheduler.has_next_subtree():
                exec_subtree = scheduler.next_substree()
                exec_subtree_str = translate(exec_subtree)
                if scheduler.has_next_subtree():
                    self.db_config.cursor.execute(exec_subtree_str)
                    intermediate_table = get_columns_and_types(
                        self.db_config.cursor, exec_subtree.intermediate_table_name
                    )
                    scheduler.finish_one_subtree(exec_subtree, intermediate_table)
                else:
                    df = pd.read_sql_query(exec_subtree_str, self.db_config.conn)
            self.db_config.conn.commit()
        except psycopg2.Error as e:
            self.db_config.conn.rollback()
            raise DBRuntimeException(e)

        return df

    def clean_up(self):
        pass
