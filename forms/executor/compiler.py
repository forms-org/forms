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

import formulas
from abc import ABC, abstractmethod

from forms.utils.functions import FunctionExecutor, Function
from forms.executor.executionnode import FunctionExecutionNode
from forms.utils.treenode import link_parent_to_children
from forms.utils.exceptions import FunctionExecutorNotSupportedException
from forms.utils.optimizations import FRRFOptimization


class BaseCompiler(ABC):

    @abstractmethod
    def compile(self, func_node: FunctionExecutionNode,
                function_executor: FunctionExecutor) -> FunctionExecutionNode:
        pass


class DFCompiler(BaseCompiler):

    @abstractmethod
    def compile(self, func_node: FunctionExecutionNode,
                function_executor: FunctionExecutor) -> FunctionExecutionNode:
        if function_executor == FunctionExecutor.df_pandas_executor:
            return func_node
        elif function_executor == FunctionExecutor.df_formulas_executor:
            func_node.exec_context.function_executor = function_executor
            if func_node.fr_rf_optimization != FRRFOptimization.PHASETWO:
                formula_str = func_node.construct_formula_string()
                func_node.exec_context.compiled_formula_func = formulas.Parser().ast(formula_str)[1].compile()
                func_node.function = Function.FORMULAS

                child_ref_nodes = func_node.collect_ref_nodes_in_order()
                link_parent_to_children(func_node, child_ref_nodes)
            return func_node
        else:
            raise FunctionExecutorNotSupportedException(f"{function_executor} not supported")
