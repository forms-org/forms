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

from openpyxl.formula.tokenizer import Tokenizer, Token
from forms.planner.plannode import PlanNode, FunctionNode, LiteralNode, RefNode
from forms.utils.functions import from_function_str
from forms.utils.exceptions import (
    FormulaStringSyntaxErrorException,
    FormulaStringNotSupportedException,
    AxisNotSupportedException,
)
from forms.utils.treenode import link_parent_to_children
from forms.utils.reference import DEFAULT_AXIS, AXIS_ALONG_ROW, Ref, RefType

subexpression_tokens = {Token.FUNC, Token.PAREN, Token.LITERAL, Token.OPERAND, Token.OP_PRE}
op_in_post_tokens = {Token.OP_IN, Token.OP_POST}

formula_apply_axis = DEFAULT_AXIS


def parse_formula(formula_string: str, axis: int) -> PlanNode:
    if axis != AXIS_ALONG_ROW:
        raise AxisNotSupportedException(f"Axis {axis} not supported")
    global formula_apply_axis
    formula_apply_axis = axis
    tokenlizer = Tokenizer(formula_string)
    tokens = tokenlizer.items
    pos = 0
    return build_from_subexpression(tokens, pos)[0]


def build_from_subexpression(tokens, pos: int) -> (PlanNode, int):
    cur_pos = pos
    cur_plan_node = None
    last_token = None
    while cur_pos < len(tokens):
        cur_token = tokens[cur_pos]

        # detect syntax error
        if is_start_of_subexpression(cur_token) or cur_token.type in op_in_post_tokens:
            detect_syntax_error(cur_token, last_token, cur_pos)
            last_token = cur_token

        # parse tokens based on the type
        if is_start_of_subexpression(cur_token):
            new_plan_node = None
            if cur_token.type == Token.FUNC:
                new_plan_node, cur_pos = build_from_func_subexpression(tokens, cur_pos)
            elif cur_token.type == Token.PAREN:
                new_plan_node, cur_pos = build_from_paren_subexpression(tokens, cur_pos)
            elif cur_token.type in {Token.LITERAL, Token.OPERAND}:
                new_plan_node, cur_pos = build_from_literal_and_reference(tokens, cur_pos)
            elif cur_token.type == Token.OP_PRE:
                new_plan_node, cur_pos = build_from_op_pre_subexpression(tokens, cur_pos)
            cur_plan_node = add_as_child_node(cur_plan_node, new_plan_node)
        elif cur_token.type in op_in_post_tokens:
            cur_plan_node, cur_pos = build_from_op_in_post_subexpression(tokens, cur_pos, cur_plan_node)
        elif is_end_of_subexpression(cur_token):
            break
        elif cur_token.type == Token.WSPACE:
            cur_pos += 1
        else:  # Token.ARRAY
            raise_unsupported_exception(cur_pos, cur_token)

    return cur_plan_node, cur_pos


def build_from_paren_subexpression(tokens, pos: int) -> (PlanNode, int):
    new_plan_node, cur_pos = build_from_subexpression(tokens, pos + 1)
    cur_token = tokens[cur_pos]
    if not (cur_token.type == Token.PAREN and cur_token.subtype == Token.CLOSE):
        raise_syntax_exception(cur_pos, cur_token)
    return new_plan_node, cur_pos + 1


def build_from_func_subexpression(tokens, pos: int) -> (PlanNode, int):
    cur_pos = pos
    cur_token = tokens[cur_pos]

    func_value = cur_token.value
    func_str = from_func_value_to_func_str(func_value)

    func_node = FunctionNode(from_function_str(func_str), formula_apply_axis)
    func_node.open_value = func_value
    func_node.func_type = cur_token.type

    children = []
    while True:
        cur_pos += 1
        child_node, cur_pos = build_from_subexpression(tokens, cur_pos)
        cur_token = tokens[cur_pos]
        children.append(child_node)

        if cur_token.type == Token.FUNC and cur_token.subtype == Token.CLOSE:
            func_node.close_value = tokens[cur_pos].value
            break
        elif cur_token.type == Token.SEP:
            func_node.seps.append(cur_token.value)
        else:
            raise_syntax_exception(cur_pos, cur_token)
    link_parent_to_children(func_node, children)

    return func_node, cur_pos + 1


def build_from_literal_and_reference(tokens, pos: int) -> (PlanNode, int):
    cur_token = tokens[pos]
    leaf_node = None
    if cur_token.type == Token.LITERAL or (
        cur_token.type == Token.OPERAND
        and cur_token.subtype in {Token.TEXT, Token.LOGICAL, Token.NUMBER}
    ):  # Literal node
        literal = parse_literal(cur_token, pos)
        leaf_node = LiteralNode(literal, formula_apply_axis)
        leaf_node.open_value = cur_token.value
    elif cur_token.type == Token.OPERAND and cur_token.subtype == Token.RANGE:  # Range
        ref, ref_type = parse_range(pos, cur_token)
        leaf_node = RefNode(ref, ref_type, formula_apply_axis)
        leaf_node.open_value = cur_token.value
    else:
        raise_syntax_exception(pos, cur_token)
    return leaf_node, pos + 1


def parse_range(cur_pos, cur_token) -> (Ref, RefType):
    ref_list = cur_token.value.split(":")
    row, col, row_relative, col_relative = parse_ref_str(ref_list[0], cur_pos, cur_token)
    last_row = row
    last_col = col
    last_row_relative = row_relative
    last_col_relative = col_relative

    if len(ref_list) == 2:
        last_row, last_col, last_row_relative, last_col_relative = parse_ref_str(
            ref_list[1], cur_pos, cur_token
        )
    elif len(ref_list) > 2:
        raise_syntax_exception(cur_pos, cur_token)

    ref = Ref(row, col, last_row, last_col)
    ref_type = (
        parse_ref_type(row_relative, last_row_relative)
        if formula_apply_axis == AXIS_ALONG_ROW
        else parse_ref_type(col_relative, last_col_relative)
    )
    return ref, ref_type


def parse_ref_type(is_first_relative, is_last_relative) -> RefType:
    if is_first_relative and is_last_relative:
        ref_type = RefType.RR
    elif is_first_relative and not is_last_relative:
        ref_type = RefType.RF
    elif not is_first_relative and is_last_relative:
        ref_type = RefType.FR
    else:
        ref_type = RefType.FF
    return ref_type


def parse_ref_str(ref_str: str, cur_pos, cur_token) -> (int, int, bool, bool):
    pos = 0
    new_ref_str = ref_str.lower()
    col_relative = True
    if new_ref_str[pos] == "$":
        col_relative = False
        pos += 1

    col = 0
    while pos < len(new_ref_str) and new_ref_str[pos].isalpha():
        base = ord(new_ref_str[pos]) - ord("a") + 1
        col = base + col * 26
        pos += 1
    if col == 0:
        raise_syntax_exception(cur_pos, cur_token)
    col -= 1

    row_relative = True
    if pos < len(new_ref_str) and new_ref_str[pos] == "$":
        row_relative = False
        pos += 1
    if pos >= len(new_ref_str) or not new_ref_str[pos:].isdigit():
        raise_syntax_exception(cur_pos, cur_token)
    row = int(new_ref_str[pos:]) - 1
    return row, col, row_relative, col_relative


def build_from_op_pre_subexpression(tokens, pos: int) -> (PlanNode, int):
    cur_pos = pos
    cur_token = tokens[cur_pos]

    func_str = f"{cur_token.type}_{cur_token.value}"
    plan_node = FunctionNode(from_function_str(func_str), formula_apply_axis)
    plan_node.open_value = cur_token.value
    plan_node.func_type = cur_token.type

    child_plan_node, cur_pos = build_from_subexpression(tokens, cur_pos + 1)
    link_parent_to_children(plan_node, [child_plan_node])
    return plan_node, cur_pos


def build_from_op_in_post_subexpression(tokens, pos: int, child_plan_node: PlanNode) -> (PlanNode, int):
    cur_pos = pos
    cur_token = tokens[cur_pos]

    func_str = (
        f"{cur_token.type}_{cur_token.value}" if cur_token.type == Token.OP_POST else cur_token.value
    )
    cur_plan_node = FunctionNode(from_function_str(func_str), formula_apply_axis)
    cur_plan_node.open_value = cur_token.value
    cur_plan_node.func_type = cur_token.type
    link_parent_to_children(cur_plan_node, [child_plan_node])

    return cur_plan_node, cur_pos + 1


def add_as_child_node(parent_node: PlanNode, child_node: PlanNode) -> PlanNode:
    if parent_node is None:
        return child_node
    parent_node.children.append(child_node)
    return parent_node


def raise_syntax_exception(cur_pos, cur_token):
    raise FormulaStringSyntaxErrorException(f"Syntax Error at {cur_pos}: {cur_token.value}")


def raise_unsupported_exception(cur_pos, cur_token):
    raise FormulaStringNotSupportedException(f"Unsupported Error at {cur_pos}: {cur_token.value}")


def from_func_value_to_func_str(func_value) -> str:
    return func_value[:-1]


def detect_syntax_error(cur_token, last_token, cur_pos):
    if last_token is None:
        return

    if is_start_of_subexpression(cur_token) and (
        is_start_of_subexpression(last_token) or last_token.type == Token.OP_POST
    ):
        raise_syntax_exception(cur_pos, cur_token)
    elif cur_token.type == Token.OP_IN and last_token.type in {Token.OP_IN, Token.OP_PRE}:
        raise_syntax_exception(cur_pos, cur_token)
    elif cur_token.value == Token.OP_PRE and last_token is not None:
        raise_syntax_exception(cur_pos, cur_token)
    elif cur_token.value == Token.OP_POST and not is_start_of_subexpression(last_token):
        raise_syntax_exception(cur_pos, cur_token)


def parse_literal(token: Token, pos: int):
    if token.subtype == Token.TEXT:
        return str(token.value)
    elif token.subtype == Token.NUMBER:
        return float(token.value)
    elif token.subtype == Token.LOGICAL:
        return token.value.lower() == "true"
    raise_syntax_exception(pos, token)


def is_start_of_subexpression(token: Token) -> bool:
    if token.type in subexpression_tokens:
        if token.type == Token.FUNC:
            return token.subtype == Token.OPEN
        if token.type == Token.PAREN:
            return token.subtype == Token.OPEN
        return True
    return False


def is_end_of_subexpression(token: Token) -> bool:
    return (
        token.type == Token.SEP
        or (token.type == Token.FUNC and token.subtype == Token.CLOSE)
        or (token.type == Token.PAREN and token.subtype == Token.CLOSE)
    )
