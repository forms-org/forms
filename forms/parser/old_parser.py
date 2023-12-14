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

import glob
import os
import xlsxwriter
import jpype
import jpype.imports

from forms.planner.plannode import PlanNode, RefNode, FunctionNode, LiteralNode
from forms.utils.reference import Ref, RefType, DEFAULT_AXIS
from forms.utils.functions import from_function_str
from forms.utils.treenode import link_parent_to_children

dir_path = os.path.dirname(os.path.realpath(__file__))
workbook_name = os.path.join(dir_path, "workbook.xlsx")
formula_position = "A1"


def parse_formula(formula_string: str) -> PlanNode:
    """
    Parse a formula string using POI library and a formula plan tree
    Args:
        formula_string (str):
    """
    workbook = xlsxwriter.Workbook(workbook_name)
    worksheet = workbook.add_worksheet()
    worksheet.write(formula_position, formula_string)
    workbook.close()

    root_dir = os.path.dirname(os.path.abspath(__file__))
    jars = glob.glob(root_dir + "/**/*.jar", recursive=True)
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=":".join(jars))

    # Import of Java classes must happen *after* jpype.startJVM() is called
    from org.dataspread.sheetanalyzer import SheetAnalyzer

    sheet = SheetAnalyzer.createSheetAnalyzer(workbook_name)
    root = sheet.getFormulaTree()
    parsed_root = parse_subtree(root)

    return parsed_root


def shut_down_jvm():
    jpype.shutdownJVM()


def parse_subtree(node) -> PlanNode:
    if node.isLeafNode:
        if node.isRef:
            row = int(node.rowStart)
            col = int(node.colStart)
            last_row = int(node.rowEnd)
            last_col = int(node.colEnd)
            is_first_relative = bool(node.startRelative)
            is_last_relative = bool(node.endRelative)

            ref = Ref(row, col, last_row, last_col)
            if is_first_relative and is_last_relative:
                ref_type = RefType.RR
            elif is_first_relative and not is_last_relative:
                ref_type = RefType.RF
            elif not is_first_relative and is_last_relative:
                ref_type = RefType.FR
            else:
                ref_type = RefType.FF
            return RefNode(ref, ref_type, DEFAULT_AXIS)
        else:
            return LiteralNode(float(node.value), DEFAULT_AXIS)
    else:
        function = from_function_str(str(node.value))
        parent = FunctionNode(function, DEFAULT_AXIS)
        children = [parse_subtree(child) for child in node.children]
        link_parent_to_children(parent, children)
        return parent
