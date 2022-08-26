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


class FormSException(Exception):
    """Base exception for FormS"""


class InvalidIndexException(FormSException):
    """Exception raised for invalid reference index."""


class InvalidArithmeticInputException(FormSException):
    """Exception raised for invalid arithmetic input"""


class FunctionNotSupportedException(FormSException):
    """Exception raised for not supported function"""


class SchedulerNotSupportedException(FormSException):
    """Exception raised for not supported scheduler"""


class RuntimeNotSupportedException(FormSException):
    """Exception raised for not supported runtime"""


class FormulaStringSyntaxErrorException(FormSException):
    """Exception raised for wrong formula syntax"""


class FormulaStringNotSupportedException(FormSException):
    """Exception raised for unsupported formula string"""


class AxisNotSupportedException(FormSException):
    """Exception raised for unsupported axis"""


class ExecutorNotSupportedException(FormSException):
    """Exception raised for unsupported Executor"""


class CostModelNotSupportedException(FormSException):
    """Exception raised for unsupported CostModel"""
