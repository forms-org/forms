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

import numpy as np
import pandas as pd
import string

from forms.core.forms import config
from forms.core.config import forms_config


def test_config():
    config(2, "NotSimple")
    assert forms_config.cores == 2
    assert forms_config.scheduler == "NotSimple"


# Small test dataframe. 12 columns, 40 rows
test_df = pd.DataFrame(
    {
        "col1": ["A", "B", "C", "D"] * 10,
        "col2": [1] * 40,
        "col3": range(40),
        "col4": [1, 2, 3, 4] * 10,
        "col5": [-1, 2, -3, 4] * 10,
        "col6": [0] * 40,
        "col7": [0.4111, 1.6222, 2.93333333, 3.999] * 10,
        "col8": [0, 30, 60, 90] * 10,
        "col9": ["I", "VI", "IX", "ML"] * 10,
        "col10": [np.pi / 2] * 40,
        "col11": [2] * 40,
        "col12": [16] * 40,
    }
)


DF_ROWS = 1000000

test_strings = [i + j + k + x + y
    for i in string.ascii_lowercase
    for j in string.ascii_lowercase
    for k in string.ascii_lowercase
    for x in string.ascii_lowercase
    for y in string.ascii_lowercase
]

np.random.seed(1)

# Larger test dataframe. 12 columns, 1000 rows
test_df_big = pd.DataFrame(
    {
        "col1": np.tile(["A", "B", "C", "D"], (DF_ROWS // 4)),
        "col2": np.tile([1], DF_ROWS),
        "col3": range(DF_ROWS),
        "col4": np.tile([1, 2, 3, 4], (DF_ROWS // 4)),
        "col5": np.tile([-1, 2, -3, 4], (DF_ROWS // 4)),
        "col6": np.tile([0], DF_ROWS),
        "col7": np.tile([0.4111, 1.6222, 2.93333333, 3.999], (DF_ROWS // 4)),
        "col8": np.tile([0, 30, 60, 90], (DF_ROWS // 4)),
        "col9": np.tile(["I", "VI", "IX", "ML"], (DF_ROWS // 4)),
        "col10": np.tile([np.pi / 2], DF_ROWS),
        "col11": np.tile([2], DF_ROWS),
        "col12": np.tile([16], DF_ROWS),
        "col13": test_strings[:DF_ROWS],
        "col14": range(DF_ROWS),
        "col15": np.random.choice(test_strings, DF_ROWS, replace=False)
    }
)
