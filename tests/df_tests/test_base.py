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

from forms.core.forms import from_df


test_df = pd.DataFrame(
    {
        "col1": ["A", "B", "C", "D"] * 10,
        "col2": [1] * 40,
        "col3": ["A", "B", "C", "D"] * 10,
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


def test_open():
    wb = from_df(test_df)
    wb.close()
