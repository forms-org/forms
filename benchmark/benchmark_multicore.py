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

import argparse

import pandas as pd
import numpy as np
import forms as fs


def test_multicore(cores: int):
    df = pd.DataFrame(np.random.randint(0, 100, size=(10000000, 5)))
    formula = "=SUM(A1:C2)"
    fs.config(cores=cores)
    print(cores)
    return fs.compute_formula(df, formula)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Driver for Forms")
    parser.add_argument("--cores", help="number of cores to use", type=int, required=True)
    args = parser.parse_args()
    print(test_multicore(args.cores))
