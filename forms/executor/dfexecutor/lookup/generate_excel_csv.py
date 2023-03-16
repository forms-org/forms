import numpy as np
import pandas as pd
import sys
import string

rows = int(sys.argv[1]) if len(sys.argv) > 1 else 250000
cols = int(sys.argv[2]) if len(sys.argv) > 2 else 10
test_strings = [i + j + k + x + y
                for i in string.ascii_lowercase
                for j in string.ascii_lowercase
                for k in string.ascii_lowercase
                for x in string.ascii_lowercase
                for y in string.ascii_lowercase
                ]
search_keys = np.random.choice(test_strings, rows, replace=False)
search_keys.sort()
result1 = np.random.choice(test_strings, rows, replace=True)
result2 = np.random.choice(test_strings, rows, replace=True)
df = pd.DataFrame({0: search_keys[:rows], 1: result1, 2: result2})
values = pd.Series(np.random.choice(test_strings, rows, replace=False))
garbage = pd.concat([pd.Series(np.random.choice(test_strings, rows, replace=True))] * (cols - 4), axis=1)
result = pd.DataFrame({0: search_keys, 1: result1, 2: result2})
result = pd.concat([result, garbage, values], axis=1)
result.to_csv("forms/executor/dfexecutor/lookup/test_lookup_df.csv", header=False, index=False)
