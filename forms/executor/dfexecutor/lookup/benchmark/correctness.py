import numpy as np
import pandas as pd
from dask.distributed import Client

from forms.executor.dfexecutor.lookup.algorithm.vlookup_approx import vlookup_approx_np_vector
from forms.executor.dfexecutor.lookup.algorithm.vlookup_exact import vlookup_exact_pd_merge
from forms.executor.dfexecutor.lookup.distributed.vlookup_approx import vlookup_approx_distributed
from forms.executor.dfexecutor.lookup.distributed.vlookup_exact import vlookup_exact_distributed

girls_names_df = pd.Series([
    'Amelia', 'Brooklyn', 'Charlotte', 'Delilah', 'Emma', 'Freya', 'Gianna', 'Harper',
    'Isabella', 'Josephine', 'Kinsley', 'Luna', 'Mia', 'Nora', 'Olivia', 'Penelope', 'Quinn',
    'Riley', 'Sophia', 'Taylor', 'Unique', 'Violet', 'Willow', 'Ximena', 'Yaretzi', 'Zoey'
])
boys_names_df = pd.Series([
    'Alexander', 'Benjamin', 'Carter', 'Daniel', 'Elijah', 'Finn', 'Grayson', 'Henry',
    'Isaac', 'James', 'Kai', 'Liam', 'Mateo', 'Noah', 'Oliver', 'Parker', 'Quincy', 'Ryan',
    'Sebastian', 'Theodore', 'Uriel', 'Vincent', 'William', 'Xavier', 'Yusuf', 'Zion',
])


def test_vlookup_exact_string(client):
    values = boys_names_df
    df = pd.DataFrame({"names": girls_names_df, 0: list(range(len(girls_names_df)))})
    col_idxes = pd.Series(np.full(len(girls_names_df), 2))
    computed_df = vlookup_exact_distributed(client, values, df, col_idxes, vlookup_exact_pd_merge)
    assert computed_df.iloc[:, 0].isnull().all()

    values = girls_names_df
    computed_df = vlookup_exact_distributed(client, values, df, col_idxes, vlookup_exact_pd_merge)
    expected_df = pd.DataFrame(list(range(len(girls_names_df))))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_vlookup_approx_string(client):
    values = boys_names_df
    df = pd.DataFrame({"names": girls_names_df, 0: list(range(len(girls_names_df)))})
    col_idxes = pd.Series(np.full(len(girls_names_df), 2))
    computed_df = vlookup_approx_distributed(client, values, df, col_idxes, vlookup_approx_np_vector)
    expected_df = pd.DataFrame(
        [np.nan, 0, 1, 2, 3, 4, 6, 7, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 17, 19, 20, 20, 21, 22, 24, 24])
    mask = ~(np.isnan(computed_df) | np.isnan(computed_df))
    np.allclose(computed_df[mask], expected_df[mask])


if __name__ == '__main__':
    dask_client = Client(processes=True, n_workers=4)
    test_vlookup_exact_string(dask_client)
    test_vlookup_approx_string(dask_client)
    print("All tests passed!")
