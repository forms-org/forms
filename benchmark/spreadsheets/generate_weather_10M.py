import pandas as pd

df = pd.read_csv('weather.csv')
new_df = pd.concat([df] * 10)
new_df.to_csv('weather_10M.csv', index=False)
