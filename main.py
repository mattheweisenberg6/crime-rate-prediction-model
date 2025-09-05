import pandas as pd

df = pd.read_csv('crimes.csv', nrows=100)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
print(df)
print(df.columns.tolist())