import pandas as pd
import os

data_path = 'data/2022.csv'
if os.path.exists(data_path):
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    df = df.set_index(['date', 'type'])
    df.loc[('20220224', 'wbill'), 'variety_ids'] = 123
    df.loc[('20220224', 'wbill'), 'variety_ids'] = 123
    df.loc[('20220224', 'wbill'), 'variety_ids'] = 123
    df.loc[('20220224', 'wbill'), 'variety_ids'] = 123
    df.loc[('20220224', 'wbill'), 'variety_ids'] = 123
    print(df)