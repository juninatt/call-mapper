import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath, skiprows=1, names=['@timestamp', 'fields.RequestPath'])
    return df

