import pandas as pd


def load_data(filepath):
    # Load CSV, skip the first row, set column names, and trim whitespace in 'fields.RequestPath'
    df = pd.read_csv(filepath, skiprows=1, names=['@timestamp', 'fields.RequestPath'])
    df['fields.RequestPath'] = df['fields.RequestPath'].str.strip()
    return df
