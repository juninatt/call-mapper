import pandas as pd


def preprocess_data(df):
    df['@timestamp'] = pd.to_datetime(df['@timestamp'].str.strip('"'), errors='coerce', format="%Y-%m-%dT%H:%M:%S")
    df_valid = df.dropna(subset=['@timestamp'])
    invalid_timestamps = df[df['@timestamp'].isna()]
    return df_valid, invalid_timestamps

