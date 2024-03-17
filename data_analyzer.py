def analyze_data(df_valid):
    results = {
        'row_count': len(df_valid),
        'earliest_date': df_valid['@timestamp'].min(),
        'latest_date': df_valid['@timestamp'].max(),
        'sorted': df_valid['@timestamp'].is_monotonic_increasing
    }
    return results

