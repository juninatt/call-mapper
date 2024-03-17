def save_results(filepath, results, invalid_timestamps):
    with open(filepath, 'w') as f:
        f.write(f"Results:\n")
        for key, value in results.items():
            f.write(f"{key}: {value}\n")
        f.write("Invalid Timestamps:\n")
        for index, row in invalid_timestamps.iterrows():
            f.write(f"Row {index}: {row['@timestamp']}, {row['fields.RequestPath']}\n")

