def save_results(filepath, results, invalid_timestamps):
    # Open the specified file for writing results
    with open(filepath, 'w') as f:
        # Write aggregated results data
        f.write("Results:\n")
        for key, value in results.items():
            if key == 'top_10_calls_percentage':
                # Detail the top 10 API calls by percentage
                f.write(f"{key}:\n")
                for call, percentage in value.items():
                    rounded_percentage = round(percentage, 2)  # Round to 2 decimal places for readability
                    f.write(f"  {call}: {rounded_percentage}%\n")
            else:
                f.write(f"{key}: {value}\n")

        # Write invalid timestamp entries, if any
        f.write("\nInvalid Timestamps:\n")
        if not invalid_timestamps.empty:
            for index, row in invalid_timestamps.iterrows():
                f.write(f"Row {index}: {row['@timestamp']}, {row['fields.RequestPath']}\n")
        else:
            f.write("No invalid timestamps found.\n")
