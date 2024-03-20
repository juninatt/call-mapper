def generate_csv_report(df, metadata):
    # Check that the DataFrame only has one column
    if len(df.columns) != 1:
        raise ValueError("DataFrame should only contain one column.")

    column_name = df.columns[0]
    filepath = f"data/reports/{column_name}.csv"

    # Construct comments from metadata
    comments = '\n'.join([f"# {key}: {value}" for key, value in metadata.items()])

    # Create a new CSV file and write the comments
    with open(filepath, 'w') as f:
        f.write(comments + '\n')

    # Append DataFrame data below the comments in the CSV file
    df.to_csv(filepath, mode='a', index=False, header=True)

    print(f"The report has been saved to: {filepath}")
