import pandas as pd


def apply_journey_filter(df, column_name):
    # Specify the column to search through
    filter_column = 'fields.RequestPath'

    # Filter the DataFrame to only include rows where 'filter_column' contains "journeys"
    filtered_rows = df[df[filter_column].str.contains("journeys", na=False)]

    # Create a new DataFrame containing the matches, setting the column name as specified
    new_df = pd.DataFrame({column_name: filtered_rows[filter_column]})

    return new_df



