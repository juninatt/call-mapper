import pandas as pd
import re


def apply_locations_filter(df, column_name='api_calls', new_column='locations-requests'):
    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    modified_df = df.copy()

    # Regular expression to match 'locations' directly after the version
    locations_pattern = re.compile(r'/v[34]/locations')

    # Filter DataFrame for rows matching the pattern
    filtered_df = modified_df[modified_df[column_name].str.contains(locations_pattern, regex=True)]

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: filtered_df[column_name]})

    return final_df
