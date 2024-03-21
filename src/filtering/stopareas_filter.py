import pandas as pd
import re


def apply_stopareas_filter(df, column_name='api_calls', new_column='stopareas-requests'):
    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    modified_df = df.copy()

    # Regular expression to match 'stopareas' directly after the version
    stopareas_pattern = re.compile(r'/v[34]/stopareas')

    # Filter DataFrame for rows matching the pattern
    filtered_df = modified_df[modified_df[column_name].str.contains(stopareas_pattern, regex=True)]

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: filtered_df[column_name]})

    return final_df
