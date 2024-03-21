import pandas as pd
import re


def apply_locations_filter(df, column_name='api_calls', new_column='locations-requests'):
    # Copy the DataFrame to avoid SettingWithCopyWarning
    modified_df = df.copy()

    # Trim white spaces from strings in the specified column
    modified_df[column_name] = modified_df[column_name].str.strip()

    # Remove everything before '/v3' or '/v4'
    modified_df[column_name] = modified_df[column_name].apply(lambda x: re.sub(r'^.*?(/v[34])', r'\1', x))

    # Regular expression to match 'locations' directly after the version
    locations_pattern = re.compile(r'^/v[34]/locations')

    # Filter DataFrame for rows matching the pattern
    modified_df = modified_df[modified_df[column_name].str.contains(locations_pattern, regex=True)]

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: modified_df[column_name]})

    return final_df
