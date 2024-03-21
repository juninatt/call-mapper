import pandas as pd
import re


def apply_stopareas_filter(df, old_column='api_calls', new_column='stopareas-requests'):
    # Copy DataFrame to avoid SettingWithCopyWarning
    modified_df = df.copy()

    # Trim whitespace from strings in the specified column
    modified_df[old_column] = modified_df[old_column].str.strip()

    # Remove everything before '/v3' or '/v4'
    modified_df[old_column] = modified_df[old_column].apply(lambda x: re.sub(r'^.*?(/v[34])', r'\1', x))

    # Use regular expression to match 'stopareas' directly after the version
    stopareas_pattern = re.compile(r'^/v[34]/stopareas')
    modified_df = modified_df[modified_df[old_column].str.contains(stopareas_pattern, regex=True)]

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: modified_df[old_column]})

    return final_df
