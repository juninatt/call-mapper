import pandas as pd
import re


def trim_and_extract(df, column_name, keyword):
    """
    Trims whitespace from the ends of strings, removes characters before '/v3' or '/v4',
    and extracts rows containing a specific keyword in the URL in the specified column.

    Parameters:
    - df: Pandas DataFrame to process.
    - column_name: The name of the column to process.
    - keyword: The keyword to filter URLs by (e.g., "locations", "journeys", "stopareas").

    Returns:
    - A DataFrame with processed and filtered data.
    """
    modified_df = df.copy()

    if column_name in modified_df:
        # Trim whitespace and remove everything before '/v3' or '/v4'
        modified_df[column_name] = modified_df[column_name].str.strip().apply(
            lambda x: re.sub(r'^.*?(/v[34])', r'\1', x))

        # Regular expression to match the keyword directly after the version
        pattern = re.compile(rf'^/v[34]/{keyword}')

        # Filter DataFrame for rows matching the pattern
        modified_df = modified_df[modified_df[column_name].str.contains(pattern, regex=True)]

    return modified_df
