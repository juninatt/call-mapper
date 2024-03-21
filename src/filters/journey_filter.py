import pandas as pd
import re


# Apply filtering and create the final DataFrame
def apply_journey_filter(df, column_name='api_calls', new_column='journey_requests'):
    # First, filter DataFrame based on the query presence condition
    filtered_df = filter_on_query(df, column_name)

    # Then, strip away everything before the version part in the URL
    stripped_df = strip_and_clean_urls(filtered_df, column_name)

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: stripped_df[column_name]})

    return final_df


# Filter based on the presence of a '?' directly after 'journeys'
def filter_on_query(df, column_name='api_calls'):
    # Filter DataFrame for rows that contain a '?' directly after 'journeys'
    return df[df[column_name].str.contains('journeys\?.*', regex=True)]


def strip_and_clean_urls(df, column_name='api_calls'):
    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    modified_df = df.copy()

    # Regular expression to match everything before "/v3/" or "/v4/"
    version_pattern = re.compile(r'.*(/v[34]/.*)')

    # Regular expressions to match unwanted parameters and everything after them
    unwanted_params_patterns = [
        re.compile(r'&includeNearbyStopAreas.*?$'),
        re.compile(r'&transportModes.*?$')
    ]

    # Strip everything before "/v3/" or "/v4/", but keep the part after
    modified_df[column_name] = modified_df[column_name].apply(
        lambda x: re.sub(version_pattern, r'\1', x) if pd.notnull(x) else x)

    # Remove unwanted parameters from the URLs
    for pattern in unwanted_params_patterns:
        modified_df[column_name] = modified_df[column_name].apply(
            lambda x: re.sub(pattern, '', x) if pd.notnull(x) else x)

    return modified_df




