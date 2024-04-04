import pandas as pd
import re

from src.data_processing.progress_bar import progress_bar


def print_in_green(text):
    """Prints text in green in the console."""
    print("\033[92m" + text + "\033[0m", end='')


def process_api_calls(df_api, column='api_calls'):
    """
    Extracts and organizes values from a DataFrame of API calls into a new DataFrame
    with specified columns based on regex pattern matching.
    """
    columns = [
        'version', 'originGid', 'destinationGid',
        'originLatitude', 'destinationLatitude',
        'originLongitude', 'destinationLongitude'
    ]
    temp_dicts = []

    patterns = {
        'version': r'/(v\d+)/',
        'originGid': r'originGid=(\d+)',
        'destinationGid': r'destinationGid=(\d+)',
        'originLatitude': r'originLatitude=([-+]?\d*\.\d+|\d+)',
        'destinationLatitude': r'destinationLatitude=([-+]?\d*\.\d+|\d+)',
        'originLongitude': r'originLongitude=([-+]?\d*\.\d+|\d+)',
        'destinationLongitude': r'destinationLongitude=([-+]?\d*\.\d+|\d+)'
    }

    total_rows = len(df_api)
    for index, row in enumerate(df_api.itertuples(), start=1):
        api_call = getattr(row, column)
        temp_dict = {col: (re.search(pattern, api_call).group(1) if re.search(pattern, api_call) else 'x') for
                     col, pattern in patterns.items()}
        temp_dicts.append(temp_dict)
        progress_bar(index, total_rows)

    df_result = pd.DataFrame(temp_dicts)
    print()
    return df_result


def extract_matching_requests(df, column, a, b, x=''):
    """
    Filters a DataFrame based on the presence of specified strings ('a' and 'b')
    and the absence of an optional string ('x') in a specified column.
    """
    df_modified = strip_first_part(df, column)

    condition = (df_modified[column].str.contains(a)) & (df_modified[column].str.contains(b))
    if x.strip():
        condition = condition & (~df_modified[column].str.contains(x))
    return df_modified[condition]


def extract_non_matching_requests(original_df, matched_dfs):
    """
    Creates a DataFrame consisting of rows from the original DataFrame
    that do not match any row in the list of matched DataFrames.
    """
    non_matched_df = original_df.copy()
    for matched_df in matched_dfs:
        non_matched_df = non_matched_df.drop(matched_df.index)
    return non_matched_df


def strip_first_part(df, column):
    """
    Modifies each value in a specified column by removing everything up to
    a specific pattern, retaining only the part after it.
    """
    modified_df = df.copy()
    if column in modified_df.columns:
        modified_df[column] = modified_df[column].apply(
            lambda x: re.sub(r'^.*?(/v[34])', r'\1', x) if x else x
        )
    return modified_df
