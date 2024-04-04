import pandas as pd
from urllib.parse import urlparse, parse_qs


def calculate_combined_statistics(df):
    """
    Calculates and returns statistics for API call versions, the most common query parameters,
    and their percentages from a given DataFrame.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the API requests.

    Returns:
    - tuple: Contains dictionaries for version distribution and query parameters (both percentages and counts).
    """
    # Extract version from URL and count frequency
    df['version'] = df['all_requests'].apply(lambda x: urlparse(x).path).str.extract(r'(/v[2-4])')
    version_counts = df['version'].value_counts(normalize=True).mul(100).round(2).to_dict()

    # Work with query parameters
    query_params = df['all_requests'].apply(lambda x: parse_qs(urlparse(x).query))
    param_list = [param for sublist in query_params for param in sublist]
    param_stats_percent = pd.Series(param_list).value_counts(normalize=True).mul(100).round(2).to_dict()
    param_stats_counts = pd.Series(param_list).value_counts().to_dict()

    return version_counts, (param_stats_percent, param_stats_counts)

