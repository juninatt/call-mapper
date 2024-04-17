import re
import pandas as pd


def process(df, pattern, columns, column='all_requests'):
    sorted_df = pd.DataFrame(columns=columns)
    sorted_df[columns] = df[
        column].str.extract(pattern)

    return sorted_df


def extract_matching_requests(df, a, b, x=''):
    """
    Filters a DataFrame based on the presence of specified strings ('a' and 'b')
    and the absence of an optional string ('x') in a specified column.
    """
    column = 'all_requests'
    df_modified = strip_first_part(df, column)

    condition = (df_modified[column].str.contains(a)) & (df_modified[column].str.contains(b))
    if x.strip():
        condition = condition & (~df_modified[column].str.contains(x))
    return df_modified[condition]


def extract_non_matching_requests(original_df, matched_dfs):

    all_matched_indices = []

    for matched_df in matched_dfs:
        all_matched_indices.extend(matched_df.index)

    mask = ~original_df.index.isin(all_matched_indices)

    non_matched_df = original_df[mask]
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
