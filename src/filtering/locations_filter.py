from src.dataframe_cleaner.trimmer import trim_and_extract
import pandas as pd


def apply_locations_filter(df, column_name='api_calls', new_column='locations-requests'):

    # Use trim_and_extract to prepare the data
    filtered_df = trim_and_extract(df, column_name, 'locations')

    # Create a new DataFrame with the specified new column name
    final_df = pd.DataFrame({new_column: filtered_df[column_name]})

    return final_df


