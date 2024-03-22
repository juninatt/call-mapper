from src.dataframe_cleaner.trimmer import trim_and_extract
import pandas as pd


def apply_locations_filter(df, column_name='api_calls', new_column='locations-requests'):

    # Använd trim_and_extract för att förbereda data
    filtered_df = trim_and_extract(df, column_name, 'locations')

    # Skapa en ny DataFrame med det specificerade nya kolumnnamnet
    final_df = pd.DataFrame({new_column: filtered_df[column_name]})

    return final_df

