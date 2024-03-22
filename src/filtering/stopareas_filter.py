from src.dataframe_cleaner.trimmer import trim_and_extract
import pandas as pd


def apply_stopareas_filter(df, old_column='api_calls', new_column='stopareas-requests'):
    # Använd trim_and_extract för att förbereda data baserat på 'stopareas'
    filtered_df = trim_and_extract(df, old_column, 'stopareas')

    # Skapa en ny DataFrame med det specificerade nya kolumnnamnet
    final_df = pd.DataFrame({new_column: filtered_df[old_column]})

    return final_df
