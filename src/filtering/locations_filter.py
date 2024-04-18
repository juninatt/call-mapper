import os

from dotenv import load_dotenv

from src.data_processing.data_sorter import extract_matching_requests, process
from src.data_processing.progress_bar import ProgressBar

load_dotenv()  # Load environment variables


def get_columns(column_var):
    return os.getenv(column_var).split(',')


# v4
by_text_pattern = os.getenv('LOCATIONS_BY_TEXT_PATTERN')
by_text_columns = get_columns('LOCATIONS_BY_TEXT_COLUMNS')
by_coordinates_pattern = os.getenv('LOCATIONS_BY_COORDINATES_PATTERN')
by_coordinates_columns = get_columns('LOCATIONS_BY_COORDINATES_COLUMNS')

# v2 & v3
bytext_pattern = os.getenv('LOCATIONS_BYTEXT_PATTERN')
bytext_columns = get_columns('LOCATIONS_BYTEXT_COLUMNS')
bycoordinates_pattern = os.getenv('LOCATIONS_BYCOORDINATES_PATTERN')
bycoordinates_columns = get_columns('LOCATIONS_BYCOORDINATES_COLUMNS')


def process_locations(df):
    separation_progress = ProgressBar(total=6, title="Processing url path: locations")

    # Text v4 (by-text)
    locations_by_text_full_df = extract_matching_requests(
        df,
        'locations',
        'by-text'
    )
    locations_by_text_processed_df = process(
        locations_by_text_full_df,
        by_text_pattern,
        by_text_columns
    )
    separation_progress.update()

    # Text v2 & v3 (bytext)
    locations_bytext_full_df = extract_matching_requests(
        df,
        'locations',
        'bytext'
    )
    locations_bytext_processed_df = process(
        locations_bytext_full_df,
        bytext_pattern,
        bytext_columns
    )
    separation_progress.update()

    # Coordinates v4 (by-coordinates)
    locations_by_coordinates_full_df = extract_matching_requests(
        df,
        'locations',
        'by-coordinates'
    )
    locations_by_coordinates_processed_df = process(
        locations_by_coordinates_full_df,
        by_coordinates_pattern,
        by_coordinates_columns
    )
    separation_progress.update()

    # Coordinates v2 & v3 (bycoordinates)
    locations_bycoordinates_full_df = extract_matching_requests(
        df,
        'locations',
        'bycoordinates'
    )
    locations_bycoordinates_processed_df = process(
        locations_bycoordinates_full_df,
        bycoordinates_pattern,
        bycoordinates_columns
    )
    separation_progress.complete()

    return {
        "/locations/unprocessed/": [
            (df, 'all_locations_unprocessed.csv'),
            (locations_by_text_full_df, 'locations_by_text_unprocessed.csv'),
            (locations_bytext_full_df, 'locations_bytext_unprocessed.csv'),
            (locations_by_coordinates_full_df, 'locations_by_coordinates_unprocessed.csv'),
            (locations_bycoordinates_full_df, 'locations_bycoordinates_unprocessed.csv')
        ],
        "/locations/processed/": [
            (locations_by_text_processed_df, 'locations_by_text_processed.csv'),
            (locations_bytext_processed_df, 'locations_bytext_processed.csv'),
            (locations_by_coordinates_processed_df, 'locations_by_coordinates_processed.csv'),
            (locations_bycoordinates_processed_df, 'locations_bycoordinates_processed.csv'),
        ]
    }
