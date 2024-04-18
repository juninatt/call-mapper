import os

from dotenv import load_dotenv

from src.data_processing.data_sorter import extract_matching_requests, process, extract_non_matching_requests
from src.data_processing.progress_bar import ProgressBar

load_dotenv()

# Retrieve patterns and columns from environment variables
stop_areas_pattern = os.getenv('STOP_AREAS_PATTERN')
stopareas_pattern = os.getenv('STOPAREAS_PATTERN')
columns = os.getenv('STOP_AREAS_COLUMNS').split(',')


def process_stopareas(df):
    separation_progress = ProgressBar(
        total=6,
        title="Processing url path: stop_areas/stopareas"
    )

    filtered_stopareas_df = extract_matching_requests(
        df,
        'stop',
        'limit'
    )

    stopareas_hash_df = extract_non_matching_requests(
        df,
        [filtered_stopareas_df]
    )

    stop_areas_departures_full_df = extract_matching_requests(
        filtered_stopareas_df,
        'stop-areas',
        'departures'
    )

    stop_areas_departures_processed_df = process(
        stop_areas_departures_full_df,
        stop_areas_pattern,
        columns
    )

    stopareas_departures_full_df = extract_matching_requests(
        filtered_stopareas_df,
        'stopareas',
        'departures'
    )

    stopareas_departures_processed_df = process(
        stopareas_departures_full_df,
        stopareas_pattern,
        columns
    )

    stop_areas_arrivals_full_df = extract_matching_requests(
        filtered_stopareas_df,
        'stop-areas',
        'arrivals'
    )

    stop_areas_arrivals_processed_df = process(
        stop_areas_arrivals_full_df,
        stop_areas_pattern,
        columns
    )

    stopareas_arrivals_full_df = extract_matching_requests(
        filtered_stopareas_df,
        'stopareas',
        'arrivals'
    )

    stopareas_arrivals_processed_df = process(
        stopareas_arrivals_full_df,
        stopareas_pattern,
        columns
    )

    separation_progress.complete()

    return {
        "/stopareas/unprocessed/": [
            (filtered_stopareas_df, 'all_stopareas_unprocessed.csv'),
            (stop_areas_departures_full_df, 'stop_areas_departures_unprocessed.csv'),
            (stopareas_departures_full_df, 'stopareas_departures_unprocessed.csv'),
            (stop_areas_arrivals_full_df, 'stop_areas_arrivals_unprocessed.csv'),
            (stopareas_arrivals_full_df, 'stopareas_arrivals_unprocessed.csv')
        ],
        "/stopareas/processed/": [
            (stop_areas_departures_processed_df, 'stop_areas_departures_processed.csv'),
            (stopareas_departures_processed_df, 'stopareas_departures_processed.csv'),
            (stop_areas_arrivals_processed_df, 'stop_areas_arrivals_processed.csv'),
            (stopareas_arrivals_processed_df, 'stopareas_arrivals_processed.csv')
        ],
        "/stopareas/": [
            (stopareas_hash_df, 'stopareas_hashes.csv')
        ]
    }


