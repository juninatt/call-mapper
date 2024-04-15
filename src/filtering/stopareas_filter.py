import os

from dotenv import load_dotenv

from src.data_processing.data_sorter import extract_matching_requests, process, extract_non_matching_requests
from src.data_processing.progress_bar import ProgressBar
from src.data_processing.report_generator import generate_csv

# Load environment variables
load_dotenv()

# Retrieve patterns and columns from environment variables
stop_areas_pattern = os.getenv('STOP_AREAS_PATTERN')
stopareas_pattern = os.getenv('STOPAREAS_PATTERN')

columns = os.getenv('STOP_AREAS_COLUMNS').split(',')


def process_stopareas_calls(df):
    separation_progress = ProgressBar(total=6, title="Processing url path: stop_areas/stopareas")

    filtered_stopareas_df = extract_matching_requests(df, 'stop', 'limit')
    stopareas_hash_df = extract_non_matching_requests(df, [filtered_stopareas_df])
    separation_progress.update()

    # Departures v4 (stop-areas)
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
    separation_progress.update()

    # Departures v2 & v3 (stopareas)
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
    separation_progress.update()

    # Arrivals v4 (stop-areas)
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
    separation_progress.update()

    # Arrivals v2 & v3 (stopareas)
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
    separation_progress.update()

    # Unprocessed
    generate_csv(filtered_stopareas_df, 'stopareas/unprocessed/all_stopareas.csv')
    generate_csv(stop_areas_arrivals_full_df, 'stopareas/unprocessed/stop_areas_arrivals.csv')
    generate_csv(stopareas_arrivals_full_df, 'stopareas/unprocessed/stopareas_arrivals.csv')
    generate_csv(stopareas_departures_full_df, 'stopareas/unprocessed/stopareas_departures.csv')
    generate_csv(stop_areas_departures_full_df, 'stopareas/unprocessed/stop_areas_departures.csv')

    # Processed
    generate_csv(stop_areas_arrivals_processed_df, 'stopareas/stop_areas_arrivals.csv')
    generate_csv(stopareas_arrivals_processed_df, 'stopareas/stopareas_arrivals.csv')
    generate_csv(stopareas_departures_processed_df, 'stopareas/stopareas_departures.csv')
    generate_csv(stop_areas_departures_processed_df, 'stopareas/stop_areas_departures.csv')

    generate_csv(stopareas_hash_df, 'stopareas/unprocessed/stopareas_hashes.csv')

    separation_progress.complete()


