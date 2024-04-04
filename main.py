import time

from src.data_processing.data_loader import load_data
from src.data_processing.data_sorter import extract_matching_requests, extract_non_matching_requests
from src.data_processing.report_generator import generate_overview_report

if __name__ == "__main__":
    start_time = time.time()
    print("Loading...")

    df = load_data('data/RequestPaths.csv')

    # Extract into separate DataFrames based on base url
    all_journeys_df = extract_matching_requests(df, 'all_requests', '/v[234]/journeys', '', '')
    all_locations_df = extract_matching_requests(df, 'all_requests', '/v[234]/locations', '', '')
    all_stop_areas_df = extract_matching_requests(df, 'all_requests', '/v[234]/stop', 'areas', '')
    matched_dfs = [all_journeys_df, all_locations_df, all_stop_areas_df]
    other_df = extract_non_matching_requests(df, matched_dfs)

    # Create tuple with all DataFrames and associated names
    dataframes_with_titles = [
        (df, 'OVERVIEW'),
        (all_journeys_df, 'JOURNEYS'),
        (all_locations_df, 'LOCATIONS'),
        (all_stop_areas_df, 'STOP_AREAS')
    ]
    generate_overview_report(dataframes_with_titles, 'combined_overview.txt')

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

