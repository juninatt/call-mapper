import time

from src.data_processing.data_loader import load_data
from src.data_processing.data_sorter import extract_matching_requests, extract_non_matching_requests
from src.data_processing.report_generator import generate_overview_report, generate_csv
from src.data_processing.progress_bar import ProgressBar
from src.filtering.journeys_filter import journeys_filter_and_generate_csv

if __name__ == "__main__":
    start_time = time.time()
    print("Starting")
    df = load_data('data/RequestPaths.csv')
    print("Data Loaded")

    # Extract into separate DataFrames based on base url
    separation_progress = ProgressBar(total=3, title="Sorting API calls")
    all_journeys_df = extract_matching_requests(df, 'all_requests', '/v[234]/journeys', '', '')
    separation_progress.update()
    all_locations_df = extract_matching_requests(df, 'all_requests', '/v[234]/locations', '', '')
    separation_progress.update()
    all_stop_areas_df = extract_matching_requests(df, 'all_requests', '/v[234]/stop', 'areas', '')
    separation_progress.update()
    matched_dfs = [all_journeys_df, all_locations_df, all_stop_areas_df]
    other_df = extract_non_matching_requests(df, matched_dfs)
    separation_progress.complete()

    # Create tuple with all DataFrames and associated names and generate overview report
    dataframes_with_titles = [
        (df, 'OVERVIEW'),
        (all_journeys_df, 'JOURNEYS'),
        (all_locations_df, 'LOCATIONS'),
        (all_stop_areas_df, 'STOP_AREAS')
    ]
    generate_overview_report(dataframes_with_titles, 'combined_overview.txt')

    # Generate CSV files
    csv_progress = ProgressBar(4, 'Generating CSV files')
    generate_csv(all_journeys_df, '/journeys/all_journeys.csv')
    csv_progress.update()
    generate_csv(all_locations_df, '/locations/all_locations.csv')
    csv_progress.update()
    generate_csv(all_stop_areas_df, '/stop_areas/all_stop_areas.csv')
    csv_progress.update()
    generate_csv(other_df, 'other/other.csv')
    csv_progress.complete()

    # Generate journeys- CSV file
    journeys_filter_and_generate_csv(all_journeys_df)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

