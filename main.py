import time
from src.data_processing.data_loader import load_data
from src.filtering.locations_filter import apply_locations_filter
from src.filtering.overview_filter import generate_overview
from src.filtering.journeys_filter import apply_journey_filter
from src.data_processing.report_generator import generate_csv
from src.filtering.stopareas_filter import apply_stopareas_filter

if __name__ == "__main__":
    start_time = time.time()
    print("Loading...")

    df = load_data('data/RequestPaths.csv')

    journey_requests_df = apply_journey_filter(df)
    locations_requests_df = apply_locations_filter(df)
    stopareas_requests_df = apply_stopareas_filter(df)

    generate_csv(journey_requests_df)
    generate_csv(locations_requests_df)
    generate_csv(stopareas_requests_df)
    generate_overview(df)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

