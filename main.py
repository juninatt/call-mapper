import time
from src.data_processing.data_loader import load_data
from src.filtering.overview_filter import generate_report
from src.filtering.journeys_filter import apply_journey_filter
from src.data_processing.report_generator import generate_csv

if __name__ == "__main__":
    start_time = time.time()
    print("Loading...")

    df = load_data('data/RequestPaths.csv')

    journey_calls_df = apply_journey_filter(df)

    generate_csv(journey_calls_df)
    generate_report(df, 'data/reports/overview.txt')

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

