import time
from src.data_loader import load_data
from src.filters.overview_filter import generate_report
from src.journey_filter import apply_journey_filter
from src.report_generator import generate_csv

if __name__ == "__main__":
    start_time = time.time()
    print("Loading...")

    metadata = {
        "date_generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "description": "Analysis of request paths by base URL",
        "version": "1.0"
    }

    df = load_data('data/RequestPaths.csv')

    journey_calls_df = apply_journey_filter(df, 'journey_calls')

    generate_csv(journey_calls_df, metadata)
    generate_report(df, 'data/reports/overview.txt')

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

