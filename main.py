import time
from src.data_loader import load_data
from src.journey_filter import apply_journey_filter
from src.report_generator import generate_csv_report

if __name__ == "__main__":
    start_time = time.time()

    filepath = 'data/RequestPaths.csv'
    metadata = {
        "date_generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "description": "Analysis of request paths by base URL",
        "version": "1.0"
    }

    df = load_data(filepath)

    journey_calls_df = apply_journey_filter(df, 'journey_calls')

    generate_csv_report(journey_calls_df, metadata)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

