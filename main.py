from data_loader import load_data
from data_processor import preprocess_data
from data_analyzer import analyze_data
from result_handler import save_results

if __name__ == "__main__":
    filepath = 'api_calls_collected_traffic_iso8601.csv'
    results_file = 'report.txt'

    df = load_data(filepath)
    df_valid, invalid_timestamps = preprocess_data(df)
    results = analyze_data(df_valid)
    save_results(results_file, results, invalid_timestamps)

