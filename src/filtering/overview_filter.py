import os


def generate_overview(df):
    report_path = 'data/reports/overview.txt'
    # Generate a report combining category frequencies and common 'journeys' paths, then save to report_path.
    categorized_df = categorize_urls(df)
    category_frequencies = calculate_category_frequencies(categorized_df)
    common_journeys = extract_common_journeys(categorized_df)

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w') as f:
        f.write("Category Frequencies (%):\n")
        for category, percent in category_frequencies.items():
            f.write(f"{category}: {percent}%\n")
        f.write("\nTop 10 Common Paths Following 'journeys/' and Their Frequencies (%):\n")
        for path, percent in common_journeys.items():
            f.write(f"{path}: {percent}%\n")

    print(f"Overview document saved to: {report_path}")


def categorize_urls(df):
    # Categorize each URL by base URLs ('journeys', 'locations', 'stopareas') or 'other'.
    base_urls = ['journeys', 'locations', 'stopareas']

    def extract_category(url):
        # Return base_url if found in url, otherwise return 'other'.
        for base_url in base_urls:
            if base_url in url:
                return base_url
        return 'other'
    df['category'] = df['api_calls'].apply(extract_category)
    return df


def calculate_category_frequencies(df):
    # Calculate the frequency of each category in percentage.
    total_count = len(df)  # Not used in this function, could be removed or used if needed.
    category_counts = df['category'].value_counts(normalize=True).mul(100).round(2)
    return category_counts


def extract_common_journeys(df, top_n=10):
    # Extract the top N most common paths following 'journeys/' and calculate their frequencies.
    journeys_df = df[df['api_calls'].str.contains('journeys/', na=False)]
    paths = journeys_df['api_calls'].str.split('/').apply(lambda x: x[2] if len(x) > 2 else 'other')
    path_counts = paths.value_counts(normalize=True).head(top_n).mul(100).round(2)
    return path_counts


