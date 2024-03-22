import os


def generate_overview(df):
    report_path = 'data/reports/overview.txt'
    # Generate a report combining category frequencies, common 'journeys' paths, and common query parameters.
    categorized_df = categorize_urls(df)
    category_frequencies = calculate_category_frequencies(categorized_df)
    common_journeys = extract_common_journeys(categorized_df)
    common_query_parameters = generate_common_query_sections(categorized_df)

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w') as f:
        f.write("Category Frequencies (%):\n")
        for category, percent in category_frequencies.items():
            f.write(f"{category}: {percent}%\n")
        f.write("\nTop 10 Common Paths Following 'journeys/' and Their Frequencies (%):\n")
        for path, percent in common_journeys.items():
            f.write(f"{path}: {percent}%\n")
        f.write(common_query_parameters)
    print(f"Overview document saved to: {report_path}")


def categorize_urls(df):
    base_urls = ['journeys', 'locations', 'stopareas']

    def extract_category(url):
        for base_url in base_urls:
            if base_url in url:
                return base_url
        return 'other'
    df['category'] = df['api_calls'].apply(extract_category)
    return df


def calculate_category_frequencies(df):
    category_counts = df['category'].value_counts(normalize=True).mul(100).round(2)
    return category_counts


def extract_common_journeys(df, top_n=10):
    journeys_df = df[df['api_calls'].str.contains('journeys/', na=False)]
    paths = journeys_df['api_calls'].str.split('/').apply(lambda x: x[2] if len(x) > 2 else 'other')
    path_counts = paths.value_counts(normalize=True).head(top_n).mul(100).round(2)
    return path_counts


def extract_query_values(df, base_url):
    filtered_df = df[df['api_calls'].str.contains(f'{base_url}/', na=False)]
    query_values = filtered_df['api_calls'].str.extract(f'{base_url}/.*\?(.*)')[0]
    query_values = query_values.str.split('&').explode().str.split('=').str[0]
    value_counts = query_values.value_counts(normalize=True).head(10).mul(100).round(2)
    return value_counts


def generate_common_query_sections(df):
    sections = ""
    for base_url in ['journeys', 'locations', 'stopareas']:
        common_values = extract_query_values(df, base_url)
        sections += f"\nTop 10 Common Query Parameters Following '{base_url}/' and Their Frequencies (%):\n"
        for value, percent in common_values.items():
            sections += f"{value}: {percent}%\n"
    return sections


