import os

from src.calculation.statistics_calculator import calculate_combined_statistics
from src.data_processing.progress_bar import ProgressBar


def generate_csv(df, file_name):
    """
    Saves a DataFrame as a CSV to a predetermined desktop path. If `create_new_file` is set,
    generates a new filename if the specified one exists; otherwise, overwrites the existing file.

    Parameters:
    - df (pandas.DataFrame): DataFrame to save.
    - file_name (str): Desired file name.
    - create_new_file (bool): Whether to avoid overwriting existing files (default False).
    """
    # Generate header with info regarding the content of the file to be generated
    number_of_rows = str(len(df))
    trimmed_file_name = file_name.rstrip('.csv')
    details_title = trimmed_file_name + " - " + number_of_rows + " api calls"

    local_disk = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\västtrafik_apicalls\\{file_name}')
    os.makedirs(os.path.dirname(local_disk), exist_ok=True)

    with open(local_disk, 'w', newline='', encoding='utf-8') as file:
        file.write(details_title + "\n")

    df.to_csv(local_disk, mode='a', index=False, header=True)


def get_unique_filename(filepath):
    """
    Returns a unique filename by appending a counter before the extension if the file already exists.
    """
    basename, extension = os.path.splitext(filepath)
    counter = 1
    while os.path.exists(filepath):
        filepath = f"{basename}({counter}){extension}"
        counter += 1
    return filepath


def generate_overview_report(dataframes_with_titles, file_name):
    """
    Generates a combined report from a list of tuples (DataFrame, Title) and saves it to a specified file.
    Now includes a progress bar to display processing progress.
    """
    combined_report_str = ""
    total_reports = len(dataframes_with_titles)

    # Initialize the ProgressBar object with the total number of reports
    progress = ProgressBar(total=total_reports, title="Generating Combined Report")

    for index, (df, title) in enumerate(dataframes_with_titles, start=1):
        version_counts, (param_percent, param_counts) = calculate_combined_statistics(df)
        report_section = generate_report_section(title, version_counts, param_percent, param_counts)
        combined_report_str += report_section

        progress.update()

    desktop_path = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\västtrafik_apicalls\\{file_name}')
    os.makedirs(os.path.dirname(desktop_path), exist_ok=True)

    with open(desktop_path, 'w') as file:
        file.write(combined_report_str)

    progress.complete()


def generate_report_section(title, version_counts, param_percent, param_counts):
    """
    Generates a text-based section for the report with given statistics.

    Parameters:
    - title (str): Title of the report section.
    - version_counts (dict): Distribution of versions.
    - param_percent (dict): Percentage of query parameters.
    - param_counts (dict): Counts of query parameters.

    Returns:
    - str: A formatted text section for the report.
    """
    report_section = f"Report Title: {title}\n\n"
    report_section += "Version Distribution:\n"
    for version, count in version_counts.items():
        report_section += f"- {version}: {count}%\n"

    report_section += "\nQuery Parameters Percentage:\n"
    for param, percent in param_percent.items():
        report_section += f"- {param}: {percent}%\n"

    report_section += "\nQuery Parameters Counts:\n"
    for param, count in param_counts.items():
        report_section += f"- {param}: {count}\n"

    report_section += "\n" + "-" * 40 + "\n\n"
    return report_section
