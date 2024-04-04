import os
import pandas as pd

from src.calculation.statistics_calculator import calculate_combined_statistics


def generate_csv(df, file_name, create_new_file=False):
    """
    Saves a DataFrame as a CSV to a predetermined desktop path. If `create_new_file` is set,
    generates a new filename if the specified one exists; otherwise, overwrites the existing file.

    Parameters:
    - df (pandas.DataFrame): DataFrame to save.
    - file_name (str): Desired file name.
    - create_new_file (bool): Whether to avoid overwriting existing files (default False).
    """
    local_disk = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\västtrafik_apicalls\\{file_name}')
    os.makedirs(os.path.dirname(local_disk), exist_ok=True)
    if create_new_file:
        local_disk = get_unique_filename(local_disk)
    df.to_csv(local_disk, mode='a', index=False, header=True)
    print(f"'{file_name}' report saved to: {local_disk}")


def save_file(filepath, df, file_name, create_new_file=True):
    """
    Ensures the directory exists and saves the DataFrame to a file.
    If create_new_file is True, a new file is created if one already exists.
    Otherwise, the file will be overwritten.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if create_new_file:
        filepath = get_unique_filename(filepath)
    df.to_csv(filepath, mode='a', index=False, header=True)
    print(f"'{file_name}' report saved to: {filepath}")


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


def generate_combined_report(dataframes_with_titles, file_name):
    """
    Generates a combined report from a list of tuples (DataFrame, Title) and saves it to a specified file.

    Parameters:
    - dataframes_with_titles (list of tuple): A list where each tuple contains a DataFrame and its title.
    - file_name (str): The name of the file to save the report to.
    """
    combined_report_str = ""
    for df, title in dataframes_with_titles:
        version_counts, (param_percent, param_counts) = calculate_combined_statistics(df)
        report_section = generate_report_section(title, version_counts, param_percent, param_counts)
        combined_report_str += report_section

    desktop_path = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\västtrafik_apicalls\\{file_name}')
    os.makedirs(os.path.dirname(desktop_path), exist_ok=True)

    # Write the combined report string to file
    with open(desktop_path, 'w') as file:
        file.write(combined_report_str)

    print(f"Combined report saved to: {desktop_path}")


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
