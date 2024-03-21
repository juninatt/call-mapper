import os


def generate_csv(df):
    column_name = df.columns[0]

    # Directory paths
    project_path = os.path.join("data", "reports", f"{column_name}.csv")
    desktop_path = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\västtrafik_apicalls\\{column_name}.csv')

    save_file(project_path, df)  # Save inside the project
    save_file(desktop_path, df)  # Save on the Desktop


def save_file(filepath, df):
    # Ensures the directory exists and saves the DataFrame to a file.
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    unique_filepath = get_unique_filename(filepath)

    # Append the DataFrame to the file
    df.to_csv(unique_filepath, mode='a', index=False, header=True)

    print(f"CSV report has been saved to: {unique_filepath}")


def get_unique_filename(filepath):
    # Returns a unique filename by appending a counter before the extension if the file already exists.
    basename, extension = os.path.splitext(filepath)
    counter = 1

    while os.path.exists(filepath):
        filepath = f"{basename}({counter}){extension}"
        counter += 1

    return filepath
