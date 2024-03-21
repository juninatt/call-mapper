import os


def generate_csv_report(df, metadata):
    column_name = df.columns[0]

    # Directory paths
    project_path = os.path.join("data", "reports", f"{column_name}.csv")
    desktop_path = os.path.expanduser(f'~\\OneDrive\\Skrivbord\\{column_name}.csv')

    # Convert metadata to comments
    comments = '\n'.join([f"# {key}: {value}" for key, value in metadata.items()])

    save_file(project_path, df, comments)  # Save inside the project
    save_file(desktop_path, df, comments)  # Save on the Desktop


def save_file(filepath, df, comments):
    # Ensures the directory exists and saves the DataFrame to a file with comments at the top.
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    unique_filepath = get_unique_filename(filepath)

    # Save the comments to the file
    with open(unique_filepath, 'w') as f:
        f.write(comments + '\n')

    # Append the DataFrame to the same file
    df.to_csv(unique_filepath, mode='a', index=False, header=True)

    print(f"The report has been saved to: {unique_filepath}")


def get_unique_filename(filepath):
    # Returns a unique filename by appending a counter before the extension if the file already exists.
    basename, extension = os.path.splitext(filepath)
    counter = 1

    while os.path.exists(filepath):
        filepath = f"{basename}({counter}){extension}"
        counter += 1

    return filepath
