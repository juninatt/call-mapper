class RowCounter:
    def __init__(self):
        """Initialize an empty dictionary to hold session data counts."""
        self.counts = {}

    def start_session(self, name):
        """
        Start a new counting session if it does not already exist.

        Args:
            name (str): The name of the session to start.
        """
        if name not in self.counts:
            self.counts[name] = {}

    def update_count(self, name, path, df):
        """
        Update the row count for a given data frame if the path is not marked 'unprocessed'.

        Args:
            name (str): The session name under which the count is updated.
            path (str): The path or category for the data frame.
            df (DataFrame): The data frame from which to count rows.
        """
        if 'unprocessed' not in path:
            if path not in self.counts[name]:
                self.counts[name][path] = 0
            self.counts[name][path] += len(df)

    def total_count(self, name):
        """
        Calculate the total count of rows for a given session.

        Args:
            name (str): The session name for which the total is calculated.

        Returns:
            int: Total number of rows counted in the session.
        """
        return sum(self.counts[name].values())

    def process_data(self, name, data):
        """
        Counts the number of rows in the input data files not in 'unprocessed'-folders
        by updating counts and then displaying results.

        Args:
            name (str): The session name to process.
            data (dict): A dictionary of data lists keyed by path, each containing tuples of data frames and their paths.
        """
        self.start_session(name)
        for path, data_list in data.items():
            for df, _ in data_list:
                self.update_count(name, path, df)
        self.display_results(name)

    def display_results(self, name):
        print(f"Total number of rows for '{name}': {self.total_count(name)}")

