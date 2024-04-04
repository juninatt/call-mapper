import sys


class ProgressBar:
    """
    A class to display a progress bar in the console.

    Attributes:
        total (int): The total number of steps the progress bar should represent.
        title (str): The title of the progress bar. Defaults to "Progress".
        current (int): The current progress.

    Methods:
        update(): Increments the progress by one step and updates the progress bar display.
        complete(): Completes the progress bar by filling it to 100% and prints a new line.
    """

    def __init__(self, total, title="Progress"):
        """
        Constructs all the necessary attributes for the progress bar object.

        Parameters:
            total (int): The total number of steps the progress bar should represent.
            title (str): The title of the progress bar. Defaults to "Progress".
        """
        self.total = total
        self.title = title
        self.current = 0
        self.update(0)  # Show initial progress bar upon creation

    def update(self, step_increment=1):
        """
        Increments the progress by a specified step and updates the progress bar display.

        Parameters:
            step_increment (int): The number of steps to increment the progress by. Defaults to 1.
        """
        self.current += step_increment
        percent = 100 * (self.current / float(self.total))
        bar = '█' * int(percent / 2) + '-' * (50 - int(percent / 2))
        sys.stdout.write(f"\r{self.title}: |{bar}| {percent:.2f}% Completed")
        sys.stdout.flush()

    def complete(self):
        """
        Completes the progress bar by filling it to 100% if not already there, and prints a new line.
        """
        self.update(self.total - self.current)  # Complete to 100% if not already done
        print()  # New line for future prints

