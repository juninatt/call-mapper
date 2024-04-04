import sys


def progress_bar(progress, total, title="Processing"):
    """Displays a progress bar with a title."""
    percent = 100 * (progress / float(total))
    bar = '█' * int(percent) + '-' * (100 - int(percent))
    sys.stdout.write(f"\r{title}: |{bar}| {percent:.2f}% Completed")
    sys.stdout.flush()
