import os

from dotenv import load_dotenv

from src.data_processing.data_sorter import extract_matching_requests, extract_non_matching_requests, process
from src.data_processing.progress_bar import ProgressBar
from src.data_processing.report_generator import generate_csv

# Assign query string patterns
load_dotenv()


# Extracting and parsing patterns and columns from environment
def get_columns(env_var):
    return os.getenv(env_var).split(',')


# Patterns
gid_to_gid_pattern = os.getenv('ORIGIN_GID_DESTINATION_GID_PATTERN')
gid_to_coords_pattern = os.getenv('ORIGIN_GID_DESTINATION_COORDINATES_PATTERN')
coords_to_coords_pattern = os.getenv('ORIGIN_COORDINATES_DESTINATION_COORDINATES_PATTERN')
coords_to_gid_pattern = os.getenv('ORIGIN_COORDINATES_DESTINATION_GID_PATTERN')

# Columns
gid_to_gid_columns = get_columns('ORIGIN_GID_DESTINATION_GID_COLUMNS')
gid_to_coords_columns = get_columns('ORIGIN_GID_DESTINATION_COORDINATES_COLUMNS')
coords_to_coords_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_COORDINATES_COLUMNS')
coords_to_gid_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_GID_COLUMNS')


# Apply filtering and create the final DataFrame
def journeys_filter_and_generate_csv(df):
    separation_progress = ProgressBar(total=15 , title="Processing /journeys/")

    # urls: originGid, dateTimeRelatesTo, dateTime, originName

    # ORIGIN_GID

    originGid_full_df = extract_matching_requests(
        df,
        'journeys\?originGid',
        '='
    )

    originGid_destinationGid_full_df = extract_matching_requests(
        originGid_full_df,
        'originGid',
        'destinationGid'
    )
    originGid_destinationGid_processed_df = process(
        originGid_destinationGid_full_df,
        gid_to_gid_pattern,
        gid_to_gid_columns
    )
    separation_progress.update()

    originGid_allCoordinates_full_df = extract_matching_requests(
        originGid_full_df,
        'originLatitude',
        'originLongitude',
        'destinationGid'
    )
    originGid_allCoordinates_processed_df = process(
        originGid_allCoordinates_full_df,
        coords_to_coords_pattern,
        coords_to_coords_columns
    )
    separation_progress.update()

    originGid_destinationCoordinates_full_df = extract_matching_requests(
        originGid_full_df,
        'originGid',
        'destinationName',
        'originName'
    )
    originGid_destinationCoordinates_processed_df = process(
        originGid_destinationCoordinates_full_df,
        gid_to_coords_pattern,
        gid_to_coords_columns
    )
    separation_progress.update()

    # Unprocessed
    generate_csv(
        originGid_full_df,
        'journeys/originGid/unprocessed/all_originGid.csv'
    )
    generate_csv(
        originGid_destinationGid_full_df,
        '/journeys/originGid/unprocessed/originGid_destinationGid.csv'
    )
    generate_csv(
        originGid_destinationCoordinates_full_df,
        'journeys/originGid/unprocessed/originGid_destinationCoordinates.csv'
    )
    generate_csv(
        originGid_allCoordinates_full_df,
        'journeys/originGid/unprocessed/originGid_coords_to_coords.csv'
    )

    # Processed
    generate_csv(
        originGid_destinationGid_processed_df,
        '/journeys/originGid/originGid_destinationGid.csv'
    )
    generate_csv(
        originGid_allCoordinates_processed_df,
        'journeys/originGid/originGid_allCoordinates.csv'
    )
    generate_csv(
        originGid_destinationCoordinates_processed_df,
        'journeys/originGid/originGid_destinationCoordinates.csv'
    )

    #  DATE_TIME

    dateTime_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?dateTime='
    )

    dateTime_originGid_destinationGid_full_df = extract_matching_requests(
        dateTime_full_df,
        'originGid',
        'destinationGid'
    )
    dateTime_originGid_destinationGid_processed_df = process(
        dateTime_originGid_destinationGid_full_df,
        gid_to_gid_pattern,
        gid_to_gid_columns
    )
    separation_progress.update()

    dateTime_originGid_destinationCoordinates_full_df = extract_matching_requests(
        dateTime_full_df,
        'originGid',
        'destinationLatitude'
    )
    dateTime_originGid_destinationCoordinates_processed_df = process(
        dateTime_originGid_destinationCoordinates_full_df,
        gid_to_coords_pattern,
        gid_to_coords_columns
    )
    separation_progress.update()

    dateTime_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        dateTime_full_df,
        'originLatitude',
        'destinationLatitude'
    )
    dateTime_originCoordinates_destinationCoordinates_processed_df = process(
        dateTime_originCoordinates_destinationCoordinates_full_df,
        coords_to_coords_pattern,
        coords_to_coords_columns
    )
    separation_progress.update()

    dateTime_originCoordinates_destinationGid_full_df = extract_matching_requests(
        dateTime_full_df,
        'originLatitude',
        'destinationGid'
    )
    dateTime_originCoordinates_destinationGid_processed_df = process(
        dateTime_originCoordinates_destinationGid_full_df,
        coords_to_gid_pattern,
        coords_to_gid_columns
    )
    separation_progress.update()

    # Unprocessed
    generate_csv(
        dateTime_full_df,
        'journeys/dateTime/unprocessed/all_dateTime.csv'
    )
    generate_csv(
        dateTime_originGid_destinationGid_full_df,
        'journeys/dateTime/unprocessed/dateTime_originGid_destinationGid.csv'
    )
    generate_csv(
        dateTime_originGid_destinationCoordinates_full_df,
        'journeys/dateTime/unprocessed/dateTime_originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTime_originCoordinates_destinationCoordinates_full_df,
        'journeys/dateTime/unprocessed/dateTime_originCoordinates_destinationCoordinates.csv'
    )
    generate_csv(
        dateTime_originCoordinates_destinationGid_full_df,
        'journeys/dateTime/unprocessed/dateTime_originCoordinates_destinationGid.csv'
    )

    # Processed
    generate_csv(
        dateTime_originGid_destinationGid_processed_df,
        'journeys/dateTime/dateTime_originGid_destinationGid.csv'
    )
    generate_csv(
        dateTime_originGid_destinationCoordinates_processed_df,
        'journeys/dateTime/dateTime_originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTime_originCoordinates_destinationCoordinates_processed_df,
        'journeys/dateTime/dateTime_originCoordinates_destinationCoordinates.csv'
    )
    generate_csv(
        dateTime_originCoordinates_destinationGid_processed_df,
        'journeys/dateTime/dateTime_originCoordinates_destinationGid.csv'
    )

    #  DATE_TIME_RELATES_TO

    dateTimeRelatesTo_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?dateTimeRelatesTo='
    )

    dateTimeRelatesTo_originGid_destinationGid_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originGid',
        'destinationGid'
    )
    dateTimeRelatesTo_originGid_destinationGid_processed_df = process(
        dateTimeRelatesTo_originGid_destinationGid_full_df,
        gid_to_gid_pattern,
        gid_to_gid_columns
    )
    separation_progress.update()

    dateTimeRelatesTo_originGid_destinationCoordinates_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originGid',
        'destinationLatitude'
    )
    dateTimeRelatesTo_originGid_destinationCoordinates_processed_df = process(
        dateTimeRelatesTo_originGid_destinationCoordinates_full_df,
        gid_to_coords_pattern,
        gid_to_coords_columns
    )
    separation_progress.update()

    dateTimeRelatesTo_originCoordinates_destinationGid_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originLatitude',
        'destinationGid'
    )
    dateTimeRelatesTo_originCoordinates_destinationGid_processed_df = process(
        dateTimeRelatesTo_originCoordinates_destinationGid_full_df,
        coords_to_gid_pattern,
        coords_to_gid_columns
    )
    separation_progress.update()

    dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originLatitude',
        'destinationLatitude'
    )
    dateTimeRelatesTo_originCoordinates_destinationCoordinates_processed_df = process(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df,
        coords_to_coords_pattern,
        coords_to_coords_columns
    )
    separation_progress.update()

    # Unprocessed
    generate_csv(
        dateTimeRelatesTo_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/all_dateTimeRelatesTo.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationCoordinates_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/dateTimeRelatesTo_originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationGid_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/dateTimeRelatesTo_originGid_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationGid_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/dateTimeRelatesTo_originCoordinates_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/dateTimeRelatesTo_originCoordinates_destinationCoordinates.csv'
    )

    # Processed
    generate_csv(
        dateTimeRelatesTo_originGid_destinationGid_processed_df,
        '/journeys/dateTimeRelatesTo/dateTimeRelatesTo_originGid_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationCoordinates_processed_df,
        '/journeys/dateTimeRelatesTo/dateTimeRelatesTo_originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationGid_processed_df,
        '/journeys/dateTimeRelatesTo/dateTimeRelatesTo_originCoordinates_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_processed_df,
        '/journeys/dateTimeRelatesTo/dateTimeRelatesTo_originCoordinates_destinationCoordinates.csv'
    )

    # ORIGIN_NAME

    originName_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?originName='
    )

    originName_originGid_destinationGid_full_df = extract_matching_requests(
        originName_full_df,
        'originGid',
        'destinationGid',
        'originLatitude'
    )
    originName_originGid_destinationGid_processed_df = process(
        originName_originGid_destinationGid_full_df,
        gid_to_gid_pattern,
        gid_to_gid_columns
    )
    separation_progress.update()

    originName_originGid_destinationCoordinates_full_df = extract_matching_requests(
        originName_full_df,
        'originGid',
        'destinationLatitude'
    )
    originName_originGid_destinationCoordinates_processed_df = process(
        originName_originGid_destinationCoordinates_full_df,
        gid_to_coords_pattern,
        gid_to_coords_columns
    )
    separation_progress.update()

    originName_originCoordinates_destinationGid_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'originLatitude',
        'destinationLatitude'
    )
    originName_originCoordinates_destinationGid_processed_df = process(
        originName_originCoordinates_destinationGid_full_df,
        coords_to_gid_pattern,
        coords_to_gid_columns
    )
    separation_progress.update()

    originName_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'destinationLatitude',
        'originGid'
    )
    originName_originCoordinates_destinationCoordinates_processed_df = process(
        originName_originCoordinates_destinationCoordinates_full_df,
        coords_to_coords_pattern,
        coords_to_coords_columns
    )
    separation_progress.update()

    # Unprocessed
    generate_csv(
        originName_full_df,
        '/journeys/originName/unprocessed/all_originName.csv'
    )
    generate_csv(
        originName_originGid_destinationGid_full_df,
        '/journeys/originName/unprocessed/originName_originGid_destinationGid.csv'
    )
    generate_csv(
        originName_originGid_destinationCoordinates_full_df,
        '/journeys/originName/unprocessed/originName_originGid_destinationCoordinates .csv'
    )
    generate_csv(
        originName_originCoordinates_destinationGid_full_df,
        '/journeys/originName/unprocessed/originName_originCoordinates_destinationGid.csv'
    )
    generate_csv(
        originName_originCoordinates_destinationCoordinates_full_df,
        '/journeys/originName/unprocessed/originName_originCoordinates_destinationCoordinates.csv'
    )

    # Processed
    generate_csv(
        originName_originGid_destinationGid_processed_df,
        '/journeys/originName/originName_originGid_destinationGid.csv'
    )
    generate_csv(
        originName_originCoordinates_destinationCoordinates_processed_df,
        '/journeys/originName/originName_originGid_destinationCoordinates.csv'
    )
    generate_csv(
        originName_originCoordinates_destinationGid_processed_df,
        '/journeys/originName/originName_originCoordinates_destinationGid.csv'
    )
    generate_csv(
        originName_originGid_destinationCoordinates_processed_df,
        '/journeys/originName/originName_originGid_destinationCoordinates.csv'
    )

    hashes = extract_non_matching_requests(
        df,
        [originGid_full_df,
         dateTime_full_df,
         dateTimeRelatesTo_full_df,
         originName_full_df]
    )

    generate_csv(hashes, 'journeys/leftovers.csv')

    separation_progress.complete()
