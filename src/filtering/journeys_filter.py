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
originGid_and_coords_to_coords_pattern = os.getenv('ORIGIN_GID_ALL_COORDINATES_PATTERN')

# Columns
gid_to_gid_columns = get_columns('ORIGIN_GID_DESTINATION_GID_COLUMNS')
gid_to_coords_columns = get_columns('ORIGIN_GID_DESTINATION_COORDINATES_COLUMNS')
coords_to_coords_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_COORDINATES_COLUMNS')
coords_to_gid_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_GID_COLUMNS')
originGid_and_coords_to_coords_columns = get_columns('ORIGIN_GID_ALL_COORDINATES_COLUMNS')


# Apply filtering and create the final DataFrame
def process_journeys(df):
    separation_progress = ProgressBar(total=15, title="Processing url path: journeys")

    # urls: originGid, dateTime, dateTimeRelatesTo, originName

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
        originGid_and_coords_to_coords_pattern,
        originGid_and_coords_to_coords_columns
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

    #  DATE_TIME

    dateTime_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?dateTime='
    )

    dateTime_gid_to_gid_full_df = extract_matching_requests(
        dateTime_full_df,
        'originGid',
        'destinationGid'
    )
    dateTime_gid_to_gid_processed_df = process(
        dateTime_gid_to_gid_full_df,
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

    #  DATE_TIME_RELATES_TO

    dateTimeRelatesTo_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?dateTimeRelatesTo='
    )

    dateTimeRelatesTo_gid_to_gid_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originGid',
        'destinationGid'
    )
    dateTimeRelatesTo_gid_to_gid_processed_df = process(
        dateTimeRelatesTo_gid_to_gid_full_df,
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


    # ORIGIN_NAME

    originName_full_df = extract_matching_requests(
        df,
        'v[234]',
        'journeys\?originName='
    )

    originName_gid_to_gid_full_df = extract_matching_requests(
        originName_full_df,
        'originGid',
        'destinationGid',
        'originLatitude'
    )
    originName_gid_to_gid_processed_df = process(
        originName_gid_to_gid_full_df,
        gid_to_gid_pattern,
        gid_to_gid_columns
    )
    separation_progress.update()

    originName_gid_to_coords_full_df = extract_matching_requests(
        originName_full_df,
        'originGid',
        'destinationLatitude'
    )
    originName_gid_to_coords_processed_df = process(
        originName_gid_to_coords_full_df,
        gid_to_coords_pattern,
        gid_to_coords_columns
    )
    separation_progress.update()

    originName_coords_to_gid_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'originLatitude',
        'destinationLatitude'
    )
    originName_coords_to_gid_processed_df = process(
        originName_coords_to_gid_full_df,
        coords_to_gid_pattern,
        coords_to_gid_columns
    )
    separation_progress.update()

    originName_coords_to_coords_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'destinationLatitude',
        'originGid'
    )
    originName_coords_to_coords_processed_df = process(
        originName_coords_to_coords_full_df,
        coords_to_coords_pattern,
        coords_to_coords_columns
    )
    separation_progress.update()

    hashes = extract_non_matching_requests(
        df,
        [originGid_full_df,
         dateTime_full_df,
         dateTimeRelatesTo_full_df,
         originName_full_df]
    )

    generate_csv(hashes, 'journeys/leftovers.csv')

    separation_progress.complete()

    return {
        "/journeys/originGid/unprocessed/": [
            (originGid_full_df, 'all_originGid_unprocessed.csv'),
            (originGid_destinationGid_full_df, 'originGid_destinationGid.csv'),
            (originGid_allCoordinates_full_df, 'originGid_coords_to_coords.csv'),
            (originGid_destinationCoordinates_full_df, 'originGid_destinationCoordinates.csv')
        ],
        "/journeys/originGid/processed/": [
            (originGid_destinationGid_processed_df, '/originGid_destinationGid.csv'),
            (originGid_allCoordinates_processed_df, 'originGid_allCoordinates.csv'),
            (originGid_destinationCoordinates_processed_df, '/originGid_destinationCoordinates.csv'),
        ],
        "/journeys/dateTime/unprocessed/": [
            (dateTime_full_df, 'all_dateTime.csv'),
            (dateTime_gid_to_gid_full_df, 'dateTime_gid_to_gid_full.csv'),
            (dateTime_originGid_destinationCoordinates_full_df, 'dateTime_originGid_destinationCoordinates.csv'),
            (dateTime_originCoordinates_destinationCoordinates_full_df, 'dateTime_originCoordinates_destinationCoordinates.csv'),
            (dateTime_originCoordinates_destinationGid_full_df, 'dateTime_originCoordinates_destinationGid.csv')
        ],
        "/journeys/dateTime/processed/": [
            (dateTime_gid_to_gid_processed_df, 'dateTime_gid_to_gid.csv'),
            (dateTime_originGid_destinationCoordinates_processed_df, 'dateTime_originGid_destinationCoordinates.csv'),
            (dateTime_originCoordinates_destinationCoordinates_processed_df, 'dateTime_originCoordinates_destinationCoordinates.csv'),
            (dateTime_originCoordinates_destinationGid_processed_df, 'dateTime_originCoordinates_destinationGid.csv'),
        ],
        "/journeys/dateTimeRelatesTo/unprocessed/": [
            (dateTimeRelatesTo_full_df, 'all_dateTimeRelatesTo.csv'),
            (dateTimeRelatesTo_originGid_destinationCoordinates_full_df, 'dateTimeRelatesTo_originGid_destinationCoordinates.csv'),
            (dateTimeRelatesTo_gid_to_gid_full_df, 'dateTimeRelatesTo_gid_to_gid.csv'),
            (dateTimeRelatesTo_originCoordinates_destinationGid_full_df, 'dateTimeRelatesTo_originCoordinates_destinationGid.csv'),
            (dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df, 'dateTimeRelatesTo_originCoordinates_destinationCoordinates.csv')
        ],

        "/journeys/dateTimeRelatesTo/processed/": [
            (dateTimeRelatesTo_gid_to_gid_processed_df, 'dateTimeRelatesTo_gid_to_gid.csv'),
            (dateTimeRelatesTo_originGid_destinationCoordinates_processed_df, 'dateTimeRelatesTo_originGid_destinationCoordinates.csv'),
            (dateTimeRelatesTo_originCoordinates_destinationGid_processed_df, 'dateTimeRelatesTo_originCoordinates_destinationGid.csv'),
            (dateTimeRelatesTo_originCoordinates_destinationCoordinates_processed_df, 'dateTimeRelatesTo_originCoordinates_destinationCoordinates.csv'),
        ],
        "/journeys/originName/unprocessed/": [
            (originName_full_df, 'all_originName.csv'),
            (originName_gid_to_gid_full_df, 'originName_gid_to_gid_full.csv'),
            (originName_gid_to_coords_full_df, 'originName_gid_to_coords_full.csv'),
            (originName_coords_to_gid_full_df, 'originName_coords_to_gid_full.csv'),
            (originName_coords_to_coords_full_df, 'originName_coords_to_coords_full.csv'),
        ],
        "/journeys/originName/processed/": [
            (originName_gid_to_gid_processed_df, 'originName_gid_to_gid.csv'),
            (originName_gid_to_coords_processed_df, 'originName_gid_to_coords.csv'),
            (originName_coords_to_gid_processed_df, 'originName_coords_to_gid.csv'),
            (originName_coords_to_coords_processed_df, 'originName_coords_to_coords.csv'),
        ],
        "/journeys/": [
            (hashes, 'journeys_hashes.csv')
        ]
    }
