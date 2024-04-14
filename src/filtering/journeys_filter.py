import os
from dotenv import load_dotenv
import pandas as pd

from src.data_processing.data_sorter import extract_matching_requests, extract_non_matching_requests, process
from src.data_processing.report_generator import generate_csv

# Assign query string patterns
load_dotenv()

# Load environment variables
load_dotenv()


# Extracting and parsing patterns and columns from environment
def get_columns(env_var):
    return os.getenv(env_var).split(',')


# Patterns
default_pattern = os.getenv('DEFAULT_PATTERN')
originGid_destinationGid_pattern = os.getenv('ORIGIN_GID_DESTINATION_GID_PATTERN')
originGid_destinationCoordinates_pattern = os.getenv('ORIGIN_GID_DESTINATION_COORDINATES_PATTERN')
originCoordinates_destinationCoordinates_pattern = os.getenv('ORIGIN_COORDINATES_DESTINATION_COORDINATES_PATTERN')
originCoordinates_destinationGid_pattern = os.getenv('ORIGIN_COORDINATES_DESTINATION_GID_PATTERN')

# Columns
originGid_destinationGid_columns = get_columns('ORIGIN_GID_DESTINATION_GID_COLUMNS')
originGid_destinationCoordinates_columns = get_columns('ORIGIN_GID_DESTINATION_COORDINATES_COLUMNS')
originCoordinates_destinationCoordinates_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_COORDINATES_COLUMNS')
originCoordinates_destinationGid_columns = get_columns('ORIGIN_COORDINATES_DESTINATION_GID_COLUMNS')


# Apply filtering and create the final DataFrame
def journeys_filter_and_generate_csv(df, column='all_requests'):
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
        originGid_destinationGid_pattern,
        originGid_destinationGid_columns
    )

    originGid_allCoordinates_full_df = extract_matching_requests(
        originGid_full_df,
        'originLatitude',
        'originLongitude',
        'destinationGid'
    )

    originGid_allCoordinates_processed_df = process(
        originGid_allCoordinates_full_df,
        originCoordinates_destinationCoordinates_pattern,
        originCoordinates_destinationCoordinates_columns
    )

    originGid_destinationCoordinates_full_df = extract_matching_requests(
        originGid_full_df,
        'originGid',
        'destinationName',
        'originName')

    originGid_destinationCoordinates_processed_df = process(
        originGid_destinationCoordinates_full_df,
        originGid_destinationCoordinates_pattern,
        originGid_destinationCoordinates_columns
    )

    # Unprocessed
    generate_csv(
        originGid_full_df,
        'journeys/originGid/unprocessed/all_originGid.csv'
    )
    generate_csv(
        originGid_destinationGid_full_df,
        '/journeys/originGid/unprocessed/originGid_destinationGid.csv')
    generate_csv(
        originGid_destinationCoordinates_full_df,
        'journeys/originGid/unprocessed/originGid_destinationCoordinates.csv')
    generate_csv(
        originGid_allCoordinates_full_df,
        'journeys/originGid/unprocessed/originGid_allCoordinates.csv')

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
        'journeys\?dateTime=')

    dateTime_originGid_destinationGid_full_df = extract_matching_requests(
        dateTime_full_df,
        'originGid',
        'destinationGid'
    )
    dateTime_originGid_destinationGid_processed_df = process(
        dateTime_originGid_destinationGid_full_df,
        originGid_destinationGid_pattern,
        originGid_destinationGid_columns
    )

    dateTime_originGid_destinationCoordinates_full_df = extract_matching_requests(
        dateTime_full_df,
        'originGid',
        'destinationLatitude'
    )
    dateTime_originGid_destinationCoordinates_processed_df = process(
        dateTime_originGid_destinationCoordinates_full_df,
        originGid_destinationCoordinates_pattern,
        originGid_destinationCoordinates_columns
    )

    dateTime_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        dateTime_full_df,
        'originLatitude',
        'destinationLatitude'
    )
    dateTime_originCoordinates_destinationCoordinates_processed_df = process(
        dateTime_originCoordinates_destinationCoordinates_full_df,
        originCoordinates_destinationCoordinates_pattern,
        originCoordinates_destinationCoordinates_columns
    )
    dateTime_originCoordinates_destinationGid_full_df = extract_matching_requests(
        dateTime_full_df,
        'originLatitude',
        'destinationGid'
    )
    dateTime_originCoordinates_destinationGid_processed_df = process(
        dateTime_originCoordinates_destinationGid_full_df,
        originCoordinates_destinationGid_pattern,
        originCoordinates_destinationGid_columns
    )

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

    dateTimeRelatesTo_full_df = extract_matching_requests(df, 'v[234]', 'journeys\?dateTimeRelatesTo=')

    dateTimeRelatesTo_originGid_destinationGid_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originGid',
        'destinationGid'
    )
    dateTimeRelatesTo_originGid_destinationGid_processed_df = process(
        dateTimeRelatesTo_originGid_destinationGid_full_df,
        originGid_destinationGid_pattern,
        originGid_destinationGid_columns
    )

    dateTimeRelatesTo_originGid_destinationCoordinates_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originGid',
        'destinationLatitude'
    )
    dateTimeRelatesTo_originGid_destinationCoordinates_processed_df = process(
        dateTimeRelatesTo_originGid_destinationCoordinates_full_df,
        originGid_destinationCoordinates_pattern,
        originGid_destinationCoordinates_columns
    )

    dateTimeRelatesTo_originCoordinates_destinationGid_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originLatitude',
        'destinationGid'
    )
    dateTimeRelatesTo_originCoordinates_destinationGid_processed_df = process(
        dateTimeRelatesTo_originCoordinates_destinationGid_full_df,
        originCoordinates_destinationGid_pattern,
        originCoordinates_destinationGid_columns
    )

    dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        dateTimeRelatesTo_full_df,
        'originLatitude',
        'destinationLatitude'
    )
    dateTimeRelatesTo_originCoordinates_destinationCoordinates_processed_df = process(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df,
        originCoordinates_destinationCoordinates_pattern,
        originCoordinates_destinationCoordinates_columns
    )

    # Unprocessed
    generate_csv(
        dateTimeRelatesTo_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/all_dateTimeRelatesTo.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationCoordinates_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationGid_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/originGid_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationGid_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/originCoordinates_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_full_df,
        '/journeys/dateTimeRelatesTo/unprocessed/originCoordinates_destinationCoordinates.csv'
    )

    # Processed
    generate_csv(
        dateTimeRelatesTo_originGid_destinationGid_processed_df,
        '/journeys/dateTimeRelatesTo/originGid_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originGid_destinationCoordinates_processed_df,
        '/journeys/dateTimeRelatesTo/originGid_destinationCoordinates.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationGid_processed_df,
        '/journeys/dateTimeRelatesTo/originCoordinates_destinationGid.csv'
    )
    generate_csv(
        dateTimeRelatesTo_originCoordinates_destinationCoordinates_processed_df,
        '/journeys/dateTimeRelatesTo/originCoordinates_destinationCoordinates.csv'
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
        originGid_destinationGid_pattern,
        originGid_destinationGid_columns
    )

    originName_originGid_destinationCoordinates_full_df = extract_matching_requests(
        originName_full_df,
        'originGid',
        'destinationLatitude'
    )
    originName_originGid_destinationCoordinates_processed_df = process(
        originName_originGid_destinationCoordinates_full_df,
        originGid_destinationCoordinates_pattern,
        originGid_destinationCoordinates_columns
    )

    originName_originCoordinates_destinationGid_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'originLatitude',
        'destinationLatitude'
    )
    originName_originCoordinates_destinationGid_processed_df = process(
        originName_originCoordinates_destinationGid_full_df,
        originCoordinates_destinationGid_pattern,
        originCoordinates_destinationGid_columns
    )

    originName_originCoordinates_destinationCoordinates_full_df = extract_matching_requests(
        originName_full_df,
        'originLongitude',
        'destinationLatitude',
        'originGid'
    )
    originName_originCoordinates_destinationCoordinates_processed_df = process(
        originName_originCoordinates_destinationCoordinates_full_df,
        originCoordinates_destinationCoordinates_pattern,
        originCoordinates_destinationCoordinates_columns
    )

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
        '/journeys/originName/originGid_destinationGid.csv'
    )
    generate_csv(
        originName_originCoordinates_destinationCoordinates_processed_df,
        '/journeys/originName/originGid_destinationCoordinates.csv'
    )
    generate_csv(
        originName_originCoordinates_destinationGid_processed_df,
        '/journeys/originName/originCoordinates_destinationGid.csv'
    )

    hashes = extract_non_matching_requests(
        df, [originGid_full_df, dateTime_full_df, dateTimeRelatesTo_full_df, originName_full_df]
    )

    generate_csv(hashes, 'journeys/leftovers.csv')
