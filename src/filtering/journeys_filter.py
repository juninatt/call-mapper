import pandas
import re
from tqdm import tqdm

from src.data_processing.data_sorter import process_api_calls, extract_matching_requests
from src.data_processing.report_generator import generate_csv
import pandas as pd


origin_name_pattern = r'/(v\d+)/journeys\?.*?originGid=(\d+).*?&originName=.*?&originLatitude=([\d.-]+).*?&originLongitude=([\d.-]+).*?&destinationGid=(\d+).*?&destinationName=.*?&destinationLatitude=([\d.-]+).*?&destinationLongitude=([\d.-]+)'
origin_name_columns = ['version', 'originGid', 'destinationGid', 'originLatitude', 'originLongitude', 'destinationLatitude', 'destinationLongitude']

destination_name_pattern = r'/(v\d+)/journeys\?.*?originGid=(\d+).*?destinationLatitude=([\d.-]+).*?&destinationLongitude=([\d.-]+)'
destination_name_columns = ['version', 'originGid', 'destinationLatitude', 'destinationLongitude']


# Apply filtering and create the final DataFrame
def journeys_filter_and_generate_csv(df, column='all_requests'):
    # Extract all journeys calls except followed by hash
    generate_csv(df, '/journeys/all_journeys.csv')

    # ORIGIN_GID & ...
    all_origin_gid_df = extract_matching_requests(df, column, 'journeys\?originGid', '=', '')

    # ORIGIN_NAME
    all_origin_name_df = extract_matching_requests(all_origin_gid_df, column, 'originGid', 'originName', '')
    final_origin_name_df = pd.DataFrame(columns=origin_name_columns)
    final_origin_name_df[origin_name_columns] = all_origin_name_df[column].str.extract(origin_name_pattern)
    generate_csv(final_origin_name_df, '/journeys/originGid_originName.csv')

    # DESTINATION_NAME
    all_destination_name_df = extract_matching_requests(all_origin_gid_df, column, 'originGid', 'destinationName', 'originName')
    final_destination_name_df = pd.DataFrame(columns=destination_name_columns)
    final_destination_name_df[destination_name_columns] = all_origin_name_df[
        column].str.extract(destination_name_pattern)
    generate_csv(final_destination_name_df, 'journeys/originGid_destinationName.csv')

    #  DATE_TIME -------------------------------------------

    all_datetime_df = extract_matching_requests(df, column, 'v[234]', 'journeys\?dateTime=')
    final_datetime_df = pd.DataFrame(columns=['version', 'originGid', 'destinationGid'])
    final_datetime_df[['version', 'originGid', 'destinationGid']] = all_datetime_df[column].str.extract(
        r'/v(\d+)/.*?originGid=(\d+)&.*?destinationGid=(\d+)',
        expand=True
    )
    generate_csv(final_datetime_df, 'journeys/final_datetime.csv')

    #  DATE_TIME_RELATES_TO ---------------------------------

    all_datetime_relates_df = extract_matching_requests(df, column, 'v[234]', 'journeys\?dateTimeRelatesTo=')

    final_datetime_relates_df = pd.DataFrame(columns=['version', 'originGid', 'destinationGid'])
    final_datetime_relates_df[['version', 'originGid', 'destinationGid']] = all_datetime_relates_df[column].str.extract(
        r'/v(\d+)/.*?originGid=(\d+)&.*?destinationGid=(\d+)',
        expand=True
    )
    generate_csv(all_datetime_relates_df, 'journeys/all_datetime_relates_to.csv')
    generate_csv(final_datetime_relates_df, 'journeys/final_datetime_relates_to.csv')





