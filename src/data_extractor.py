import pandas as pd


def analyze_data(df):
    # Extract URL parts
    df['journeys_next'] = df[df['fields.RequestPath'].str.contains('journeys')]['fields.RequestPath'].str.extract('journeys/([^/?]+)')
    df['locations_next'] = df[df['fields.RequestPath'].str.contains('locations')]['fields.RequestPath'].str.extract('locations/([^/?]+)')
    df['journeys_second_part'] = df[df['fields.RequestPath'].str.contains('journeys')]['fields.RequestPath'].str.extract('journeys/[^/?]+/([^/?]+)')

    # Calculate frequencies in percentage
    journeys_next_freq = (df['journeys_next'].value_counts(normalize=True).head(10) * 100).to_dict()
    locations_next_freq = (df['locations_next'].value_counts(normalize=True).head(10) * 100).to_dict()
    journeys_second_part_freq = (df['journeys_second_part'].value_counts(normalize=True).head(10) * 100).to_dict()

    results = {
        'journeys_next_freq': journeys_next_freq,
        'locations_next_freq': locations_next_freq,
        'journeys_second_part_freq': journeys_second_part_freq,
    }

    return results



