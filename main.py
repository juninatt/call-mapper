

import pandas as pd
import time

start_time = time.time()

# Läser in CSV-filen, hoppar över den första raden som är en header
df = pd.read_csv('api_calls_collected_traffic_iso8601.csv', skiprows=1, names=['@timestamp', 'fields.RequestPath'])

# Konverterar '@timestamp' kolumnen till datetime, markerar felaktigt formaterade datum som NaT
df['@timestamp'] = pd.to_datetime(df['@timestamp'].str.strip('"'), errors='coerce', format="%Y-%m-%dT%H:%M:%S.%fZ")

# Sparar rader med NaT i '@timestamp' till en lista
invalid_timestamps = df[df['@timestamp'].isna()]

# Skapar en ny DataFrame utan rader med NaT i '@timestamp'
df_valid = df.dropna(subset=['@timestamp'])

# Fortsätt med resten av din logik på df_valid
row_count = len(df_valid)
tidigaste_datum = df_valid['@timestamp'].min()
senaste_datum = df_valid['@timestamp'].max()
sorterad = df_valid['@timestamp'].is_monotonic

elapsed_time = time.time() - start_time

# Skriver resultat och rader med ogiltiga tidsstämplar till en textfil
with open('report.txt', 'w') as f:
    f.write(f'Antal giltiga rader: {row_count}\n')
    f.write(f'Tid för att skanna: {elapsed_time} sekunder\n')
    f.write(f'Tidigaste datum/tid: {tidigaste_datum}\n')
    f.write(f'Senaste datum/tid: {senaste_datum}\n')
    f.write(f'Sorterade: {sorterad}\n')
    f.write('Rader med ogiltiga tidsstämplar:\n')
    for index, row in invalid_timestamps.iterrows():
        f.write(f'Rad {index}: {row["@timestamp"]}, {row["fields.RequestPath"]}\n')

print(f'Klart! Antal giltiga rader: {row_count}, Tid: {elapsed_time} sekunder')

