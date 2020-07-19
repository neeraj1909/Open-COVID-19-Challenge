from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra.cqlengine import connection

import arrow
from datetime import datetime
import pandas as pd
import numpy as np

KEYSPACE = 'covid19'

connection.setup(['127.0.0.1'], "cqlengine", protocol_version=3)
create_keyspace_simple(KEYSPACE, 1)


class CovidModel(Model):
    __keyspace__ = KEYSPACE
    __table_name__ = 'covid19data'

    # Index(['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered'], dtype='object')
    id = columns.Text(primary_key=True)
    province_or_state = columns.Text()
    country_or_region = columns.Text()
    last_update       = columns.DateTime()
    confirmed         = columns.Integer()
    deaths            = columns.Integer()
    recovered         = columns.Integer()


sync_table(CovidModel)

print('Already in database', CovidModel.objects.count())


import glob, os

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

CSV_INPUT_DIRECTORY = os.path.join(CURRENT_DIRECTORY, "./COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/")
os.chdir(CSV_INPUT_DIRECTORY)


def parse_datetime_string(datetime_str):
    try:
        return arrow.get(datetime_str).datetime
    except arrow.parser.ParserError:
        pass

    SUPPORTED_FORMATS =  [
        'M/D/YYYY HH:mm',
        'M/D/YY HH:mm',
        'M/D/YYYY H:mm',
        'M/D/YY H:mm',
    ]

    for format in SUPPORTED_FORMATS:
        try:
            return arrow.get(datetime_str, format).datetime 
        except arrow.parser.ParserMatchError:
            continue
    
    raise ValueError(f'Could not parse datetime string: {datetime_str}')
    

def get_column_rename_dict(given_column_names):
    all_mappings = {
        'province_or_state': ['Province/State', 'Province_State'],
        'country_or_region': ['Country/Region', 'Country_Region'],
        'last_update': ['Last Update', 'Last_Update'],
        'confirmed': ['Confirmed',],
        'deaths': ['Deaths',],
        'recovered': ['Recovered',],
    }

    mapping_dict_for_current = {}
    for desired_column_name, possible_col_headers in all_mappings.items():
        for col_header in possible_col_headers:
            if col_header in given_column_names:
                mapping_dict_for_current[col_header] = desired_column_name
                break
    
    missing_keys = set(all_mappings.keys()) - set(mapping_dict_for_current.values())
    if missing_keys:
        raise KeyError(f'Did not find anything for : {missing_keys}', given_column_names)
    
    return mapping_dict_for_current



for filename in glob.glob("*.csv"):
    csv_filepath = os.path.join(CSV_INPUT_DIRECTORY, filename)
    print(csv_filepath)

    # analysis part
    raw_df = pd.read_csv(csv_filepath)

    covid_data_df = raw_df.rename(columns=get_column_rename_dict(list(raw_df.columns.values)))

    # Inserting each row entry in the table CovidModel
    for row_index, row in covid_data_df.iterrows():
        data = dict()    
        
        data['country_or_region'] = row.country_or_region.strip()
        data['province_or_state'] = '' if pd.isna(row.province_or_state) else row.province_or_state.strip()
        
        data['last_update'] = parse_datetime_string(row.last_update)
        
        data['confirmed'] = 0 if pd.isna(row.confirmed) else int(row.confirmed)
        data['deaths'] = 0 if pd.isna(row.deaths) else int(row.deaths)
        data['recovered'] = 0 if pd.isna(row.recovered) else int(row.recovered)

        data['id'] = '__'.join([data['country_or_region'], data['province_or_state'], data['last_update'].isoformat()])

        try:
            CovidModel.create(**data)
        except Exception as e:
            print(row_dict)
            raise e
            continue
