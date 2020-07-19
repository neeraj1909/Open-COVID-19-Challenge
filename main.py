# execution profile
# from cassandra import ConsistencyLevel
# from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
# from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
# from cassandra.query import tuple_factory



# profile = ExecutionProfile(
#     load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
#     retry_policy=DowngradingConsistencyRetryPolicy(),
#     consistency_level=ConsistencyLevel.LOCAL_QUORUM,
#     serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
#     request_timeout=15,
#     row_factory=tuple_factory
# )
# cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
# session = cluster.connect('covid19')

# print(session.execute("SELECT release_version FROM system.local").one())



# # execution profile
#     # from cassandra.cluster import Cluster
#     # cluster = Cluster()
#     # session = cluster.connect()
# local_query = 'SELECT rpc_address FROM system.local'
# for _ in cluster.metadata.all_hosts():
#     print(session.execute(local_query)[0])



# Model
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra.cqlengine import connection

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
    province_or_state = columns.Text(primary_key=True)
    country_or_region = columns.Text()
    last_update       = columns.DateTime()
    confirmed         = columns.Integer()
    deaths            = columns.Integer()
    recovered         = columns.Integer()


sync_table(CovidModel)

print('Already in database', CovidModel.objects.count())

# analysis part
raw_df = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/01-22-2020.csv')

covid_data_df = raw_df.rename(columns={
    'Province/State': 'province_or_state',
    'Country/Region': 'country_or_region',
    'Last Update': 'last_update',
    'Confirmed': 'confirmed',
    'Deaths': 'deaths',
    'Recovered': 'recovered'
})

# Inserting each row entry in the table CovidModel
for row in covid_data_df.iterrows():
    row_dict = row[1].to_dict()
    
    row_dict['confirmed'] = 0 if pd.isna(row_dict['confirmed']) else int(row_dict['confirmed'])
    row_dict['deaths'] = 0 if pd.isna(row_dict['deaths']) else int(row_dict['deaths'])
    row_dict['recovered'] = 0 if pd.isna(row_dict['recovered']) else int(row_dict['recovered'])

    try:
        CovidModel.create(**row_dict)
    except TypeError as e:
        print(row_dict)
        raise e
        continue
