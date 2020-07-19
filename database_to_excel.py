from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pathlib import Path
import pandas as pd

current_directory = Path(__file__).parent.absolute()

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

cluster = Cluster(
    contact_points=['127.0.0.1'], 
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
)
session = cluster.connect()
session.set_keyspace('covid19')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

rows = session.execute("""select * from covid19data""")
covid19_df = rows._current_rows

# print(df)

covid19_df.to_excel(Path(current_directory, 'covid19_database.xlsx').absolute(), index=False)

