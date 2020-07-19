from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config = {
    # 'secure_connect_bundle': '/path/to/secure-connect-dbname.zip'
}
auth_provider = PlainTextAuthProvider(username='user', password='pass')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

