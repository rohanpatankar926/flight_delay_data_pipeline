from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

def connect_cassandra():
    cloud_config= {
    'secure_connect_bundle': f'{os.getcwd()}/flight_delay/data_pipeline/secure-connect-cassandra-demo(1).zip'
    }
    auth_provider = PlainTextAuthProvider('wSwnJwCiTyJoKICHIifZNIGD', 'pPXf_ulN2xybkRRvhj._gehdaRPIvNr1bdtsCnfG2OAxAiqv6BN27MUfKuObS-.Qv9tqTw+SxBLecQT4g_7Jk7qUUdDpyymH1Rc8l_n0NKZUdAzYC.n4NGqKeY0+M0.M')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace("test")
    return session

if __name__=="__main__":
    session = connect_cassandra()
    res=session.execute("select * from a")
    for i in res:
        print(i)
    