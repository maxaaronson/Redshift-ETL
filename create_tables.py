import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''Drop all tables in the cluster

Drops all existing tables in the cluster based on the
drop_table_queries list

Arguments:
cur - a psycopg2 cursor object
conn - a psycogp2 connection object configured for the cluster
'''


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


'''Creates 2 staging and 5 analytics tables

Creates tables in the cluster based on the create_table_queries
list.  The analytics tables use a star schema

Arguments:
cur - a psycopg2 cursor object
conn - a psycogp2 connection object configured for the cluster
'''


def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(host=config['CLUSTER']['HOST'],
                            dbname=config['CLUSTER']['DB_NAME'],
                            user=config['CLUSTER']['DB_USER'],
                            password=config['CLUSTER']['DB_PASSWORD'],
                            port=config['CLUSTER']['DB_PORT'])

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    print("done")
