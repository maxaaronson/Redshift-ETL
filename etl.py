import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''Loads the staging tables

Copies data from S3 log and song data buckets and into the
staging tables in the cluster based on the queries
in copy_table_queries

Arguments:
cur - a psycopg2 cursor object
conn - a psycogp2 connection object configured for the cluster
'''


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


'''Populates the analytics tables in the cluster

Queries the data in the staging tables and inserts it into the
analytics tables using a star schema.  The insert queries are read
from insert_table_queries

Arguments:
cur - a psycopg2 cursor object
conn - a psycogp2 connection object configured for the cluster
'''


def insert_tables(cur, conn):
    for query in insert_table_queries:
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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
