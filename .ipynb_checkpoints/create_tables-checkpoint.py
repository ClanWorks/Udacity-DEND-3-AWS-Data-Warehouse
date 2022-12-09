import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    # Drop tables if they already exist
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    # Create staging and target tables in preparation for etl.py
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establish a connection to an Amazon Redshift cluster using the personal information provided in dwh.cfg
    Drop any existing tables to be used in this project.
    Create new tables with correct structure and data types
    Close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()