import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Description: 
        A function to extract song and user log data from JSON files stored in an S3 bucket.
        This data is used to populate the song_raw and event_raw staging tables
        
    Arguments:
        conn: Connection to Amazon Redshift
        cur: cursor object. 

    Returns:
        None
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    Description: 
        A function to extract song and user information from staging tables,
        the data is then transformed and loaded into the five target tables
        
    Arguments:
        conn: Connection to Amazon Redshift
        cur: cursor object. 

    Returns:
        None
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Description: 
        The main function.
        A connection is established to an Amazon Redshift clusterusing user information provided in dwh.cdf.
        The load_staging_tables and insert_tables functions are run for desired bucket.
        
    Arguments:
        None
        
    Returns:
        None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()