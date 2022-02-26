import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into staging tables using queries in the copy_table_queries list
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    """
    Loads data into 5 tables n the schema using queries in the copy_table_queries list
    """
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Parses config file.
    
    - connects with redshift database using config values.
    
    - calls the previously defined load_staging_tables function to load staging tables
    
    - calls previously defined insert_tables function to create final tables
    
    - closes database connection.
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