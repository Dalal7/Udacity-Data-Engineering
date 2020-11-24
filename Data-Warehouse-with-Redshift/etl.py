import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    Iterates over the queries in the copy_table_queries list to load data from s3 to staging tables
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    conn (psycopg2.connection): the database connection
    
    """        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    Iterates over the queries in the insert_table_queries list to insert data in analytics tables from the staging tables
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    conn (psycopg2.connection): the database connection
    
    """        
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()