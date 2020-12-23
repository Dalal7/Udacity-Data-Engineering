import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
    Iterates over the queries in the drop_table_queries list to drop all tables in the database
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    conn (psycopg2.connection): the database connection
    
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Iterates over the queries in the create_table_queries list to create the tables in the database
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    conn (psycopg2.connection): the database connection
    
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()