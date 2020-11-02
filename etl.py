import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_list


def load_staging_tables(cur, conn):
    print("\n-------COPY The Files Data to Staging Tables------\n")
    i = 0    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("{} : data is copied.".format(table_list[i]))
        i = i + 1


def insert_tables(cur, conn):
    print("\n-------INSERT The Data into Analytics Tables------\n")
    i = 2
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("{} : data is inserted.".format(table_list[i]))
        i = i + 1


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