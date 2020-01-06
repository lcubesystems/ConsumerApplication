#!/usr/bin/python
import psycopg2



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        #params = Config()

        # connect to the PostgreSQL server
       # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname='nlp' user='glace' host='192.168.2.3' password='glacenxt'")

        # create a cursor
        cur = conn.cursor()

        # execute a statement
       # print('PostgreSQL database version:')
       # cur.execute('SELECT *  from kafkalogs ')

        # display the PostgreSQL database server version
       # db_version = cur.fetchall()
       # print(db_version)

        # close the communication with the PostgreSQL
        #cur.close()
        return  conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
   # finally:
       # if conn is not None:
            #conn.close()
            #print('Database connection closed.')


if __name__ == '__main__':
    connect()