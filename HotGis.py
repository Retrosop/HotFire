import argparse
import sys
import mysql.connector

from mysql.connector import connect, Error

#sys.argv.extend(['--CreateDB','ClimateDB'])
sys.argv.extend(['--QueryDB','SELECT * From daily_po1_rn limit 10'])

userDB = "root"
passwordDB = "root"
nameDB = "iki2017"
ipDB = "127.0.0.1"

def main(args):
    ret = 0
    parser = argparse.ArgumentParser()
    parser.add_argument('--CreateDB', help = 'Создание БД')
    parser.add_argument('--QueryDB', help = 'Запрос к  БД')

    pa = parser.parse_args(args)

    if pa.CreateDB is not None:
        print(pa.CreateDB)
        nameDB  =  pa.CreateDB
    
    if pa.QueryDB is not None:
        print(pa.QueryDB)
        queryDB  =  pa.QueryDB
        runSQL(queryDB)
 
    return ret

def runSQL(in_Query):

    try:
        with connect(
            host = ipDB,
            user = userDB,
            password = passwordDB,
            database = nameDB
        ) as connection:
            db_query = in_Query
            with connection.cursor() as cursor:
                cursor.execute(db_query)
                for db in cursor:
                    print(db)
    except Error as e:
        print(e)

if __name__ == '__main__':
    ret = main(sys.argv[1:])
    exit(ret)