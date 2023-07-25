import argparse
from asyncio.windows_events import NULL
from math import nan
from queue import Empty
import sys
import mysql.connector
import pandas as pd
import numpy as np

from mysql.connector import connect, Error

#sys.argv.extend(['--CreateDB','ClimateDB'])
#sys.argv.extend(['--QueryDB','SELECT month(dt), sum(rn)*10/sum(tx) From daily_po1_rn GROUP by month(dt)'])
sys.argv.extend(['--QueryDF','2000-31725099999.csv'])

userDB = "root"
passwordDB = "root"
nameDB = "iki2017"
ipDB = "127.0.0.1"

def main(args):
    ret = 0
    parser = argparse.ArgumentParser()
    parser.add_argument('--CreateDB', help = 'Создание БД')
    parser.add_argument('--QueryDB', help = 'Запрос к  БД')
    parser.add_argument('--QueryDF', help = 'Запрос к DF')


    pa = parser.parse_args(args)

    if pa.CreateDB is not None:
        print(pa.CreateDB)
        nameDB  =  pa.CreateDB
    
    if pa.QueryDB is not None:
        print(pa.QueryDB)
        queryDB  =  pa.QueryDB
        runSQL(queryDB)

    if pa.QueryDF is not None:
        print(pa.QueryDF)
        queryDF  =  pa.QueryDF
        workDataFrame(queryDF)
 
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
def loadDataFrame(in_nameFile):
    ret = []
    try:
        ret = pd.read_csv(
            in_nameFile,
            names=["STATION","DATE","SOURCE","LATITUDE","LONGITUDE","ELEVATION","NAME",
                   "REPORT_TYPE","CALL_SIGN","QUALITY_CONTROL","WND","CIG","VIS","TMP","DEW",
                   "SLP","AA1","AJ1","AW1","AY1","AY2","AZ1","GA1","GF1","IA1","IA2","KA1",
                   "MA1","MD1","MW1","OA1","REM","EQD"],
            sep=',',               
            skiprows = range(0, 1) 
        )
    except:
        print(f'Error loadDataFrame. File {in_nameFile} not founds\n')
    return ret


def workDataFrame(in_nameFile):
    ret = 0
    df = loadDataFrame(in_nameFile)[1000:1500]
    dfo = df[['DATE','TMP','DEW','AA1']]
    for index,row in dfo.iterrows():
        dfo.at[index, 'TMP'] = float((row['TMP'].replace(',','.'))) / 10
        dfo.at[index, 'DEW'] = float((row['DEW'].replace(',','.'))) / 10
        if pd.isna(row['AA1']):
            dfo.at[index, 'AA1'] = -1

    print(dfo)
    return ret




if __name__ == '__main__':
    ret = main(sys.argv[1:])
    exit(ret)