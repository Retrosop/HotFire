import argparse
from asyncio.windows_events import NULL
from math import nan
from queue import Empty
import sys
import mysql.connector
import pandas as pd
import numpy as np
import os

from mysql.connector import connect, Error

from datetime import datetime, timedelta
from dateutil.parser import parse


from args import *
from dataframe import *


#python hotgis.py --createdb ClimateDB
#python hotgis.py --querydb SELECT month(dt), sum(rn)*10/sum(tx) From daily_po1_rn GROUP by month(dt)
#sys.argv.extend(['--querydb','SELECT month(dt), sum(rn)*10/sum(tx) From daily_po1_rn GROUP by month(dt)'])
#python hotgis.py --querydf 2000-31725099999.csv
#sys.argv.extend(['--querydf','317250'])
#sys.argv.extend(['--querydf','31725'])
#python hotgis.py --movedata E:\NASAMETEO 2017 meteostation.csv
#sys.argv.extend(['--movedata','e:\\nasameteo,2022,meteostation.csv'])

userDB = "root"
passwordDB = "root"
nameDB = "iki2017"
ipDB = "127.0.0.1"



def main(args):

    ret=0
    
    pa = ArgProcess()

    #TODO
    if pa.createdb is not None:
        print(pa.createdb)
        nameDB  =  pa.createdb
    #TODO
    if pa.querydb is not None:
        print(pa.querydb)
        queryDB  =  pa.querydb
        runSQL(queryDB)

    if pa.querydf is not None:
        print(pa.querydf)
        ret+=workDataFrame(pa.querydf,2022,2022)
        
    #TODO
    if pa.movedata is not None:
        moveYear  =  pa.movedata.split(',')
        if len(moveYear) == 3:
            moveData(moveYear[0],moveYear[1],moveYear[2])
        else:
            print('Error read parametr --moveyear')
 
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

        
'''def loadDataFrame(in_nameFile,in_year):
    ret = []

    try:
        nameFile = f'C:\\HotFire\\31725099999.csv'
        #nameFile = f'C:\\NASAMETEO\\{in_year}dvo\\{in_nameFile}'
        x = open(nameFile,'r')
        namesRow = x.readline().replace('"','')
        ret = pd.read_csv(
            nameFile,
            names = namesRow.split(','),
            sep = ',',               
            skiprows = range(0, 1) 
        )
    except:
        print(f'Error loadDataFrame. File {in_nameFile} not founds in {in_year}\n')
    return ret'''


'''def workDataFrame(in_nameFile, in_nyear, in_eyear):
    ret = 0


    dfo = None
    for iYear in range(in_nyear, in_eyear + 1):  
        df = loadDataFrame(f'{in_nameFile}099999.csv', iYear)
        df0 = df[['DATE','TMP','DEW','AA1']]
        if dfo is None:
            dfo = df[['DATE','TMP','DEW','AA1']]
        else:
            dfo = pd.concat([dfo,df0], ignore_index=True)

    # data.set_index(['index_column'], inplace=True)
    # data.sort_index(inplace = True)
    # data.loc['index_value1', 'column_y']

    dfo.loc[:, 'RAIN'] = 0
    dfo['TMPF'] = 0
    dfo['DEWF'] = 0
    dfo['RAINF'] = 0
    dfo['HOURF'] = 0
    dfo['LPZF'] = 0
    dfo['KPZF'] = 0
    rain = 0
    tmpf = 0
    dewf = 0
    kpzf = 0

    for index,row in dfo.iterrows():

        dfo.at[index, 'TMP'] = round(float((row['TMP'].replace(',','.'))) / 10, 2)
        dfo.at[index, 'DEW'] = round(float((row['DEW'].replace(',','.'))) / 10, 2)
        
        if (parse(row['DATE']) + timedelta(hours=10)).hour == 10:
            dfo.at[index, 'TMPF'] = tmpf
            dfo.at[index, 'DEWF'] = dewf
            dfo.at[index, 'RAINF'] = rain
            dfo.at[index, 'HOURF'] = 10

            if dewf == 999.99:
                dewf  = tmpf - 5 #Восстановление точки росы

            if (parse(row['DATE']) + timedelta(hours=10)).month in range(3,12):
                if tmpf < 5:
                    lpz = 50
                else:    
                    lpz = tmpf * (tmpf - dewf)

                dfo.at[index, 'LPZF'] = round(lpz,2)
            
                if rain > 3:
                    kpzf = 0
                else:
                    kpzf += lpz

                dfo.at[index, 'KPZF'] = round(kpzf,2)
                if kpzf < 0:
                    print(row['DATE'])
                    print(tmpf)
                    print(dewf)
                    print(rain)
                    print('-------------')


            rain = 0

        
        if pd.isna(row['AA1']):
            dfo.at[index, 'AA1'] = '00,0000,0,0'
        else:
            if int(row['AA1'].split(',')[1]) != 9999:
                rain += int(row['AA1'].split(',')[1])
            else:
                rain = 0

        if (parse(row['DATE']) + timedelta(hours=10)).hour == 16:
            tmpf = round(float((row['TMP'].replace(',','.'))) / 10, 2)
            dewf = round(float((row['DEW'].replace(',','.'))) / 10, 2)

        dfo.at[index, 'DATE'] = (parse(row['DATE']) + timedelta(hours=10)).date()

 
    dfo = dfo.drop('TMP', axis=1)
    dfo = dfo.drop('DEW', axis=1)
    dfo = dfo.drop('AA1', axis=1)

    dfo = dfo.query('HOURF == 10')

    dfo = dfo.drop('HOURF', axis=1)

    df.set_index('DATE')

    (dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(0.1*dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())).to_csv(f'selyninova {in_nameFile}.csv', sep=";")
    (dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(10+dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())).to_csv(f'martonna {in_nameFile}.csv', sep=";")
    (dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['KPZF'].mean()).to_csv(f'nesterov {in_nameFile}.csv', sep=";")

    print(dfo['TMPF'].sum())
    print(dfo['RAINF'].sum())

    print(dfo)
    dfo.to_csv(f'{in_nameFile}.csv', sep=';', encoding='utf-8', index = False)
    #dfo_selyninova.to_csv('selyninova'+in_outputFile, sep=';', encoding='utf-8', index = False)
    return ret

def moveData(sPath,year,fMeteoStation):
    ret = 0
    deleteMeteo = pd.read_csv(fMeteoStation, sep =';', encoding='utf8')
    
    print(deleteMeteo['indx'])

    sFile = deleteMeteo['indx'].tolist()

    print(sPath, sFile)
	
    for f in sFile:
        path_new = f'{sPath}\\{year}dvo\\{f}099999.csv'
        path_old = f'{sPath}\\{year}\\{f}099999.csv'
	   
        if (os.path.isdir(path_new)):
            print (f'Catalog {year}dvo not found')
        elif (os.path.isdir(path_old)):
            print (f'Catalog {year} not found')
       
        try:
            os.rename(path_old, path_new)
            print(f'File copy {f}099999.csv')
        except:
            print(f'File not found {f}099999.csv')

    return ret'''

import args

if __name__ == '__main__':
    ret = main(sys.argv[1:])
    exit(ret)
