import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta,date
import db

FULL_ID_STATION="31713099999"
SHORT_ID_STATION="31713"
RANGE_YEARS=[2022,2023]


def loadDataFrame(in_nameFile,in_year):

    try:
        df=pd.read_csv(in_nameFile)
        return df
    except:
        print(f'Error loadDataFrame. File {in_nameFile} not founds in {in_year}\n')
        return None


def workDataFrame(in_nameFile, in_nyear, in_eyear):
    ret = 0


    dfo = None
    for iYear in range(in_nyear, in_eyear + 1):  
        df = loadDataFrame(f'{in_nameFile}099999.csv', iYear)
        if(df is None):
            return ret+1
        try:
            dfo = df[['DATE','TMP','DEW','AA1']]
        except Exception as e:
            print(e)
            return ret+1   

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

    #(dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(0.1*dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())).to_csv(f'selyninova {in_nameFile}.csv', sep=";")
    #(dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(10+dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())).to_csv(f'martonna {in_nameFile}.csv', sep=";")
    #(dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['KPZF'].mean()).to_csv(f'nesterov {in_nameFile}.csv', sep=";")
    dfs=dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(0.1*dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())
    dfm=dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['RAINF'].sum()/(10+dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['TMPF'].sum())
    dfn=dfo.groupby(pd.PeriodIndex(dfo['DATE'], freq="M"))['KPZF'].mean()
    
    indexS=db.DbConnect()
    mounse=0
    year=in_nyear
    date_format="%Y-%m"
    for i in range(len(dfs)):
        mounse+=1
        str_date=str(year)+"-"+str(mounse)
        date_object = datetime.strptime(str_date, date_format)
        
        values=[None,in_nameFile,date_object,round(dfs[i], 2),round(dfm[i], 2),round(dfn[i], 2)]
        indexS.insert("drought_index",values)
    indexS.save()
    
    print(dfo['TMPF'].sum())
    print(dfo['RAINF'].sum())

    print(dfo)
    #dfo.to_csv(f'{in_nameFile}.csv', sep=';', encoding='utf-8', index = False)
    #dfo_selyninova.to_csv('selyninova'+in_outputFile, sep=';', encoding='utf-8', index = False)
    return ret
