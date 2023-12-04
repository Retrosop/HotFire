import argparse

def ValidQueryDF(n:list):
    select=["31713",2022,2023]
    for i in range(len(n)):
        if(i>2):
            break
        elif(i>1):
            try:
                select[i]=int(n[i])
            except Exception as e:
                print("Неудалось преобразовать аргумент {} в число".format(n[i]), e)
                return(None)
        else:
            select[i]=n[i]
        
    return select


def ArgProcess():

    parser = argparse.ArgumentParser()
    parser.add_argument('--createdb', type = str, help = 'Создание БД')
    parser.add_argument('--querydb', type = str, help = 'Запрос к  БД')
    #parser.add_argument('--querydf', type=str, nargs="+",help = 'Запрос к DF')
    parser.add_argument('--querydf', type=str, help = 'Запрос к DF')
    parser.add_argument('--movedata', type = str, help = 'Перемещение файлов заданного года')
    
    return parser.parse_args()
    

if(__name__=="__main__"):
    args=ArgProcess()
    print(args)
