import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

class DbConnect():
    def __init__(self):
        self.user="mysql"
        self.password="mysql"
        self.database="metio"
        self.host="localhost"

        config = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }

        self.connected=False
        self.cnx=self.connect_to_mysql(config)
        if self.cnx and self.cnx.is_connected():
            self.connected=True

                



    def connect_to_mysql(self,config):
        try:
            return mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return None


    def __del__(self):
        if self.connected:
            self.cnx.close()

            

    def insert(self,table_name:str,columns:list):

        values=','.join(['%s'] * len(columns))
        insert_query="INSERT INTO {} VALUES ({})".format(table_name,values)
        
        cursor=self.cnx.cursor()
        try:
            cursor.execute(insert_query, columns)
        except mysql.connector.Error as err:
            print("Error INSERT IN {} VALUES {}:".format(table_name,columns),err.msg)
        else:
            print("INSERT IN {} VALUES {}: OK".format(table_name,columns))

        cursor.close()
                        

    def save(self):
        self.cnx.commit()

    def truncate_table(self,table_name):
        truncate_query="TRUNCATE TABLE {}".format(table_name)
        cursor=self.cnx.cursor()
        try:
            cursor.execute(truncate_query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print(truncate_query,": OK")
        cursor.close()
    

    def create_tables(self):
        TABLES = {}
        TABLES["drought_index"] = (
        "CREATE TABLE `drought_index` ("
        "  `id` int(11) NOT NULL AUTO_INCREMENT,"
        "  `indexvmo` int(6) NOT NULL,"
        "  `date` DATE NOT NULL,"
        "  `index_selyninova` FLOAT,"
        "  `index_martonna` FLOAT,"
        "  `index_nesterova` FLOAT,"
        "  PRIMARY KEY (`id`)"
        ") ENGINE=InnoDB")

        cursor=self.cnx.cursor()
        
        for table_name in TABLES:
            table_description = TABLES[table_name]
            
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")
        cursor.close()


if __name__=="__main__":
    a=DbConnect()
    a.create_tables()
    
    #str_date="2023-09"
    #date_format="%Y-%m"
    #date_object = datetime.strptime(str_date, date_format)
    
    #a.insert("drought_index",[None,1321,date_object,45.45754,33.34,0.566343646])
    #a.truncate_table("drought_index")
    #a.save()
    pass
