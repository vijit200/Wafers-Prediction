import shutil
import mysql.connector as conn
from os import listdir
from datetime import datetime
import csv
from Logging.logger import App_Logger
import os

class dbOperation:

    def __init__(self) -> None:
        
        """This class shall be used for handling all the SQL operations."""


        self.path = 'Training_Database/'
        self.badFilePath = 'Training_Raw_Files_validated/Bad_Raw'
        self.goodFilePath = 'Training_Raw_Files_validated/Good_Raw'
        self.logger = App_Logger()

    def databaseconnection(self,Databasename):

        "connecting to database and creating database if not exsist"

        try:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')

            mydb = conn.connect(host = 'localhost',user = 'root',password = 'Vijit699@')
            cur = mydb.cursor()
            cur.execute('CREATE DATABASE IF NOT EXISTS {}'.format(Databasename))

            self.logger.log(file, "Opened %s database successfully" % Databasename)
            file.close()

        except ConnectionError:

            file = open('Training_Logs/DataBaseConnectionLog.txt','a+')

            self.logger.log(file,"There is error in connecting database : %s " %ConnectionError)
            file.close()
            raise ConnectionError

    def createTable(self,Databasename,column_names):

        "Here we are creating table in database"

        try:
            c = self.databaseconnection(Databasename)
            cur = c.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")

            if c.fetchone()[0] ==1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % Databasename)
                file.close()

            else:
                for key in column_names.keys():
                    type = column_names[key]

                    #if table exsist we alter it else create it
                    try:
                        cur.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column}" {ty}'.format(column=key,ty=type))

                    except:

                        cur.execute('CREATE TABLE Good_Raw_Data ({column} {ty})'.format(column=key,ty=type))

            conn.close()

            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')

            self.logger.log(file,'Table created successfully')

            file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % Databasename)
            file.close()
            raise e


    def insertintotableGoodData(self,Database):

        """This function help to insert data into above table"""

        c = self.databaseconnection(Database)
        cur = c.cursor()
        good_file_path = self.goodFilePath
        bad_file_path = self.badFilePath
        onlyfilr = [f for f in listdir(good_file_path)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfilr:
            try:

                with open(good_file_path + '/' + file,'r') as f:

                    next(f)
                    reader = csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for line_ in (line[1]):
                            try:
                                cur.execute('INSERT INTO Good_Raw_Data VALUES({values})'.format(values=(line_)))

                                self.logger.log(log_file,'Data Inserted successfully')

                                log_file.close()
                            except Exception as e:

                                self.logger.log(log_file,'Data Not Inserted successfully')
                                log_file.close()

                                raise e
            except Exception as e:

                cur.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(good_file_path+'/' + file, bad_file_path)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                cur.close()


        cur.close()
        log_file.close()


        def selectingdatafromtableintocsv(self,Database):

            """Here we will extracr data from table and stroe in form of csv"""

            self.fileFromDb = 'Training_FileFromDB/'
            self.fileName = 'InputFile.csv'
            log_file = open("Training_Logs/ExportToCsv.txt", 'a+')

            try:

                c = conn.conn.connect(host = 'localhost',user = 'root',password = 'Vijit699@')

                cur = c.cursor()
                cur.execute('USE {}'.format(Database))

                cur.execute('select * from Good_Raw_Data')

                result = cur.fetchall()
                #getting header of csv file
                header = [i[0] for i in cur.description]
                #Making csv output directory
                if not os.path.isdir(self.fileFromDb):
                    os.makedirs(self.fileFromDb)
                #Open Csv file for writing
                csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

                # Add the headers and data to the CSV file.
                csvFile.writerow(header)
                csvFile.writerows(result)

                self.logger.log(log_file, "File exported successfully!!!")
                log_file.close()

            except Exception as e:
                self.logger.log(log_file, "File exporting failed. Error : %s" %e)
                log_file.close()

                







        


    