import csv
from sqlalchemy import create_engine
import cx_Oracle
import csv
import base64
import sys
import pandas as pd
#from cryptography.fernet import Fernet
#import key

def sql_writer(listVals):
    import key
    host = '10.141.58.104'
    port = 1521
    SID = 'NJIO'
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=SID)
    pwd = key.fetchkey('gAAAAABf8wbPKzV80fuiCiLcmLy1Sgzy-ozWlNcDlvApju-on-Pk4TYY_u3E6Qw9HU_yIEZ60Zo0eUJC7XJMvgoiI43bXgCGkg==')
    user = 'topstna'
    db = cx_Oracle.connect(user, pwd, dsn_tns)

    input_query = "INSERT INTO weak_password_audit(IP,pwd_data) values(:1,:2)"

    cursor = cx_Oracle.Cursor(db)
    cursor.prepare(input_query)
    cursor.executemany(None, listVals)

    # password extraction
    db.commit()
    cursor.close()
    db.close()

def delete_old_data(ip):
    import key
    host = '10.141.58.104'
    port = 1521
    SID = 'NJIO'
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=SID)
    pwd = key.fetchkey('gAAAAABf8wbPKzV80fuiCiLcmLy1Sgzy-ozWlNcDlvApju-on-Pk4TYY_u3E6Qw9HU_yIEZ60Zo0eUJC7XJMvgoiI43bXgCGkg==')
    user = 'topstna'
    db = cx_Oracle.connect(user, pwd, dsn_tns)

    input_query = "delete from weak_password_audit where ip in ({0})".format(ip)
    print('delete query',input_query)
    cursor = db.cursor()
    cursor.execute(input_query)
    db.commit()


def main(input_path):
        # listVals will contain the rows to be inserted
        listVals = []
        # list of distinct IP present in the current input payload
        listIPs = []
        with open(input_path) as f:
                        reader = csv.reader(f)
                        #data = list(reader)
                        ip = ''
                        for row in reader:
                                        # extracting the ips
                                        if 'IPAddress' in row[0]:
                                                        ip = row[0].split('-')[1]
                                                        if 'passwd' in ip or 'shadow' in ip:
                                                                        ipString = row[0].split('\n')
                                                                        ip = ipString[0].split('-')[1]
                                                                        #delete_old_data(ip)
                                                                        listIPs.append(ip)
                                                                        for j in ipString[0:]:
                                                                                listVals.append([str(ip),str(j)])
                                                                        continue
                                                        #delete_old_data(ip)
                                                        listIPs.append(ip)

                                        listVals.append([str(ip),str(row[0])])
        print('Total number of new ips to be inserted/updated', len(listIPs))

        # oracle "in" condition can not take more than 1000 values
        for i in range(0, len(listIPs), 1000):
                del_condition = "'" + listIPs[i] + "',"
                for j in listIPs[i+1:i+1000]:
                        del_condition = del_condition + "'" + j + "',"
                delete_old_data(del_condition[:-1])

        print("Total number of rows to be update/inserted", len(listVals))

        # Since the number of rows are in 20+ lakhs, one time insertion is difficult, breaking it into 1 lakh row chunk
        for i in range(0,len(listVals),100000):
                sql_writer(listVals[i:i+100000])


if __name__ == "__main__":
                input_path = sys.argv[1]
                main(input_path)
