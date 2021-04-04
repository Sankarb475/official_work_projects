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
    host = '10.143.182.153'
    port = 1521
    SID = 'NJIODEV'
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=SID)
    pwd = 'Eblwdb8#t'
    user = 'SIJIODB'
    db = cx_Oracle.connect(user, pwd, dsn_tns)

    input_query = "INSERT INTO password_strength(IP,PWD) values(:1,:2)"

    cursor = cx_Oracle.Cursor(db)
    cursor.prepare(input_query)
    cursor.executemany(None, listVals)

    # password extraction
    db.commit()
    cursor.close()
    db.close()

def main():
	listVals = []
	with open('PASSWORDAUDIT_01-03-2021.csv') as f:
			reader = csv.reader(f)
			#data = list(reader)
			ip = ''
			for row in reader:
					if 'IPAddress' in row[0]:
							ip = row[0].split('-')[1]
							if 'passwd' in ip or 'shadow' in ip:
									ipString = row[0].split('\n')
									ip = ipString[0].split('-')
									for j in ipString[1:]:
										if len(j)>50:
											print(j)
											print(row[0])
										listVals.append([str(ip),str(j)])
									continue
							if len(row)>50:
								print(row[0])
							continue
					listVals.append([str(ip),str(row[0])])
	print("length", len(listVals))
	for i in range(0,len(listVals),100000):
		sql_writer(listVals[i:i+100000])

				
if __name__ == "__main__":
		main()
		
