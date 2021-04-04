from sqlalchemy import create_engine
import cx_Oracle
import csv
import base64
import sys
import pandas as pd
from cryptography.fernet import Fernet
import key

# converts the query output list to a dictionary
def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


def get_connection_details(indicator, identifier, input_query):
        import key
        ip = ''
        port = 1521
        SID = ''
        dsn_tns = cx_Oracle.makedsn(ip, port, service_name=SID)
        pwd = key.fetchkey('')
        user = ''
        db = cx_Oracle.connect(user, pwd, dsn_tns)
        cursor = db.cursor()

        # password extraction
        query = "SELECT * FROM ANSIBLE_DB_CONNECTION_BKP where id = '{0}'".format(identifier)
        cursor.execute(query)
        dict = rows_to_dict_list(cursor)[0]
        #print(dict)

        # New connection based on user input
        dict['PWD'] = key.fetchkey(dict['PWD'])
        if indicator == 0:
                dict['QUERY'] = ''.join(dict['QUERY'].read())
        else:
                dict['QUERY'] = input_query

        dict['INDICATOR'] = indicator
        cursor.close()
        db.close()

        return dict


def main(details):
        import key

        dsn_tns = cx_Oracle.makedsn(details['HOST'], details['PORT'], service_name=details['SERVICE_NAME'])
        db = cx_Oracle.connect(details['USER_ID'], details['PWD'], dsn_tns)
        cursor = db.cursor()

        input_query = details['QUERY']
        operation = details['OPERATION']
        indicator = details['INDICATOR']

        if operation== 'R':
                #print('Before final query')
                df_ora = pd.read_sql(input_query, con=db)
                #print('after fianl query')
                if 'WRITE_PATH' in details.keys():
                        write_path = details['WRITE_PATH']
                        #if len(df_ora.columns) == 1:
                        m = df_ora.values.tolist()
                        #print(m)
                        print('select query output path:', write_path)
                        with open(write_path, 'w') as f:
                                for i in m:
                                        i = ['' if x is None else x for x in i]
                                        i = [str(x) for x in i]
                                        #print(','.join(i))
                                        f.write("%s\n" % ','.join(i))
                        #df_ora.to_csv(write_path,header = False, sep = '\t',index = False,quoting=csv.QUOTE_NONE, quotechar="", escapechar="\\")
                        #else:
                         #       df_ora.to_csv(write_path,header = False, sep = ',',index = False)

                else:
                        print(df_ora)
                print(df_ora)
                #print('read operation completed')
        elif operation == 'W':
		table_name = input_query.upper().split('INTO')[1].split('(')[0].strip()
                #input_path = details['READ_PATH']
                Error_rows = []
                Error_messages = []
                successful_count = 0
                wrong_count = 0
		if 'refresh' in details['ID'].lower():
			try:
				cursor.execute(input_query)
				successful_count = successful_count + 1
			except Exception as e:
				wrong_count = wrong_count + 1
				Error_rows.append([no+1, my_list[no]])
				Error_messages.append([no+1,e.message])
				
                else:
			input_path = details['READ_PATH']
			no_of_values = input_query.upper().split('VALUES')[1].split(':')[-1].strip()[:-1]
			no_of_values = int(no_of_values)
			print('insert query input file path: ',input_path)
			with open(input_path, 'r') as f:
				file = csv.reader(f)
				my_list = list(file)
				#print(my_list)
				for no in range(len(my_list)):
					try:
						#print(line)
						cursor.execute(input_query,my_list[no][:no_of_values])
						successful_count = successful_count + 1
					except Exception as e:
						wrong_count = wrong_count + 1
						Error_rows.append([no+1, my_list[no]])
						Error_messages.append([no+1,e.message])
                db.commit()
                print('successful insertion :', successful_count)
                print('error count :', wrong_count)
                # content of .log file
                if 'LOG_PATH' in details.keys():
                        log_path = details['LOG_PATH']
                        print(log_path)
                        log_file = open(log_path, 'w')
                        log_file.write('Table {0} \n'.format(table_name))
                        log_file.write("{0} Rows successfully uploaded. \n".format(successful_count))
                        log_file.write("{0} Rows not loaded due to data errors. \n".format(len(Error_messages)))
                        log_file.close()
                        df_logs = pd.DataFrame(Error_messages)
                        df_logs.to_csv(log_path, header=None, index=None, sep=' ', mode='a')

                # writing to .bad file
                if 'BAD_PATH' in details.keys():
                        bad_path = details['BAD_PATH']
                        print(bad_path)
                        bad_file = open(bad_path, 'w')
                        bad_file.write('Table {0} \n'.format(table_name))
                        bad_file.close()
                        df_errors = pd.DataFrame(Error_rows)
                        df_errors.to_csv(bad_path, header=None, index=None, sep=' ', mode='a')

        elif operation == 'D':
                #print(input_query)
                cursor.execute(input_query)
                db.commit()

        else:
                db.close()
                print('Wrong operation given in table')
                sys.exit()

        db.close()


if __name__ == "__main__":
                identifier = sys.argv[1]
                indicator = int(sys.argv[2])
                input_query = ''
                if indicator == 1:
                        input_query = sys.argv[3]

                connection = get_connection_details(indicator,identifier,input_query)

                if connection['OPERATION'] == 'R':
                        if indicator == 1:
                                if len(sys.argv) == 5:
                                        connection['WRITE_PATH'] = sys.argv[4]
                                        main(connection)
                                else:
                                        main(connection)
                        elif indicator == 0:
                                if len(sys.argv) == 4:
                                        connection['WRITE_PATH'] = sys.argv[3]
                                        main(connection)
                                else:
                                        main(connection)

                elif connection['OPERATION'] == 'W':
                        if indicator == 1:
                                connection['READ_PATH'] = sys.argv[4]
                                if len(sys.argv) == 6:
                                        connection['LOG_PATH'] = sys.argv[5]
                                        main(connection)
                                elif len(sys.argv) == 7:
                                        connection['BAD_PATH'] = sys.argv[6]
                                        connection['LOG_PATH'] = sys.argv[5]
                                        main(connection)
                                else:
                                        main(connection)

                        elif indicator == 0:
				if 'refresh' in identifier.lower():
					if len(sys.argv) == 4:
						connection['LOG_PATH'] = sys.argv[3]
						main(connection)
					elif len(sys.argv) == 5:
						connection['BAD_PATH'] = sys.argv[5]
						connection['LOG_PATH'] = sys.argv[4]
						main(connection)
					else:
						main(connection)
									
				else:
					connection['READ_PATH'] = sys.argv[3]
					if len(sys.argv) == 5:
						connection['LOG_PATH'] = sys.argv[4]
						main(connection)
					elif len(sys.argv) == 6:
						connection['BAD_PATH'] = sys.argv[5]
						connection['LOG_PATH'] = sys.argv[4]
						main(connection)
					else:
						main(connection)

                elif connection['OPERATION'] == 'D':
                        main(connection)





# IDENTIFIER BOOLEAN
# BOOLEAN 0 => QUERY FROM TABLE; BOOLEAN 1 => QUERY DYNAMICALLY

# different calling option => R
# python app IDENTIFIER 1 "select * from XXX"
# python app IDENTIFIER 1 "select * from XXX" "/root/temp/test.out"

# python app IDENTIFIER 0
# python app IDENTIFIER 0 "/root/temp/test.out"


# different calling option => W
# python app IDENTIFIER 1 "select * from XXX" "file_path.csv'
# python app IDENTIFIER 1 "select * from XXX" "/root/temp/test.out" ".log" ".bad"

# python app IDENTIFIER 0 "file_path.csv'
# python app IDENTIFIER 0 "/root/temp/test.csv" ".log" ".bad"
