'''
Date: 05/18/2020
Author: Brandon Lin
Version: 0.01
Purpose: Web crawler for Weatherunderground data
Funtioning logic:
			1) define target webpage --> get_target_webpage (called by crawler)
			2) get interested data --> crawler
			3) check target data is valid then connect to SQL server
			4) assemble table name, header. 
			5) drop SQL table if existed, else create a new table (optional)
			6) upload to SQL
Change Log:
			- 
'''
import pymssql
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import calendar
import pandas as pd

def Build_SQL_connection(Database, server_address, user_account, user_password):
	try:
		conn = pymssql.connect(server=server_address, user=user_account, password=user_password, database=Database)
		cursor = conn.cursor()
		return conn, cursor
	except Exception:
		print('Error! Fail to connect to the SQL server')
		return 'Null', 'Null'		

def Build_SQL_table(conn, cursor, Table_Name, Table_Header):
	if conn != 'Null' and cursor != 'Null':
		# drop existing table, then create a new one. 
		try:
			cursor.execute('DROP TABLE IF EXISTS %s' %Table_Name)
		finally:
			cursor.execute('CREATE TABLE %s (%s)' %(Table_Name, Table_Header))
			# Table_Header is a string 'column1_name datatype1, column2_name dataype2, ...'
			conn.commit()

def Upload_to_SQL(conn, cursor, Database, Table_Name, Table_Header, target_data):
	if conn != 'Null' and cursor != 'Null':

		# Assemble value for the certain year
		value = ''
		List_of_keys = list(target_data.keys())
		for i in range(len(target_data[List_of_keys[0]])-1):
			value +=  "(\'%s\', \'%s\', \'%s\',\'%s\', \'%s\', \'%s\', \'%s\', \'%s\'),"\
			%(target_data[List_of_keys[0]][i+1], target_data[List_of_keys[1]][i+1], target_data[List_of_keys[2]][i+1], target_data[List_of_keys[3]][i+1],\
			  target_data[List_of_keys[4]][i+1], target_data[List_of_keys[5]][i+1], target_data[List_of_keys[6]][i+1], target_data[List_of_keys[7]][i+1])
		# print(value)

		# If match date, UPDATE data. If not match, INSERT data
		SQL_Merge_Query = 'MERGE [%s].[dbo].[%s] AS TRG\
						   USING (VALUES %s) AS SRC(Date, Time, Temperature, Speed, Gust, Pressure, Precip_Accum, Density_Alt)\
						   ON TRG.Date = SRC.Date\
						   AND TRG.Time = SRC.Time\
						   WHEN MATCHED THEN \
						   UPDATE SET TRG.Temperature = SRC.Temperature, TRG.Speed = SRC.Speed, TRG.Gust = SRC.Gust,\
						   TRG.Pressure = SRC.Pressure, TRG.Precip_Accum = SRC.Precip_Accum, TRG.Density_Alt = SRC.Density_Alt\
						   WHEN NOT MATCHED THEN\
						   INSERT (Date, Time, Temperature, Speed, Gust, Pressure, Precip_Accum, Density_Alt)\
						   VALUES (SRC.Date, SRC.Time, SRC.Temperature, SRC.Speed, SRC.Gust, SRC.Pressure, SRC.Precip_Accum, SRC.Density_Alt);'\
						   %(Database, Table_Name, value[:-1])

		try:
			print('Inserting data into table: [%s] in database: [%s] ... \n' %(Table_Name, Database))
			cursor.execute(SQL_Merge_Query)
			conn.commit()

		except Exception as e:
			print('SQL Update failed!')

		finally:
			conn.close()
	
	else:
		print('Connection failed!')

def get_target_webpage(year, month, day):
	headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
	           'Accept': 'text/html;q=0.9,*/*;q=0.8',
	           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
	           'Accept-Encoding': 'gzip',
	           'Connection': 'close'}

	requestURL 		= 'https://www.wunderground.com/dashboard/pws/KNVFERNL22/table/%s-%s-%s/%s-%s-%s/daily' %(year, month, day, year, month, day)
	target_webpage 	= requests.get(requestURL, headers=headers)

	return target_webpage

def crawler(year, month, day):
	# initialize data container
	target_data	= {'Date': ['date']}
	column_name = ['Date']

	# get webpage data
	target_webpage 	= get_target_webpage(year, month, day)

	if target_webpage.ok: 
		# Parse interested data
		soup = BeautifulSoup(target_webpage.text, 'html.parser')
		target_table = soup.find('table',{'class':"history-table desktop-table"}) 
		
		try:
			# get table column names
			headers = target_table.findAll('th')
			for col_head in headers:
				if col_head.text == 'Time':
					target_data[col_head.text] = ['time']
					column_name.append(col_head.text)

				if col_head.text == 'Precip. Accum.':
					target_data['Precip_Accum'] = ['decimal(18,2)']
					column_name.append('Precip_Accum')

				elif col_head.text == 'Temperature' or col_head.text == 'Speed' or col_head.text == 'Gust' or col_head.text == 'Pressure':
					target_data[col_head.text] = ['decimal(18,2)']
					column_name.append(col_head.text)
			
			# add DA column
			target_data['Density_Alt'] = ['decimal(18,2)']
			column_name.append('Density_Alt')

			# print(column_name) # debug
			# print(target_data) # debug

			rows = target_table.findAll('tr')
			# row_idx=0 # debug
			for row in rows:
				cols = row.findAll(['td', 'strong', 'span'])
				# row_idx += 1 # debug
				# print('\n', row_idx) # debug

				count = 0 # start a new row 
				get_time = True
				for col in cols:
					
					if col.name == 'strong' and get_time:
						target_data['Time'].append(col.text)
						target_data['Date'].append('%s/%s/%s' %(year,month,day))
						# target_data['Density_Alt'].append('')
						get_time = False # The first "strong" is time, the second would be wind direction
						# print('\n',col.text)

					if col.name == 'span' and (col['class'][0] == 'wu-value' or col['class'][0] == 'wu-unit-no-value'):
						count += 1
						# print(count, col.text)
						if count == 1 and col.text == '--':
							target_data['Temperature'].append('NULL')
						elif count == 1:
							target_data['Temperature'].append(col.text)

						if count == 4 and col.text == '--':
							target_data['Speed'].append('NULL')
						elif count == 4:
							target_data['Speed'].append(col.text)

						if count == 5 and col.text == '--':
							target_data['Gust'].append('NULL')
						elif count == 5:
							target_data['Gust'].append(col.text)
							
						if count == 6 and col.text == '--':
							target_data['Pressure'].append('NULL')
						elif count == 6:
							target_data['Pressure'].append(col.text)

						if count == 8 and col.text == '--':
							target_data['Precip_Accum'].append('NULL')
						elif count == 8:
							target_data['Precip_Accum'].append(col.text)

			# calculate density altitude				
			for idx in range(len(target_data['Time'])-1):		
				try:
					ISA_temp = (15. - (4397./1000.)*(2.)) # standard temp in C at 4347+50 ft elev.
					measured_Pressure 	= float(target_data['Pressure'][idx+1])
					measured_Temp_F2C 	= (float(target_data['Temperature'][idx+1])-32.)*5./9.
					PA = (29.92 - measured_Pressure)*1000 + 4300 # Pressure Altitude
					DA =  PA + 120*(measured_Temp_F2C - ISA_temp) # Density Altitude
					target_data['Density_Alt'].append('%.2f' %DA)
				except Exception:
					target_data['Density_Alt'].append('NULL')
							
			return target_data, column_name, True
		
		except Exception:
			return 0,0,0
		
def get_weather_data(Database, server_address, user_account, user_password, Table_Name, start_date, end_date, format_table):

	duration_days = (end_date - start_date).days

	for day in range(duration_days+1): #add today
		target_date = start_date + timedelta(days=day)
		# print(target_date.year, target_date.month, target_date.day)
		target_data, column_name, data_health = crawler(target_date.year, target_date.month, target_date.day)
		if data_health:
			conn, cursor = Build_SQL_connection(Database, server_address, user_account, user_password)

			Table_Header 	= ''
			for key in target_data.keys():
				Table_Header += key + ' ' + target_data[key][0] + ', '

			while format_table:
				Build_SQL_table(conn, cursor, Table_Name, Table_Header[:-2]) # drop the last two character (', ') in header string
				format_table = False # should only format the table once

			output = pd.DataFrame(target_data, columns = column_name)
			print(output)

			Upload_to_SQL(conn, cursor, Database, Table_Name, Table_Header, target_data)
			
		else:
			print('No Data on %s'%target_date)

def main():
	Database       = '' # database name
	server_address = '' # server address
	user_account   = '' # server account ID
	user_password  = '' # server password
	Table_Name     = 'Weather_data' # table name
	format_table   = True # change to False if saving data to an existing table
	start_date     = datetime.today() - timedelta(days = 1) # auto update or selecting an specific date, datetime(2018,5,14)
	end_date       = datetime.today() # auto update or selecting an specific date, datetime.datetime(2020,1,10) 

	get_weather_data(Database, server_address, user_account, user_password, Table_Name, start_date, end_date, format_table)


if __name__ == '__main__':
	main()
