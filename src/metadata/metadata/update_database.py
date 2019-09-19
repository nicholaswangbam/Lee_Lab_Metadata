import mysql.connector
import re
from datetime import datetime
from collections import Counter
import xlrd
import csv
import os

from pathlib import Path


#dictionary to store certain phrases as integers (keep lowercase)
CODIFY = {'undetectable': '-111', 'na': 'NULL', 'missing': 'NULL', '/': 'NULL'}

#dictionary to store column names that refer to the same type of data, replace spaces with underscore
SAME_COLUMN = {'Viral_load': ['current_VL', 'viral_load_value', 'vl_stool', 'HIV_viral_load', 'Viral_Load_(copies/mL)', 'VL', 'vl', 'current VL'], 'age': ['Age_(yrs)', 'host_age', 'Age_at_stool_collection', 'Age (yrs)'],
'CD4_count': ['CD4 count', 'CD4_Tcell_counts', 'CD4_Absolute_Count_(cells/µL)', 'cd4_value', 'CD4', 'cd4', 'Current_CD4_count', 'cd4_time_of_stool'], 'ART_regimen': ['ART_which'], 'BMI': ['host_body_mass_index', 'bmi'],
'Region': ['geo_loc_name', 'distric_rc', 'geographic_location_country_and_or_sea', 'country_origin', 'Region', 'geographic_location'],
'Sample_ID': ['SRA_Sample', 'BiosampleID', '#SampleID', 'ID']}

#dictionary to store data values that have the same meaning but are represented differently across different studies (case sensitive, but keep data keys lowercase), include own header name at end of list
#format is dict = {database_header: ([alternative headers], {alternative data: translated data})}
SAME_VALUE = {'MSM_Status': (['Risk', 'risk', 'msm', 'host_RiskGroup', 'is_gay', 'HIV_RiskGroup', 'MSM_Status'], {'msm': 'Yes', 'msm activity': 'Yes', 'gay': 'Yes', '' 'else': 'No', 'unknown': 'NULL', '/': 'NULL'}),
'HIV_Status': (['hiv', 'HIV_serostatus', 'hivstatus', 'HIV+', 'HIVstatus', 'disease', 'host_HIVStatus', 'HIV_Status'], {'positive': 'Positive', 'positive id': 'Positive', 'si': 'Positive',
'hiv+': 'Positive', 'hivpos': 'Positive', '1': 'Positive', 'hivneg': 'Negative', 'hiv-': 'Negative', 'negative': 'Negative', 'no': 'Negative', '0': 'Negative'}),
'ART_Status': (['treatment', 'ARTuse', 'ARTyesno', 'ART_Status'], {'yes': 'Yes', 'treated': 'Yes', '1': 'Yes', 'positive': 'Yes', 'si': 'Yes', 'else': 'No'}),
'Sex': (['sex', 'gender', 'Gender', 'host_sex', 'Sex'], {'male': 'M', '1': 'M', '0': 'F', 'female': 'F', 'v': 'F', 'missing': 'NULL'})
}

STATIC_COLUMNS = 14 #rowID, Study_name, Sample_ID, Age, Race, Sex, BMI, Region, Viral_load, CD4_Count, MSM_status, ART_status, ART_regimen, HIV_status

#possible date formats
DATE_FORMATS = ['%m/%d/%y', '%m/%d/%Y', '%Y-%b-%d', '%Y-%m-%d', '%d-%b-%y', '%y-%b-%d',
							'%d-%b-%Y', '%m-%d-%y', '%Y', '%Y-%m-%d %H:%M', '%m/%d/%y %H:%M', '%Y-%m-%dT%H:%M:%SZ']


class UpdateDatabase:
	"""Class used when adding new data to and searching database"""
	def __init__(self, host, database_name, user, password, table_name):
		"""Connect to database and initialize formats/variables"""	
		self.table_name = table_name
		self.connection = None
		self.cursor = None
		self.datatypes = None #dictionary of headers to their datatypes
		try:
			#self.connection = mysql.connector.connect(host=host,
#											database=database_name,
#											user=user,
#											password=password,
#											charset='utf8mb4',
#											auth_plugin='mysql_native_password')
			self.connection = mysql.connector.connect(host=host,
											database=database_name,
											user=user,
											password=password)
			self.cursor = self.connection.cursor()				
		except Exception as e:
			print("Error while connection to database", e)

		self._getTableHeaders() #store existing table headers in self.existingHeaders, self.datatypes

		#hardcode possible date input formats
		self.dateFormats = DATE_FORMATS
		self.dateFormatDict = {}
		
		#set up dictionaries
		self.codify = CODIFY
		self.same_column = SAME_COLUMN
		self.same_value = SAME_VALUE
		self.reverse_same_column = {}
		for header in self.same_column:
			for col_name in self.same_column[header]:
				self.reverse_same_column[col_name] = header


	def _getTableHeaders(self):
		"""Collect headers which already exist in database"""
		self.cursor.execute("SHOW columns FROM {}".format(self.table_name))
		table_metadata = self.cursor.fetchall()
		self.existingHeaders = [column[0] for column in table_metadata]
		self.datatypes = dict(zip(self.existingHeaders, (col[1] for col in table_metadata)))


	def _determine_file_type(self, file):
		

		pos = file.tell()
		line = file.readline()
		file.seek(pos)
		for char in line:
			if char == '\t':
				return '\t'
			elif char == ',':
				return ','
				
		
		
		
	def add_excel(self, filename):
		"""Main method called to add data from excel into database by converting to txt."""
		# open the output csv
		filename_path = Path(filename)
		self.study_name = filename_path.name.split('-info')[0]
		
		with open('/var/www/temp.txt', 'w') as txt_file:
			# define a writer
			wr = csv.writer(txt_file, delimiter="\t")

			# open the xlsx file 
			book = xlrd.open_workbook(filename)
			# get a sheet
			mysheet = book.sheet_by_index(0)

			# write the rows
			for rownum in range(mysheet.nrows):
				row = mysheet.row_values(rownum)
				for colnum in range(0, len(row)):
					cell = mysheet.cell(rownum, colnum)
					if cell.ctype == 2:
						if str(row[colnum])[-2:] == '.0':
							row[colnum] = str(row[colnum])[:-2]
					elif cell.ctype == 3:
						date = xlrd.xldate_as_tuple(row[colnum], book.datemode)
						try:
							datetime_obj = datetime(*date)
							date = datetime_obj.strftime("%Y-%m-%d")
							row[colnum] = date
						except Exception:
							#invalid date
							row[colnum] = ''

						

				wr.writerow(row)
		

		self.add_txt('/var/www/temp.txt')
		os.remove('/var/www/temp.txt')


	def add_txt(self, filename):
		"""Main method called to add data from txt into database."""
		if filename != '/var/www/temp.txt':	
			filename_path = Path(filename)
			self.study_name = filename_path.name.split('-info')[0]
		
		with open(filename, 'r', encoding = 'ISO-8859-1') as file:
			delimiter = self._determine_file_type(file)

			header_line = file.readline()
			header_line = self._clean_tabs(header_line)
			header_list = re.split(delimiter, header_line)

			self._clean_header_list(header_list)			

			#add rest of lines
			cnt = 1

			
			for data_line in file:
				if data_line.isspace():
					continue


				data_line_fixed = self._clean_tabs(data_line)
				data_list = re.split(delimiter, data_line_fixed)
				self._clean_data_line(data_list)
				#do work needed after getting first line
				if cnt == 1:
					#determine datatypes for each column based on first row
					self._determine_datatypes(data_list, header_list)
					#add new columns to database if they don't exist under the same or a different name
					for index, header in enumerate(header_list):
						if header != ' ':	
							if header.lower() not in [exist_head.lower() for exist_head in self.existingHeaders]:
								add_col_SQL = "ALTER TABLE " + self.table_name + " ADD `" + header + "` " + self.datatypes[header_list[index]] + ";"
								print(add_col_SQL)
								result = self.cursor.execute(add_col_SQL)
								self.existingHeaders.append(header)
							else:
								#change case of header that exists in a different case in the database
								if header not in self.existingHeaders:
									header_list[index] = next((exist_head for exist_head in self.existingHeaders if header.lower() == exist_head.lower()), None)
					

					self._add_static_cols(header_list)
					

				
				cnt+=1
				self._insert_row(data_list, header_list)
			
			
			
			self._clean_empty_columns()	
			self.connection.commit()
			

	

	
	def _clean_header_list(self, header_list):
		"""Make any necessary changes to header list in order to smoothly add to database"""
		#parse trailing new line
		if '\n' in header_list[-1]:
			header_list[-1] = header_list[-1][:-1]
		
		for index, header in enumerate(header_list):
			#resolve known headers with the same data but different header names
			if ' ' in header:
				header_list[index] = header.replace(' ', '_')

			if header in self.reverse_same_column:
				header_list[index] = self.reverse_same_column[header]

		#handle duplicate headers
		header_appearances = Counter([header.lower() for header in header_list])
		realtime_header_count = {}
		for index, header in enumerate(header_list):
			#handle duplicate headers
			if header_appearances[header.lower()] > 1:
				if header.lower() in realtime_header_count:
					realtime_header_count[header.lower()] += 1
					header_list[index] = header + '_' + str(realtime_header_count[header.lower()])
				else:
					realtime_header_count[header.lower()] = 0

	def _add_static_cols(self, header_list):
		"""Add columns that may need to be checked in 'same_values'"""
		for i in range(STATIC_COLUMNS):
			if self.existingHeaders[i] == 'rowID':
				continue
			elif self.existingHeaders[i] not in header_list:
				header_list.append(self.existingHeaders[i])

	def _clean_tabs(self, line):
		"""Format empty data as a space in tab separated txt files"""
		line_fixed = line.replace('\t\t', '\t \t')
		previous = line
		while line_fixed != previous:
			previous = line_fixed
			line_fixed = line_fixed.replace('\t\t', '\t \t')
		return line_fixed

	def _determine_datatypes(self, first_row_list, header_list):
		"""Attempt to determine datatype of unknown column by looking at first row"""
		self.title_flag = 0
		
		for index, data in enumerate(header_list):
			lower_case_datatypes = [head.lower() for head in self.datatypes]
			if not data.lower() in lower_case_datatypes:	
				self.datatypes[header_list[index]] = self._check_datatype(first_row_list[index], index)
				

	def _clean_data_line(self, data_list):
		if '\n' in data_list[-1]:
			data_list[-1] = data_list[-1][:-1]

	def _check_datatype(self, data, index):
		"""Check different datatypes"""
		if data == " " or data == "" or data == None:
			return "varchar(1)"
		if data.lower() in self.codify:
			return "varchar(20)"
		elif self._check_if_int(data):
			return "int(9)"
		elif self._check_if_float(data):
			return "float"
		elif self._check_if_date(data, index):
			return "date"
		else:
			return "blob"

	def _check_if_int(self, data):
		if data[0] == '-':
			data = data[1:]
		if data.isdigit():
			return True
		return False

	def _check_if_float(self, data):
		if data[0] == '-':
			data = data[1:]
		if '.' in data:
			parts = data.split('.')
			if len(parts) == 2:
				if parts[0].isdigit() and parts[1].isdigit():
					return True
		return False

	def _check_if_date(self, data, index):

		if '/' in data or '-' in data:
			return self._match_date_form(data, index)
		return False
	
	def _match_date_form(self, data, index):
		objDate = ""
		for form in self.dateFormats:
			try:
				objDate = datetime.strptime(data, form) 
				self.dateFormatDict[index] = form
			except Exception:
				pass
			if objDate != "":
				return True
		return False

	

	def _insert_row(self, data_list, header_list):
		"""Insert parsed data from txt file into MySQL database"""
		#initialize SQL prefix
		insert_SQL_prefix = "INSERT INTO " + self.table_name + " ("
		for header in header_list:
			if header != ' ':	
				insert_SQL_prefix += '`'
				insert_SQL_prefix += header
				insert_SQL_prefix += "`, "
		insert_SQL_prefix = insert_SQL_prefix[:-2]
		insert_SQL_prefix += ") VALUES ("
		insert_SQL_string = insert_SQL_prefix

		same_value_col_dict = {}

		#determine SQL to execute
		for index, _header in enumerate(header_list):

			if header_list[index] == ' ':
				continue

			#temporary data for columns that pull data from another column
			if index < len(data_list):
				data = data_list[index]
			else:
				data = 'TEMPORARY DATA'

			#parse ending new line character
			if index == (len(data_list) - 1):
				if '\n' in data:
					data = data[:-1]

			if data == " " or data == "":
				data = "NULL"
			else:
				#handle study name column
				if header_list[index] == 'Study_Name':
					data = self.study_name


				#handle field that was previously all null
				if self.datatypes[header_list[index]] == "varchar(1)":		
					self._change_col_type(data, index, header_list)
				
				#handle field that was previously only something in self.codify
				elif self.datatypes[header_list[index]] == "varchar(20)" and data.lower() not in self.codify:
					
					datatype = self._check_datatype(data, index)
					if datatype == 'int(9)' or datatype == 'float':
						for replace in self.codify:
							update_SQL = "UPDATE " + self.table_name + " SET `" + header_list[index] + "` = " + self.codify[replace] + " WHERE `" + header_list[index] + "` = '" + replace + "'"
							try:
								self.cursor.execute(update_SQL)
							except Exception as e:
								raise Exception('%s \n SQLstring: %s' % (e, update_SQL))
								print("Error changing codify data")
												
					elif datatype == 'date':
						update_SQL = "UPDATE " + self.table_name + " SET `" + header_list[index] + "` = NULL"
						
						try:
							self.cursor.execute(update_SQL)
						except Exception as e:
							raise Exception('%s \n SQLstring: %s' % (e, update_SQL))
							print("Error changing codify data")
						
					elif datatype == 'blob':
						for replace in self.codify:
							if self.codify[replace] == 'NULL':
								update_SQL = "UPDATE " + self.table_name + " SET `" + header_list[index] + "` = NULL"
							
								try:
									self.cursor.execute(update_SQL)
								except Exception as e:
									raise Exception('%s \n SQLstring: %s' % (e, update_SQL))
									print("Error changing codify data")

					self._change_col_type(data, index, header_list)

				
				

				#handle same value data
				elif header_list[index] in self.same_value:
					for pot_header in self.same_value[header_list[index]][0]:
						if pot_header in header_list:
							if pot_header not in same_value_col_dict:
								for col_index, header in enumerate(header_list):
									if header == pot_header:
										same_value_col_dict[header] = col_index
										break
							same_value_dict = self.same_value[header_list[index]][1] #actual dictionary of values
							if same_value_col_dict[pot_header] < len(data_list):		
								same_data = data_list[same_value_col_dict[pot_header]]
							else:
								same_data = "NULL"

							if same_data != "NULL" and same_data != " " and same_data != "":

								if same_data.lower() in same_value_dict:
									data = same_value_dict[same_data.lower()]

								else:
									if 'else' in same_value_dict:
										data = same_value_dict['else']
									else:
										data = same_data
							else:
								data = "NULL"
							

							break

				if data == 'TEMPORARY DATA':
					data = 'NULL'
				
									
							

				#handle number type data
				if 'int' in self.datatypes[header_list[index]] or 'float' in self.datatypes[header_list[index]]:
					if data.isdigit():
						pass
					elif self._isfloat(data):
						pass
					elif data.lower() in self.codify:
						data = self.codify[data.lower()]
					elif header_list[index] == 'Viral_load': #handle special case of VL
						if '<' in data:
							data = self.codify['undetectable']
						else:
							data = "NULL"
					else:
						data = "NULL"
				
				#handle date type data
				elif self.datatypes[header_list[index]] == "date" and data != " ":
					#only year
					if data.isdigit():
						data = data + '-00-00'

					else:
						if index not in self.dateFormatDict:
							self._match_date_form(data, index)
						try:
							objDate = datetime.strptime(data, self.dateFormatDict[index])
							data = objDate.strftime('%Y-%m-%d')
						except Exception:
							data = "NULL"

				#handle case where a value in a blob column should be translated to null
				elif data.lower() in self.codify:
					if self.codify[data.lower()] == "NULL":
						data = "NULL"

				if '"' in data:
					data = data.replace('"', '')
			

			if data == "NULL" or 'int' in self.datatypes[header_list[index]] or 'float' in self.datatypes[header_list[index]]:
				add = " " + data + ", "
				insert_SQL_string += add
			else:
				add = ' "' + data + '", '
				insert_SQL_string += add
		insert_SQL_string = insert_SQL_string[:-2]
		insert_SQL_string += ");" 
		#print(insert_SQL_string)
		try:	
			self.cursor.execute(insert_SQL_string)
		except Exception as e:
			print("Error inserting data", e)
			raise Exception('%s \n SQLstring: %s' % (e, insert_SQL_string))
		
		


	def _change_col_type(self, data, index, header_list):
		datatype = self._check_datatype(data, index)
		change_datatype_SQL = ("ALTER TABLE " + self.table_name + " MODIFY COLUMN `" 
		+ header_list[index] + "` " + datatype + ";")
		self.datatypes[header_list[index]] = datatype
		try:
			self.cursor.execute(change_datatype_SQL)
		except Exception as e:
			raise Exception('%s \n SQLstring: %s' % (e, change_datatype_SQL))
			print("Error changing column type")
		

	def _isfloat(self, data):
		try:
			a = float(data)
		except ValueError:
			return False
		else:
			return True

	def _clean_empty_columns(self):
		for header in self.datatypes:
			if self.datatypes[header] == 'varchar(1)':
				delete_SQL = "ALTER TABLE " + self.table_name + " DROP COLUMN `" + header + "`;"
				print(delete_SQL)
				self.cursor.execute(delete_SQL)

	def create_format_dict(self):
		"""Method called to format search page"""
		format_list = {}
		for column in self.datatypes:
			format_list[column] = self._format_search(column)
		return format_list

	def _format_search(self, column_name):
		"""Determine how each column search input should be formatted."""
		if self.datatypes[column_name] == "int" or self.datatypes[column_name] == "int(9)" or self.datatypes[column_name] == "float":
			return self._format_num_search(column_name)
		elif self.datatypes[column_name] == "blob":
			return self._format_varchar_search(column_name)
		elif self.datatypes[column_name] == "date":
			return self._format_date_search(column_name)

	def _format_num_search(self, column_name):
		"""Return min and max of column"""
		SQLstring = "SELECT MIN(NULLIF(`" + column_name + "`, -111)), MAX(`" + column_name + "`) FROM " + self.table_name
		self.cursor.execute(SQLstring)
		result = self.cursor.fetchall()
		#for loop checks for possible values from codify
		for key in self.codify:
			SQLstring = "SELECT `" + column_name + "` FROM " + self.table_name + " WHERE `" + column_name +  "` = " + self.codify[key]
			self.cursor.execute(SQLstring)
			temp = self.cursor.fetchall()
			if temp:
				if len(result) == 3:
					result[2].append(key)
				else:
					result.append([key])
		if result[0][0] != None and result[0][1] != None:
			result = [(round(result[0][0], 2), round(result[0][1], 2))]
		
		return result
		
	def _format_varchar_search(self, column_name):
		"""If less than 5 distinct values in column, return list of values. Else return empty list"""
		static_dropdown = ['study_name', 'race', 'art_regimen', 'region']
		SQLstring = "SELECT DISTINCT `" + column_name + "` FROM " + self.table_name
		self.cursor.execute(SQLstring)
		column_data = [value[0] for value in self.cursor.fetchall()]
		if len(column_data) <= 10 or column_name.lower() in static_dropdown:
			if None in column_data:
				column_data.remove(None)
			if '' in column_data:
				column_data.remove('')

			return column_data
		else:
			return []

	def _format_date_search(self, column_name):
		"""Return earliest date and latest date of column"""
		SQLstring = "SELECT MIN(NULLIF(`" + column_name + "`, -111)), MAX(`" + column_name + "`) FROM " + self.table_name
		#for some reason none is returned sometimes
		date_cursor = self.connection.cursor(raw=True)
		date_cursor.execute(SQLstring)
		result = date_cursor.fetchall()[0]
		if result[0] != None:
			result = [result[0].decode(), result[1].decode()]
			#clean unknown values and make the last day of that month/year
			if result[0][-2:] == '00':
				if result[0][-5:] == '00-00':
					result[0] = result[0][:-5] + '12-31'
				else:
					result[0] = result[0][:-2] + '28'

			if result[1][-2:] == '00':
				if result[1][-5:] == '00-00':
					result[1] = result[1][:-5] + '12-31'
				else:
					result[1] = result[1][:-2] + '28'

		return result

	def search_database(self, criteria):
		"""Receives list of search criteria that must all be true. Returns list of lists."""

		#format SQL query string
		if criteria:
			SQLstring = "SELECT * FROM " + self.table_name + " WHERE "
			for search_field in criteria:
				SQLstring += '('
				for condition in search_field:
					SQLstring += condition[0]
					SQLstring += condition[1]
					SQLstring += " OR "
				SQLstring = SQLstring[:-4]
				SQLstring += ')'
				SQLstring += ' AND '
			SQLstring = SQLstring[:-5]
			SQLstring += ';'
		else:
			SQLstring = "SELECT * FROM " + self.table_name
		
		try:	
			self.cursor.execute(SQLstring)
		except mysql.connector.errors.ProgrammingError:
			print("Error in", SQLstring)
		result = self.cursor.fetchall()
		#print(result)
		return result

	

	def close(self):
		self.connection.close()
		
