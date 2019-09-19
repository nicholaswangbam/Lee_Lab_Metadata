import os
import sys

from metadata import app
from datetime import date
from flask import Flask, flash, request, redirect, url_for, render_template
from flask.json import JSONEncoder, jsonify
from metadata.update_database import UpdateDatabase
from werkzeug.utils import secure_filename

scriptdir=os.path.dirname(os.path.realpath(__file__))
sys.path.append(scriptdir+'/..')

MYSQL_HOST = '****'
MYSQL_USER = '****'
MYSQL_PASS = '****'
DATABASE_NAME = '****'
TABLE_NAME = '****'

#class to keep date in orginal format when converting data list to JSON
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


ALLOWED_EXTENSIONS = set(['txt', 'xls', 'xlsx', 'csv'])
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)),'temp_data_files')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
with open("/tmp/test","w") as f:
  print(UPLOAD_FOLDER,file=f)
  print("Path",os.path.dirname(os.path.realpath(__file__)),file=f)
  print(os.getcwd(),file=f)
  

app.json_encoder = CustomJSONEncoder

def clean_header(header):
	clean_header = header.capitalize()
	clean_header = clean_header.replace('_', ' ')
	return clean_header


def allowed_file(filename):
	return '.' in filename and \
		   filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_header_name(form_name):
	return "`" + form_name.split('_search')[0] + "`"

def format_search(form_values, codify_dict):
	"""Method to convert values from input into criteria list for UpdateDatabase search_database function"""
	search_criteria = []
	
	for field in form_values:
		
		field_list = []
		for value in form_values.getlist(field):
			if value == '':
				continue
			if field[-4:] == '_min':
				field_list.append((get_header_name(field), ' >= {}'.format(value)))
			elif field[-4:] == '_max':
				field_list.append((get_header_name(field), ' <= {}'.format(value)))
			elif field[-6:] == '_first':
				field_list.append((get_header_name(field), " >= '{}'".format(value)))
			elif field[-5:] == '_last':
				field_list.append((get_header_name(field), " <= '{}'".format(value)))
			elif field[-8:] == '_numText':
				field_list.append((get_header_name(field), " = {}".format(codify_dict[value])))
			else:
				field_list.append((get_header_name(field), ' = "{}"'.format(value)))
			
		if field_list:
			search_criteria.append(field_list)

	return search_criteria

@app.route('/', methods=['GET', 'POST'])
def homepage():
	if request.method == 'POST':
		print('in post')
		#Receive 'POST' call from homepage and use form data to search through UpdateDatabase object
		
		DatabaseObj = UpdateDatabase(MYSQL_HOST, DATABASE_NAME, MYSQL_USER, MYSQL_PASS, TABLE_NAME)
		search_criteria = format_search(request.form, DatabaseObj.codify)
		table_list = DatabaseObj.search_database(search_criteria)
		DatabaseObj.close()
		return jsonify(table_list)
	
	#initial 'GET' request that will load search criteria
	DatabaseObj = UpdateDatabase(MYSQL_HOST, DATABASE_NAME, MYSQL_USER, MYSQL_PASS, TABLE_NAME)
	headers_datatypes = DatabaseObj.datatypes
	format_input = DatabaseObj.create_format_dict()

	DatabaseObj.close()
	print(format_input)
	return render_template('index.html', headers_datatypes=headers_datatypes, format_input = format_input, codify = DatabaseObj.codify, clean_header = clean_header)
	
@app.route('/add_data', methods=['GET', 'POST'])
def add_data():
	error = ''
	if request.method == 'POST':
		# check if the post request has the file part
		if 'file' not in request.files:
			flash('No file part')
			return redirect(request.url)
		file = request.files['file']
		# if user does not select file, browser also
		# submit an empty part without filename
		if file.filename == '':
			flash('No selected file')
			return redirect(request.url)
		if file and allowed_file(file.filename):
			DatabaseObj = UpdateDatabase(MYSQL_HOST, DATABASE_NAME, MYSQL_USER, MYSQL_PASS, TABLE_NAME)
			filename = secure_filename(file.filename)
			filename = os.path.join(app.config['UPLOAD_FOLDER'], filename)
			file.save(filename)
			extension = file.filename.split('.')[1]
			if(extension == 'txt'):
				try:
					DatabaseObj.add_txt(filename)
				except Exception as e:
					print("ERROR: ", e)
					error = str(e)
				
				

			elif(extension == 'xls' or extension == 'xlsx'):
				DatabaseObj.add_excel(filename)
				
			os.remove(filename)
			DatabaseObj.close()
			
			return render_template('add_data.html',
									error=error)
		if not allowed_file(file.filename):
			flash('Unsupported file extension')
			return redirect(request.url)
	return render_template('add_data.html', error=error)
