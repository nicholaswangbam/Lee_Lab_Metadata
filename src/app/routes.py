from app import app
from datetime import date
from flask import Flask, flash, request, redirect, url_for, render_template
from flask.json import JSONEncoder, jsonify
import os
from .update_database import UpdateDatabase
from werkzeug.utils import secure_filename

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
UPLOAD_FOLDER = os.getcwd()
UPLOAD_FOLDER += '\\app\\temp_data_files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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
		if form_values[field] == '':
			continue
		if field[-4:] == '_min':
			search_criteria.append((get_header_name(field), ' >= {}'.format(form_values[field])))
		elif field[-4:] == '_max':
			search_criteria.append((get_header_name(field), ' <= {}'.format(form_values[field])))
		elif field[-6:] == '_first':
			search_criteria.append((get_header_name(field), " >= '{}'".format(form_values[field])))
		elif field[-5:] == '_last':
			search_criteria.append((get_header_name(field), " <= '{}'".format(form_values[field])))
		elif field[-8:] == '_numText':
			search_criteria.append((get_header_name(field), " = {}".format(codify_dict[form_values[field]])))
		else:
			search_criteria.append((get_header_name(field), ' = "{}"'.format(form_values[field])))
			#print(form_values[field])

	return search_criteria

@app.route('/', methods=['GET', 'POST'])
def homepage():
	if request.method == 'POST':
		#Receive 'POST' call from homepage and use form data to search through UpdateDatabase object
		DatabaseObj = UpdateDatabase("localhost", "data", "root", "root", "records")
		search_criteria = format_search(request.form, DatabaseObj.codify)
		#print(request.form)
		table_list = DatabaseObj.search_database(search_criteria)
		DatabaseObj.close()
		return jsonify(table_list)
	
	#initial 'GET' request that will load search criteria
	DatabaseObj = UpdateDatabase("localhost", "data", "root", "root", "records")
	headers_datatypes = DatabaseObj.datatypes
	format_input = DatabaseObj.create_format_dict()

	DatabaseObj.close()
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
			DatabaseObj = UpdateDatabase("localhost", "data", "root", "root", "records")
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