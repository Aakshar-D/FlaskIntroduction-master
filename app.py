''''
A app to upload a csv file and query Salesforce for matching records.
'''
from flask import Flask, render_template, request, send_from_directory
from flask_uploads import UploadSet, configure_uploads
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import os
import subprocess
from werkzeug.utils import secure_filename
import openpyxl

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///SQL\Main.db'
db = SQLAlchemy(app)

# Configure file uploads
files = UploadSet('files', extensions=('csv', 'xls', 'xlsx'))
app.config['UPLOADED_FILES_DEST'] = 'uploads'
configure_uploads(app, files)

@app.route('/')
def index():
    return render_template('Home.html')

@app.route('/upload_tempalet', methods=['POST'])
def upload_tempalet():
    if request.method == 'POST' and 'file' in request.files:
        # Load uploaded file into Pandas DataFrame
        file = request.files['file']
        filename = secure_filename(file.filename)
        extension = os.path.splitext(filename)[1]

        if extension == '.csv':
            df = pd.read_csv(file)
        elif extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file, engine='openpyxl')
        else:
            return render_template('Home.html', error='Unsupported file format. Please upload a CSV or Excel file.')
        df.to_csv('csv_templates\\Column_map - AE.csv', index=False)
        return render_template('home.html', success='File uploaded successfully.')
    return render_template('Home.html')


@app.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST' and 'file' in request.files:
        # Load uploaded file into Pandas DataFrame
        file = request.files['file']
        filename = secure_filename(file.filename)
        extension = os.path.splitext(filename)[1]

        if extension == '.csv':
            df = pd.read_csv(file)
        elif extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file, engine='openpyxl')
        else:
            return render_template('Home.html', error='Unsupported file format. Please upload a CSV or Excel file.')

        df.to_csv('csv_templates\data.csv', index=False)

        subprocess.call(['python','Demo_sych.py'], shell=False)
        subprocess.Popen(['python', 'Demo_sych.py'], bufsize=0)

        subprocess.call(['python3','Demo_sych.py'], shell=False)
        subprocess.Popen(['python3', 'Demo_sych.py'], bufsize=0)


        return render_template('Home.html', success='File uploaded successfully.')
    return render_template('Home.html')

@app.route('/Insert_file', methods=['GET', 'POST'])
def Insert_file():
    directory = 'Results'
    filename = 'Insert.csv'
    if request.method == 'POST':
        return send_from_directory(directory, filename, as_attachment=True)
    return render_template('Home.html')

@app.route('/Update_file', methods=['GET', 'POST'])
def Update_file():
    directory = 'Results'
    filename = 'Update.csv'
    if request.method == 'POST':
        return send_from_directory(directory, filename, as_attachment=True)
    return render_template('Home.html')

@app.route('/Data_loader', methods=['GET', 'POST'])
def Data_loader():
    return render_template('Data_loader.html')

@app.route('/upload_data', methods=['POST'])
def upload_data():
    if request.method == 'POST' and 'file' in request.files:
        # Load uploaded file into Pandas DataFrame
        file = request.files['file']
        filename = secure_filename(file.filename)
        extension = os.path.splitext(filename)[1]

        if extension == '.csv':
            df = pd.read_csv(file)
        elif extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file, engine='openpyxl')
        else:
            return render_template('Data_loader.html', error='Unsupported file format. Please upload a CSV or Excel file.')

        df.to_csv("csv_templates\Insert_data.csv", index=False)
        subprocess.call(['python','Bulk_import.py'], shell=False)
        subprocess.Popen(['python', 'Bulk_import.py'], bufsize=0)

        return render_template('Data_loader.html', success='File uploaded successfully.')
    return render_template('Data_loader.html')

@app.route('/Sucess_File', methods=['GET', 'POST'])
def Sucess_File():
    directory = 'Results'
    filename = 'Sucess.csv'
    if request.method == 'POST':
        return send_from_directory(directory, filename, as_attachment=True)
    return render_template('Data_loader.html')

@app.route('/Erorr_File', methods=['GET', 'POST'])
def Erorr_File():
    directory = 'Results'
    filename = 'Error.csv'
    if request.method == 'POST':
        return send_from_directory(directory, filename, as_attachment=True)
    return render_template('Data_loader.html')


if __name__ == '__main__':
    app.run(debug=True)

