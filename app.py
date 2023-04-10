''''
A app to upload a csv file and query Salesforce for matching records.
'''
# q:review the code and optimze

from flask import Flask, render_template, request, send_file
from flask_uploads import UploadSet, configure_uploads, DATA
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceLogin
import os
import openpyxl
import csv_templates
import phonenumbers
import requests
import encodings
import sqlite3
from pandas import io
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///SQL\Main.db'
db = SQLAlchemy(app)

# Configure file uploads
files = UploadSet('files', extensions=('csv', 'xls', 'xlsx'))
app.config['UPLOADED_FILES_DEST'] = 'uploads'
configure_uploads(app, files)

# Use environment variables for Salesforce credentials
SF_USERNAME = os.environ.get('akshar.dharwadkar@webware.io')
SF_PASSWORD = os.environ.get('Akshar@1995')
SF_SECURITY_TOKEN = os.environ.get('nAIj2ETyLBP6zouVa9bQqqd06')

df_rq = pd.DataFrame()
df_seg = pd.DataFrame()
df_user = pd.DataFrame()
Lead_phone = pd.DataFrame()
Lead_email = pd.DataFrame()
Acc_Phone = pd.DataFrame()
Acc_Email = pd.DataFrame()
opp_phone = pd.DataFrame()
opp_email = pd.DataFrame()
opp_acc = pd.DataFrame()
df_Colunm  = pd.read_csv('csv_templates\Column_map - AE.csv',header=None,index_col=0)
df_Priority = pd.read_csv('csv_templates\Priority .csv', header=None, index_col=0)

# Define routes
@app.route('/')
def index():
    return render_template('home.html')

def format_phone_number(phone_number):
    try:
        parsed_number = phonenumbers.parse(phone_number, "CA")
        formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.NATIONAL)
        formatted_number = formatted_number.replace(" ", "").replace("-", "")
        formatted_number = formatted_number[:4] + ") " + formatted_number[5:8] + "-" + formatted_number[8:]
        return formatted_number
    except phonenumbers.phonenumberutil.NumberParseException:
        return phone_number

@app.route('/query_salesforce', methods=['POST'])
def query_salesforce():
    global df_seg
    global df_user
    global Lead_phone
    global Lead_email
    global Acc_Phone
    global Acc_Email
    global opp_phone
    global opp_email
    global opp_acc
    try:
        # Salesforce authentication
        session_id, instance = SalesforceLogin(
            username=SF_USERNAME,
            password=SF_PASSWORD,
            security_token=SF_SECURITY_TOKEN,
            domain='login'
        )
        Sf = Salesforce(instance=instance, session_id=session_id)

        # Query Salesforce and load results into Pandas DataFrame
        query_result = Sf.query_all("Select Phone,Email,PODIO_LEAD_UNIQUE_ID__C,Id,IsDeleted FROM Lead ")
        dfq = pd.DataFrame(query_result['records'])
        dfq.drop(dfq[dfq['IsDeleted'] == True ].index, axis=0, inplace=True)
        dfq.drop(columns=['attributes','IsDeleted'],inplace=True)
        Lead_phone_1 = dfq[['Phone','Id']]
        Lead_phone_1.loc[:, 'Phone'] = Lead_phone_1['Phone'].apply(lambda x: format_phone_number(str(x)))
        Lead_email_1 = dfq[['Email','Id']]
        Lead_phone = pd.concat([Lead_phone, Lead_phone_1], ignore_index=True)
        Lead_email = pd.concat([Lead_email, Lead_email_1], ignore_index=True)

        query_result_acc = Sf.query_all("Select Phone,Email__c,Podio_Lead_Unique_id__c,Id,IsDeleted FROM Account")
        df2 = pd.DataFrame(query_result_acc['records'])
        df2.drop(df2[df2['IsDeleted'] == True ].index, axis=0, inplace=True)
        df2.drop(columns=['attributes','IsDeleted'],inplace=True)
        Acc_Phone_1 = df2[['Phone','Id']]
        Acc_Phone_1.iloc[:,'Phone'] = Acc_Phone_1['Phone'].apply(lambda x: format_phone_number(str(x)))
        Acc_Email_1 = df2[['Email__c','Id']]
        Acc_Phone = pd.concat([Acc_Phone, Acc_Phone_1], ignore_index=True)
        Acc_Email = pd.concat([Acc_Email, Acc_Email_1], ignore_index=True)

        oppac_quarry = Sf.query_all("Select Phone__c,Email__c,AccountId,IsDeleted,Id FROM Opportunity")
        oppac  = pd.DataFrame(oppac_quarry ['records'])
        oppac.drop(oppac[oppac['IsDeleted'] == True ].index, axis=0, inplace=True)
        oppac.drop(columns=['attributes','IsDeleted'],inplace=True)
        opp_phone_1 = oppac[['Phone__c','Id']]
        opp_phone_1.iloc[:, 'Phone__c'] = opp_phone_1['Phone__c'].apply(lambda x: format_phone_number(str(x)))
        opp_email_1 = oppac[['Email__c','Id']]
        opp_acc_1 = oppac[['AccountId','Id']]
        opp_phone = pd.concat([opp_phone, opp_phone_1], ignore_index=True)
        opp_email = pd.concat([opp_email, opp_email_1], ignore_index=True)
        opp_acc = pd.concat([opp_acc, opp_acc_1], ignore_index=True)

        Segments = Sf.query_all("Select Name, Salesforce_Segment_Id__c  FROM Segment__c ")
        DFseg = pd.DataFrame(Segments['records'])
        User = Sf.query("Select Email, Id FROM User ")
        Userdf = pd.DataFrame(User['records'])
        df_seg = pd.concat([df_seg, DFseg], ignore_index=True)
        df_user = pd.concat([df_user, Userdf], ignore_index=True)

        # Get the plain text version of the DataFrame
        result_text = 'Data loaded sucessful'

        return render_template('home.html', result=result_text)
    except Exception as e:
        return render_template('home.html', error=str(e))

def clean_data():
    global df_seg
    global df_user
    global Lead_phone
    global Lead_email
    global Acc_Phone
    global Acc_Email
    global opp_phone
    global opp_email
    global opp_acc
    global df_Colunm
    global df_Priority
    d = dict(zip(df_seg['Name'].tolist(), df_seg['Salesforce_Segment_Id__c'].tolist()))
    y = dict(zip(df_user['Email'].tolist(), df_user['Id'].tolist()))
    c = df_Colunm.to_dict()
    P = df_Priority.to_dict()
    z = list(c.keys())
    d.update({'Film Production/Rental companies': 'a055e0000012i6cAAA'})
    L_phone = dict(zip(Lead_phone['Phone'].tolist(), Lead_phone['Id'].tolist()))
    L_email = dict(zip(Lead_email['Email'].tolist(), Lead_email['Id'].tolist()))
    Ac_phone = dict(zip(Acc_Phone['Phone'].tolist(), Acc_Phone['Id'].tolist()))
    Ac_email = dict(zip(Acc_Email['Email__c'].tolist(), Acc_Email['Id'].tolist()))
    Op_phone = dict(zip(opp_phone['Phone__c'].tolist(), opp_phone['Id'].tolist()))
    Op_email = dict(zip(opp_email['Email__c'].tolist(), opp_email['Id'].tolist()))
    Op_Ac = dict(zip(opp_acc['AccountId'].tolist(), opp_acc['Id'].tolist()))
    df_rq['Email'] = df_rq['Email'].str.split(';').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split(';').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('/').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('x').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('ext.').str[0]
    df_rq['Email'].fillna(df_rq['Email_1'], inplace=True)
    df_rq['Email'].fillna(df_rq['Email_2'], inplace=True)
    df_rq['Email'].fillna(df_rq['Email_3'], inplace=True)
    df_rq['LastName'].fillna(df_rq['LastName_1'], inplace=True)
    df_rq['LastName'].fillna(df_rq['LastName_2'], inplace=True)
    df_rq['LastName'].fillna(df_rq['Company'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_1'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_2'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_3'], inplace=True)
    df_rq['Phone'] = df_rq['Phone'].str.split(';').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('/').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('x').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split('ext.').str[0]
    df_rq['Phone'] = df_rq['Phone'].str.split(',').str[0]
    df_rq['Phone'] = df_rq['Phone'].apply(lambda x: format_phone_number(str(x)))
    df_rq['Yelp_Linkedin_Other__c'] = df_rq['Yelp_Linkedin_Other__c'].str.split(';').str[0]
    df_rq.loc[df_rq["Lead_Source__c"] == "Cold Non-Pro", "Sales_Channel__c"] = 'Inside Sales'
    df_rq.loc[df_rq["Lead_Source__c"] == "Cold Pro", "Sales_Channel__c"] = 'Webware Pro'
    df_rq.loc[df_rq["Sales_Channel__c"] == "Webware Pro", "Lead_Source__c"] = 'Cold Pro'
    df_rq.loc[df_rq["Sales_Channel__c"] == "Inside Sales", "Lead_Source__c"] = 'Cold Non-Pro'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Voicemail 1", "Lead_Stage__c"] = 'Voicemail'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Voicemail 2", "Lead_Stage__c"] = 'Voicemail'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Voicemail 3", "Lead_Stage__c"] = 'Voicemail'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Voicemail 4", "Lead_Stage__c"] = 'Voicemail'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Attempt 1", "Lead_Stage__c"] = 'Attempted'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Attempt 2", "Lead_Stage__c"] = 'Attempted'
    df_rq.loc[df_rq["Lead_Stage__c"] == "Attempt 3", "Lead_Stage__c"] = 'Attempted'
    df_rq.loc[df_rq["Time_Zone__c"] == "Others", "Time_Zone__c"] = 'Other'
    df_rq['Country__c'] = df_rq["Podio_Lead_Unique_id__c"].str[0:3]
    df_rq.loc[df_rq['Country__c']== 'USA','Country__c'] = 'USA'
    df_rq.loc[df_rq['Country__c']== 'CAN','Country__c'] = 'CANADA'
    df_rq.loc[(pd.isnull(df_rq['LastName'])), 'LastName'] = df_rq['FirstName']
    df_rq.loc[(pd.isnull(df_rq['LastName'])), 'LastName'] = df_rq['Company']
    df_rq.loc[df_rq['FirstName']== df_rq['LastName'],'FirstName'] = ' '
    df_rq.drop(df_rq[df_rq['Lead_Stage__c'] == 'Duplicate Lead' ].index, axis=0, inplace=True)
    df_rq['Segment__c'] = df_rq['Segment_text__c'].map(d)
    df_rq['OwnerId'] = df_rq['Lead_Owner_Email__c'].map(y)
    df_rq['Priority__c'] = df_rq['Sales_Channel__c'].map(P)
    df_rq['Ac_Id_phone']= df_rq['Phone'].map(Ac_phone)
    df_rq['AcIId_email']= df_rq['Email'].map(Ac_email)
    df_rq['L_Id_phone']= df_rq['Phone'].map(L_phone)
    df_rq['L_Id_email']= df_rq['Email'].map(L_email)
    df_rq['Op_Id_phone']= df_rq['Phone'].map(Op_phone)
    df_rq['Op_Id_email']= df_rq['Email'].map(Op_email)
    df_rq['Op_Id_email_ac'] = df_rq['AcIId_email'].map(Op_Ac)
    df_rq['Op_Id_phone_ac'] = df_rq['Ac_Id_phone'].map(Op_Ac)


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    global df_rq
    if request.method == 'POST' and 'file' in request.files:
        # Load uploaded file into Pandas DataFrame
        file = request.files['file']
        filename = files.save(file)
        file_path = os.path.join(app.config['UPLOADED_FILES_DEST'], filename)
        extension = os.path.splitext(filename)[1]
        if extension == '.csv':
            df = pd.read_csv(file_path)
        elif extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            return render_template('home.html', error='Unsupported file format. Please upload a CSV or Excel file.')
        # Connect to database and insert DataFrame
        conn = sqlite3.connect('Main.db')
        try:
            df.to_sql('mytable', conn, if_exists='replace')
        except sqlite3.OperationalError as e:
            if str(e).startswith('duplicate column name:'):
                print('Error: Duplicate column names encountered')
            else:
                raise e
        # Close connection and return success message
        conn.close()

        # Save DataFrame to file and allow user to download it
        output_file = os.path.join(app.config['UPLOADED_FILES_DEST'], 'output.csv')
    return render_template('home.html')

# Connect to database
conn = sqlite3.connect('Main.db')
table_name = 'mytable'
# Define function to check if table exists
def table_exists(table_name):
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    result = conn.execute(query).fetchall()
    return len(result) > 0

# Check if table exists
if table_exists('mytable'):
    print('mytable exists')
else:
    print('mytable does not exist')

# Close connection
conn.close()

if __name__ == '__main__':
    app.run(debug=True)

