import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
from simple_salesforce import Salesforce
from simple_salesforce import SalesforceLogin
import os
import phonenumbers
import requests
import encodings
import concurrent.futures

Ac_phone, Ac_email = {}, {}
L_phone, L_email = {}, {}
op_email, op_phone, op_ac = {}, {}, {}
d, y = {}, {}

session_id, instance = SalesforceLogin(
                                        username='akshar.dharwadkar@webware.io',
                                        password='Akshar@1995',
                                        security_token='nAIj2ETyLBP6zouVa9bQqqd06',
                                        domain='login'
                                        )

Sf = Salesforce(instance=instance, session_id=session_id)

def format_phone_number(phone_number):
    try:
        parsed_number = phonenumbers.parse(phone_number, "CA")
        formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.NATIONAL)
        formatted_number = formatted_number.replace(" ", "").replace("-", "")
        formatted_number = formatted_number[:4] + ") " + formatted_number[5:8] + "-" + formatted_number[8:]
        return formatted_number
    except phonenumbers.phonenumberutil.NumberParseException:
        return phone_number


def import_leads():
    global L_phone, L_email
    query_result = Sf.query_all("Select Phone,Email,PODIO_LEAD_UNIQUE_ID__C,Id,IsDeleted FROM Lead ")
    dfq = pd.DataFrame(query_result['records'])
    dfq.drop(dfq[dfq['IsDeleted'] == True ].index, axis=0, inplace=True)
    dfq.drop(columns=['attributes','IsDeleted'],inplace=True)
    Lead_phone = dfq[['Phone','Id']]
    Lead_email = dfq[['Email','Id']]
    Lead_phone.loc[:, 'Phone'] = Lead_phone['Phone'].apply(lambda x: format_phone_number(str(x)))
    L_p = dict(zip(Lead_phone['Phone'].tolist(), Lead_phone['Id'].tolist()))
    L_e = dict(zip(Lead_email['Email'].tolist(), Lead_email['Id'].tolist()))
    L_phone.update(L_p)
    L_email.update(L_e)


def import_Account():
    global Ac_phone, Ac_email
    query_result_acc = Sf.query_all("Select Phone,Email__c,Podio_Lead_Unique_id__c,Id,IsDeleted FROM Account")
    df2 = pd.DataFrame(query_result_acc['records'])
    df2.drop(df2[df2['IsDeleted'] == True ].index, axis=0, inplace=True)
    df2.drop(columns=['attributes','IsDeleted'],inplace=True)
    Acc_Phone = df2[['Phone','Id']]
    Acc_Phone.loc[:, 'Phone'] = Acc_Phone['Phone'].apply(lambda x: format_phone_number(str(x)))
    Acc_Email = df2[['Email__c','Id']]
    Ac_p = dict(zip(Acc_Phone['Phone'].tolist(), Acc_Phone['Id'].tolist()))
    Ac_e = dict(zip(Acc_Email['Email__c'].tolist(), Acc_Email['Id'].tolist()))
    Ac_phone.update(Ac_p)
    Ac_email.update(Ac_e)

def import_opp():
    global op_phone, op_email, op_acc
    oppac_quarry = Sf.query_all("Select Phone__c,Email__c,AccountId,IsDeleted,Id FROM Opportunity")
    oppac  = pd.DataFrame(oppac_quarry ['records'])
    oppac.drop(oppac[oppac['IsDeleted'] == True ].index, axis=0, inplace=True)
    oppac.drop(columns=['attributes','IsDeleted'],inplace=True)
    opp_phone = oppac[['Phone__c','Id']]
    opp_phone.iloc[:, 'Phone__c'] = opp_phone['Phone__c'].apply(lambda x: format_phone_number(str(x)))
    opp_email = oppac[['Email__c','Id']]
    opp_acc = oppac[['AccountId','Id']]
    Op_p = dict(zip(opp_phone['Phone__c'].tolist(), opp_phone['Id'].tolist()))
    Op_e = dict(zip(opp_email['Email__c'].tolist(), opp_email['Id'].tolist()))
    Op_A = dict(zip(opp_acc['AccountId'].tolist(), opp_acc['Id'].tolist()))
    op_phone.update(Op_p)
    op_email.update(Op_e)
    op_ac.update(Op_A)

def import_Segments():
    global d
    Segments = Sf.query("Select Name, Salesforce_Segment_Id__c  FROM Segment__c ")
    DFseg = pd.DataFrame(Segments['records'])
    DFseg.drop(columns='attributes',inplace=True)
    s = dict(zip(DFseg['Name'].tolist(), DFseg['Salesforce_Segment_Id__c'].tolist()))
    d.update(s)

def import_User():
    global y
    User = Sf.query("Select Email, Id FROM User ")
    Userdf = pd.DataFrame(User['records'])
    Userdf.drop(columns='attributes',inplace=True)
    U = dict(zip(Userdf['Email'].tolist(), Userdf['Id'].tolist()))
    y.update(U)

with concurrent.futures.ThreadPoolExecutor() as executor:
    future1 = executor.submit(import_leads)
    future2 = executor.submit(import_Account)
    future3 = executor.submit(import_Segments)
    future4 = executor.submit(import_User)

# Wait for both functions to complete
concurrent.futures.wait([future1, future2,future3,future4])

Lead_stage_map = "csv_templates\Lead status.csv"

Colunm_map = 'csv_templates\\Column_map - AE.csv'

Priority = "csv_templates\\Priority .csv"


df_Colunm = pd.read_csv(Colunm_map, header=None, index_col=0, squeeze = True)
df_Priority = pd.read_csv(Priority, header=None, index_col=0, squeeze = True)
df_Lead_stage = pd.read_csv(Lead_stage_map, header=None, index_col=0, squeeze = True)

c = df_Colunm.to_dict()
P = df_Priority.to_dict()
a = df_Lead_stage.to_dict()

z = list(c.keys())
d.update({'Film Production/Rental companies': 'a055e0000012i6cAAA'})


df_data = pd.read_csv('csv_templates\data.csv')
df_data
df_rq = df_data.filter(z)
df_rq
df_rq.rename(columns=c,inplace=True)

try:
    df_rq['Email'].fillna(df_rq['Email_1'], inplace=True)
    df_rq['Email'].fillna(df_rq['Email_2'], inplace=True)
    df_rq['Email'].fillna(df_rq['Email_3'], inplace=True)
    df_rq['LastName'].fillna(df_rq['LastName_1'], inplace=True)
    df_rq['LastName'].fillna(df_rq['LastName_2'], inplace=True)
    df_rq['LastName'].fillna(df_rq['Company'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_1'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_2'], inplace=True)
    df_rq['Phone'].fillna(df_rq['Phone_3'], inplace=True)
except:
       pass
df_rq['Email'] = df_rq['Email'].str.split(';').str[0]
df_rq['Phone'] = df_rq['Phone'].str.split(';').str[0]
df_rq['Phone'] = df_rq['Phone'].str.split('/').str[0]
df_rq['Phone'] = df_rq['Phone'].str.split('x').str[0]
df_rq['Phone'] = df_rq['Phone'].str.split('ext.').str[0]
df_rq['Phone'] = df_rq['Phone'].str.split(',').str[0]
try:
    df_rq['Additional_Customer_Info_Important_Inf__c'] = df_rq[['Additional_Customer_Info_Important_Inf__c', 'Notes_M','Notes_V' ,'Notes_H', 'Phone_1', 'Phone_2', 'Phone_3','Email_1','Email_2','Email_3']].apply(lambda x: ','.join(x.astype(str)), axis=1)
except:
    pass
df_rq['Phone'] = df_rq['Phone'].apply(lambda x: format_phone_number(str(x)))
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
df_rq.loc[(pd.isnull(df_rq['LastName'])), 'LastName'] = df_rq['Company']
df_rq.drop(df_rq[df_rq['Lead_Stage__c'] == 'Duplicate Lead' ].index, axis=0, inplace=True)

df_rq['Segment__c'] = df_rq['Segment_text__c'].map(d)
df_rq['OwnerId'] = df_rq['Lead_Owner_Email__c'].map(y)
df_rq['Priority__c'] = df_rq['Sales_Channel__c'].map(P)
df_rq['Status'] = df_rq['Lead_Stage__c'].map(a)

df_rq['Ac_Id_phone']= df_rq['Phone'].map(Ac_phone)
df_rq['AcIId_email']= df_rq['Email'].map(Ac_email)
df_rq['L_Id_phone']= df_rq['Phone'].map(L_phone)
df_rq['L_Id_email']= df_rq['Email'].map(L_email)
df_rq['Op_Id_phone']= df_rq['Phone'].map(op_phone)
df_rq['Op_Id_email']= df_rq['Email'].map(op_email)
df_rq['Op_Id_email_ac'] = df_rq['AcIId_email'].map(op_ac)
df_rq['Op_Id_phone_ac'] = df_rq['Ac_Id_phone'].map(op_ac)


Update_df = df_rq.loc[~((df_rq['Ac_Id_phone'].isnull()) & (df_rq['AcIId_email'].isnull() & df_rq['L_Id_phone'].isnull()) & (df_rq['L_Id_email'].isnull() & df_rq['Op_Id_phone'].isnull()) & (df_rq['Op_Id_email'].isnull()))]
insert_df = df_rq.loc[((df_rq['Ac_Id_phone'].isnull()) & (df_rq['AcIId_email'].isnull() & df_rq['L_Id_phone'].isnull()) & (df_rq['L_Id_email'].isnull() & df_rq['Op_Id_phone'].isnull()) & (df_rq['Op_Id_email'].isnull()) & (df_rq['Op_Id_email_ac'].isnull()) & (df_rq['Op_Id_phone_ac'].isnull()))]
insert_df.drop(columns=['Ac_Id_phone', 'AcIId_email', 'L_Id_phone', 'L_Id_email', 'Op_Id_phone', 'Op_Id_email','Op_Id_phone_ac','Op_Id_email_ac'],inplace=True)


Update_df.to_csv("E:\\Salesforce_auto\\Data_migraition\\production\\FlaskIntroduction-master\\Results\\update.csv",index = False)
insert_df.to_csv("E:\Salesforce_auto\Data_migraition\production\FlaskIntroduction-master\Results\Insert.csv",index = False)
