import csv
import pandas as pd
import numpy as np
import time
import sys
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from datetime import date
import datetime as dat
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from airtable import Airtable
import pickle
import requests
import json
from pathlib import Path
from tqdm import tqdm

pd.set_option('display.max_columns', None) # display all columns
pd.set_option('display.max_rows', 50)
pd.set_option('display.min_rows', 30)

file = open("./hubspot_credentials.txt", "r")
contents = file.read()
hubspot_credentials = contents
file.close()

file = open("./airtable_credentials.txt", "r")
contents = file.read()
airtable_credentials = contents
file.close()

SPREADSHEET_ID_BONUSES = '1iEzTES0Ov9Gk6zJXtV8X61cWsu9v4cCVy6W-8P063Wc'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
def get_gsheet_creds():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'gsheet_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

'''
creds = get_gsheet_creds()
service = build('sheets', 'v4', credentials=creds)

def upload_data(sheet_name, spreadsheet, data):
    range = sheet_name + '!A:Z'
    request = service.spreadsheets().values().clear(spreadsheetId=spreadsheet, range=range, body={})
    response = request.execute()
    values = data.values.tolist()
    values.insert(0, data.columns.values.tolist())
    body = {
        'values': values
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet, range=range,
        valueInputOption='USER_ENTERED', body=body).execute()
    print(range + ': {0} cells updated.'.format(result.get('updatedCells')))
'''

# Get all line_items' field names
url = "https://api.hubapi.com/crm/v3/properties/line_items"
querystring = {"archived":"false","hapikey":hubspot_credentials}
headers = {'accept': 'application/json'}
response = requests.request("GET", url, headers=headers, params=querystring)
lineitems_fields_list = pd.json_normalize(json.loads(response.text)['results']).name.values.tolist()
lineitems_fields = '&properties=' + '&properties='.join(lineitems_fields_list)

# Get all engagements' field names
url = "https://api.hubapi.com/crm/v3/properties/engagements"
response = requests.request("GET", url, headers=headers, params=querystring)
engagements_fields_list = pd.json_normalize(json.loads(response.text)['results']).name.values.tolist()
engagements_fields = ','.join(engagements_fields_list)

# Get all deals' field names
url = "https://api.hubapi.com/crm/v3/properties/deals"
response = requests.request("GET", url, headers=headers, params=querystring)
deal_fields_list = pd.json_normalize(json.loads(response.text)['results']).name.values.tolist()
deal_fields = ','.join(deal_fields_list)

# Get all deals' field names
url = "https://api.hubapi.com/crm/v3/properties/companies"
response = requests.request("GET", url, headers=headers, params=querystring)
companies_fields_list = pd.json_normalize(json.loads(response.text)['results']).name.values.tolist()
companies_fields = '&properties='.join(companies_fields_list)

# Deal Owner Mapping
url = "https://api.hubapi.com/owners/v2/owners?hapikey=" + hubspot_credentials + "&includeInactive=true"
r = requests.get(url = url)
df_owners = pd.json_normalize(json.loads(r.text))
df_owners['name'] = df_owners['firstName'] + ' ' + df_owners['lastName']
deal_owner_mapping = dict(zip(df_owners.ownerId, df_owners.name))
deal_owner_mapping_name_to_id = {v:k for k,v in deal_owner_mapping.items()}
deal_owner_mapping = {str(k):v for k,v in deal_owner_mapping.items()}

one_month_ms = 31 * 24 * 60 * 60 * 1000
one_month_ago_ms = (time.time() * 1000) - one_month_ms

def remove_fields(df):
    df.columns = list(map(lambda x: str.replace(x, 'fields.', ''), df.columns.values))
    return df

# Get all companies ###########################################################
start = time.time()
offset = 0
hasMore = True
df_companies = pd.DataFrame()
print('Retrieving all companies.')
companies_list = []
while(hasMore):
    retries = 0
    while(True):
        retries += 1
        try:
            url = 'https://api.hubapi.com/companies/v2/companies/paged?hapikey=' + hubspot_credentials + '&properties=' + companies_fields + '&limit=100&offset=' + str(offset)
            response = requests.request("GET", url).text
            response = json.loads(response)
            curr_engagements = pd.json_normalize(response['companies'])
            companies_list += response['companies']
            offset = response['offset']
            hasMore = response['has-more']
            print('Retrieved', len(companies_list), 'offset:', offset, end='.\r')
            break
        except Exception:
            time.sleep(2.0)
            print(sys.exc_info(), retries)
            if(retries > 3):
                raise ValueError

df_companies = pd.json_normalize(companies_list)
df_companies.reset_index(drop=True, inplace=True)

df_companies.rename(columns={'properties.name.value':'Name', 'companyId' : 'ID', 'properties.phone.value' : 'Phone'}, inplace=True)
df_companies = df_companies[['ID', 'Name', 'Phone']].copy()

# Get all Won deals ###########################################################
df_deals = pd.DataFrame()
url = 'https://api.hubapi.com/crm/v3/objects/deals/search?hapikey=' + hubspot_credentials
headers = {'Content-Type':'application/json'}
data = '''{ "archived": false, "limit": 100, "properties":''' + str(deal_fields_list).replace('\'', '\"') + ''',
    "filterGroups":[
      {
        "filters":[
                {
                    "propertyName": "dealstage",
                    "operator": "EQ",
                    "value": 1113267
                }
        ]
      },
      {
        "filters":[
                {
                    "propertyName": "dealstage",
                    "operator": "EQ",
                    "value": 3660710
                }
        ]
      }
    ]
}'''

after = 0
print('Retrieving deals')
while(after is not None):
    retries = 0
    while(True):
        retries += 1
        try:
            if type(after) != type(0):
                data = '''{
                    "after":''' + str(after) + ''', "archived": false, "limit": 100,
                    "properties":''' + str(deal_fields_list).replace('\'', '\"') + ''',
                    "filterGroups":[
                      {
                        "filters":[
                                {
                                    "propertyName": "dealstage",
                                    "operator": "EQ",
                                    "value": 1113267
                                }
                        ]
                      },
                      {
                        "filters":[
                                {
                                    "propertyName": "dealstage",
                                    "operator": "EQ",
                                    "value": 3660710
                                }
                        ]
                      }
                    ]
                }'''
            response = json.loads(requests.request("POST", url, headers=headers, data=data).text)
            deals = pd.json_normalize(response['results'])
            df_deals = df_deals.append(deals)
            after = response.get('paging', {'next':None})['next']
            if after is not None:
                after = after.get('after', None)
            print('Retrieved', len(df_deals), 'deals:', pd.to_datetime(deals['properties.policy_effective_date']).min(), end='\r')
            break
        except Exception:
            # Make sure we get all deals
            print(sys.exc_info())
            print('failed, retries', retries)
            if(retries > 3):
                raise ValueError
print('\nRetrieved', len(df_deals), 'deals, max effective date:', pd.to_datetime(deals['properties.policy_effective_date']).max())
df_deals.reset_index(drop=True, inplace=True)

################################################################################

deal_stage_mapping = pd.DataFrame()
pipeline_mapping = dict()

for idx, row in pd.read_json('./dealPipelines.json').iterrows():
    deal_stage_mapping = deal_stage_mapping.append(pd.DataFrame(row['stages']))
    pipeline_mapping[row['id']] = row['label']

deal_stage_mapping = dict(zip(deal_stage_mapping.id, deal_stage_mapping.label))

df_deals['properties.hubspot_owner_id'] = df_deals['properties.hubspot_owner_id'].map(lambda x: deal_owner_mapping.get(x, x))
df_deals['properties.pipeline'] = df_deals['properties.pipeline'].map(lambda x: pipeline_mapping.get(x, x))
df_deals['properties.dealstage'] = df_deals['properties.dealstage'].map(lambda x: deal_stage_mapping.get(x, x))

deal_stage_mapping = {int(k):v for k,v in deal_stage_mapping.items()}
unmapped_columns = df_deals.columns
def map_headers(x):
    if any(str(stage_id) in x for stage_id in list(deal_stage_mapping.keys())) and 'entered' in x:
        new_id = x.split('_')
        new_id[len(new_id) - 1] = deal_stage_mapping[int(new_id[len(new_id) - 1])]
        return '_'.join(new_id[1:])
    else:
        return x
df_deals.columns = unmapped_columns.map(lambda x: map_headers(x))

deals_columns = ['properties.hs_object_id', 'properties.policy_effective_date',
                 'properties.dealstage', 'properties.amount', 'properties.hs_priority',
                 'properties.hubspot_owner_id', 'properties.dealname', 'properties.competitors_premium',
                 'properties.wc_new_governing_class_code','properties.wc_governing_class_description',
                 'properties.current_status', 'properties.next_step', 'properties.carriers_presented',
                 'properties.closing_status', 'properties.target_closing', 'properties.carriers_quote_won',
                 'properties.closedate', 'properties.wcirb_id']

df_deals.loc[:, 'properties.closedate'] = pd.to_datetime(df_deals['properties.closedate']).dt.strftime('%m/%d/%Y').fillna('')

df_deals = df_deals[deals_columns]

df_deals['URL'] = df_deals['properties.hs_object_id'].apply(lambda x: 'https://app.hubspot.com/contacts/6612994/deal/' + str(x) + '/')

certainty_mapping = {
    'In Play' : 'In Play (50%)',
    'Committed' : 'Committed (90%)',
    'At Risk' : 'At Risk (25%)'
}

df_deals.loc[:, 'properties.amount'] = df_deals['properties.amount'].fillna('').replace('', '0').astype(float)
df_deals.loc[:, 'properties.closing_status'] = df_deals['properties.closing_status'].apply(lambda x: certainty_mapping.get(x, x))

df_deals.rename(columns={'properties.dealname' : 'Name',
                         'properties.hs_object_id' : 'Deal ID',
                         'properties.policy_effective_date' : 'Effective Date',
                         'properties.hubspot_owner_id' : 'Owner',
                         'properties.carriers_quote_won' : 'Carrier Won',
                         'properties.amount' : 'Amount',
                         'properties.dealstage' : 'Stage',
                         'properties.closedate' : 'Close Date',
                         'properties.wcirb_id' : 'Bureau ID'}, inplace=True)

df_deals = df_deals[['Deal ID', 'Effective Date', 'Stage',
                     'Amount', 'Owner', 'Name', 'Carrier Won',
                     'Close Date', 'URL', 'Bureau ID']].copy()

df_deals.fillna('', inplace=True)
df_deals.to_csv('won_deals.csv', index=False)

airtable = Airtable('appm8xqiMIVvSz2FZ', 'Hubspot Won Deals', airtable_credentials)
old_data = pd.json_normalize(airtable.get_all(fields=['Deal ID', 'Effective Date', 'Stage',
                                    'Amount', 'Owner', 'Name', 'Carrier Won',
                                    'Close Date', 'URL', 'Bureau ID'])).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
old_data = remove_fields(old_data)
old_data = remove_fields(old_data)
old_data.loc[:, 'Close Date'] = pd.to_datetime(old_data['Close Date']).dt.strftime('%m/%d/%Y').fillna('')
old_data.fillna('', inplace=True)

to_insert = []
to_update = []

def check_fields(old_deal, new_deal):
    for col in new_deal.index:
        curr_new_field = new_deal[col]
        curr_old_field = old_deal[0][col]
        if(type(curr_new_field) == str):
            try:
                curr_old_field = curr_old_field.strip('\n').replace('\\', '')
            except:
                print(curr_old_field, col)
                print(curr_new_field, col)
                raise ValueError
        if(curr_new_field != curr_old_field):
            print('Update:', col, '| new:', new_deal[col], '| old:', old_deal[0][col])
            return True
    return False

for idx, new_deal in df_deals.iterrows():
    old_record = old_data[old_data['Deal ID'] == new_deal['Deal ID']]
    if(len(old_record) > 1):
        print('Multiple deals found.')
        raise ValueError
    elif(len(old_record) == 1):
        needs_update = check_fields(old_record.to_dict('records'), new_deal)
        if(needs_update):
            to_update.append({'id': old_record.head(1).id.item(), 'fields': new_deal.to_dict()})
    elif(len(old_record) == 0):
        to_insert.append(new_deal.to_dict())
    else:
        print('Corner')
        raise ValueError

records_to_update = pd.DataFrame(to_update).to_dict('records')
records_to_insert = pd.DataFrame(to_insert).to_dict('records')

to_delete = []
new_deal_ids = df_deals['Deal ID'].unique()
for idx, row in old_data.iterrows():
    if(row['Deal ID'] not in new_deal_ids):
        to_delete.append(row.id)

if(len(to_delete) > 0):
    airtable.batch_delete(to_delete)

print('Deleted', len(to_delete), 'deals.')

if(len(records_to_insert) > 0):
    airtable.batch_insert(records_to_insert, typecast=True)

print('Inserted', len(records_to_insert), 'deals.')

if(len(records_to_update) > 0):
    airtable.batch_update(records_to_update, typecast=True)

print('Updated', len(records_to_update), 'deals.')
