from dateutil import relativedelta
from datetime import datetime
from datetime import timedelta
import pytz
import pandas as pd
import time
import sys
from airtable import Airtable
import requests
import json
import time

start = time.time()

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

df_activities_new = pd.read_pickle('C:\\Users\\shahb\\Documents\Glow\\lead-prospect-data-discovery\\deals_sales_report\\all_eng.pkl')

df_activities_new = df_activities_new[df_activities_new['engagement.type'] != 'NOTE'].copy()
df_activities_new = df_activities_new[~df_activities_new['metadata.subject'].fillna('').str.contains('insurance application')].copy()
df_activities_new = df_activities_new[[
    'engagement.type', 'engagement.createdAt', 'engagement.timestamp',
    'engagement.ownerId', 'metadata.disposition', 'metadata.status',
    'metadata.durationMilliseconds',
    'engagement.id', 'associations.dealIds', 'metadata.meetingOutcome'
]]
df_activities_new['associations.dealIds'] = df_activities_new['associations.dealIds'].apply(lambda x: x[0] if len(x) > 0 else '')
df_activities_new['engagement.timestamp'] = pd.to_datetime(df_activities_new['engagement.timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
df_activities_new['activity_time'] = df_activities_new['engagement.timestamp'].dt.strftime('%H:%M')
df_activities_new['engagement.createdAt'] = df_activities_new['engagement.createdAt'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
activity_type_mapping = {'CALL':'Call', 'EMAIL':'Email sent to contact', 'INCOMING_EMAIL':'Email reply from contact'}
df_activities_new['engagement.type'] = df_activities_new['engagement.type'].apply(lambda x: activity_type_mapping.get(x, x))
df_activities_new.rename(columns={'associations.dealIds':'hs_object_id'}, inplace=True)

earliest_effective_date = 1622505600000

# Get all deals with effective date July 1st and later
df_deals = pd.DataFrame()
url = 'https://api.hubapi.com/crm/v3/objects/deals/search?hapikey=' + hubspot_credentials
headers = {'Content-Type':'application/json'}
data = '''{ "archived": false, "limit": 100, "properties":''' + str(deal_fields_list).replace('\'', '\"') + ''',
    "filterGroups":[
      {
        "filters":[
                {
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": "default"
                },
                {
                    "propertyName": "policy_effective_date",
                    "operator": "GTE",
                    "value": "''' + str(earliest_effective_date) + '''"
                }
        ]
      },
      {
        "filters":[
                {
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": 1052209
                },
                {
                    "propertyName": "policy_effective_date",
                    "operator": "GTE",
                    "value": "''' + str(earliest_effective_date) + '''"
                }
        ]
      },
      {
        "filters":[
                {
                    "propertyName": "policy_effective_date",
                    "operator": "NOT_HAS_PROPERTY"
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
                                    "propertyName": "pipeline",
                                    "operator": "EQ",
                                    "value": "default"
                                },
                                {
                                    "propertyName": "policy_effective_date",
                                    "operator": "GTE",
                                    "value": "''' + str(earliest_effective_date) + '''"
                                }
                        ]
                      },
                      {
                        "filters":[
                                {
                                    "propertyName": "pipeline",
                                    "operator": "EQ",
                                    "value": 1052209
                                },
                                {
                                    "propertyName": "policy_effective_date",
                                    "operator": "GTE",
                                    "value": "''' + str(earliest_effective_date) + '''"
                                }
                        ]
                      },
                      {
                        "filters":[
                                {
                                    "propertyName": "policy_effective_date",
                                    "operator": "NOT_HAS_PROPERTY"
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
            time.sleep(10)
            if(retries > 20):
                raise ValueError
print('\nRetrieved', len(df_deals), 'deals, max effective date:', pd.to_datetime(deals['properties.policy_effective_date']).max())
df_deals = df_deals[df_deals['properties.pipeline'] != '2053853'].copy()
df_deals.reset_index(drop=True, inplace=True)

region_mapping = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\carrier_airtables\\zip_code_region.csv')
region_mapping.loc[:, 'zip_code'] = region_mapping.zip_code.apply(lambda x: str(x)[:3])
region_mapping = region_mapping[['zip_code', 'region']].drop_duplicates().copy()
region_mapping = dict(zip(region_mapping.zip_code, region_mapping.region))

region_category_mapping = {
    'Light' : set(['2', '6', '7', '19', '1', '3', '4', '5']),
    'Medium' : set(['8', '9', '10', '13', '17', '18']),
    'Heavy' : set(['11', '12', '14', '16', '15'])
}

def get_region_category(region):
    if(pd.isnull('region') or region == '' or region == 'Unknown'):
        return 'Unknown'
    else:
        for key, value in region_category_mapping.items():
            if(region in value):
                return key
    print('Region Category Not Found for', region)
    raise ValueError

all_apps = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\carrier_airtables\\all_apps.csv')


df_deals.rename(columns={'properties.application_id':'application_id'}, inplace=True)
df_deals = df_deals.merge(all_apps, how='left', on='application_id')

df_deals['zipcode'] = df_deals.company_addresses.apply(lambda x: str(eval(x)[0]['postalCode']) if not pd.isnull(x) else x).fillna(df_deals['properties.zip_code'])

df_deals['wcirb_region'] = df_deals.zipcode.apply(lambda x: region_mapping.get(str(x)[:3], None))
df_deals.loc[:, 'wcirb_region'] = df_deals.wcirb_region.fillna(0).astype(int).replace(0, 'Unknown').astype(str)
df_deals['region_category'] = df_deals.wcirb_region.apply(lambda x: get_region_category(x))

columns_to_drop = ['3660705', '3660706', '3660707', '3660708', '3660709', '3660710', '3660711', "3660706", '13451948', '14516663']
for stage_id in columns_to_drop:
    for col in df_deals.columns:
        if(stage_id in col):
            df_deals.drop(columns=[col], inplace=True)

deal_stage_mapping = pd.DataFrame()
pipeline_mapping = dict()

for idx, row in pd.read_json('./dealPipelines.json').iterrows():
    deal_stage_mapping = deal_stage_mapping.append(pd.DataFrame(row['stages']))
    pipeline_mapping[row['id']] = row['label']

deal_stage_mapping = dict(zip(deal_stage_mapping.id, deal_stage_mapping.label))

df_deals['properties.jr__ae'] = df_deals['properties.jr__ae'].map(lambda x: deal_owner_mapping.get(x, x))
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

deals_columns = [
    'properties.hs_object_id', 'properties.policy_effective_date',
    'properties.dealstage', 'properties.amount', 'properties.hs_priority',
    'properties.hubspot_owner_id', 'properties.dealname',
    'properties.wc_new_governing_class_code','properties.wc_governing_class_description',
    'properties.current_status', 'properties.next_step', 'properties.carriers_presented',
    'properties.closing_status', 'properties.target_closing', 'properties.carriers_quote_won',
    'properties.closedate', 'date_entered_Won',
    'date_entered_DM Reached', 'date_entered_Closing',
    'date_entered_Engaged', 'date_entered_Approaching', 'date_entered_Quoted',
    'date_entered_Qualified','date_entered_Lost', 'date_entered_Presented', 'date_entered_New', 'properties.quote_sent',
    'properties.dealtype', 'properties.mobile_phone_number', 'properties.target_close_date', 'properties.jr__ae',
    'properties.last_meeting_status', 'date_entered_Qualifying', 'properties.last_meeting_date', 'properties.pipeline',
    'properties.loss_runs_required', 'wcirb_region', 'region_category', 'zipcode', 'properties.loss_runs_received',
    'properties.createdate'
]

df_deals = df_deals[deals_columns]

df_activities_new.loc[:, 'hs_object_id'] = df_activities_new.hs_object_id.astype(str)

stages_to_get_days = ['Quoted', 'Approaching', 'DM Reached', 'Qualified', 'Engaged', 'Won', 'Closing', 'Lost', 'Presented', 'New', 'Qualifying']
for stage in stages_to_get_days:
    curr = [s for s in df_deals.columns if stage in s and 'entered' in s]
    if(len(curr) > 1):
        print('Error multiple columns found for', stage, ':', curr)
        raise ValueError
    elif(len(curr) == 0):
        print('Error no columns found for', stage, ':', curr)
        raise ValueError
    else:
        df_deals[curr[0]] = pd.to_datetime(df_deals[curr[0]]).dt.tz_localize(None)

df_activities_new.rename(columns={'engagement.type':'Activity type', 'engagement.createdAt':'Create date', 'engagement.timestamp':'Activity date',
                          'engagement.ownerId':'Activity assigned to', 'metadata.disposition':'Call outcome', 'metadata.status':'Call status',
                          'metadata.durationMilliseconds':'Call duration',
                          'engagement.id':'Engagement ID'}, inplace=True)

print('Processing Activities')
for i, row in df_deals.iterrows():
    curr_deal_id = row['properties.hs_object_id']
    curr_deal_stage = row['properties.dealstage']
    if curr_deal_stage in stages_to_get_days:
        matching_stage_column = [s for s in df_deals.columns if curr_deal_stage in s and 'entered' in s]
        if(len(matching_stage_column) > 1):
            print('Error multiple columns found for', curr_deal_stage, ':', matching_stage_column)
            raise ValueError
        else:
            df_deals.at[i, 'Days in Stage'] = (pd.Timestamp.now().normalize() - row[matching_stage_column[0]]).days
    current_activities = df_activities_new[df_activities_new['hs_object_id'] == curr_deal_id].copy()
    # No activities for current deal ID, skip
    if (len(current_activities) < 1):
        continue
    curr_deal_calls = current_activities[(current_activities['Activity type'] == 'Call')].sort_values('Activity date')
    if(len(curr_deal_calls) > 0):
        df_deals.at[i, 'Calls'] = int(len(curr_deal_calls))
        df_deals.at[i, 'Last Called'] = curr_deal_calls.tail(1)['Activity date'].values[0]
    curr_deal_emails = current_activities[(current_activities['Activity type'] == 'Email sent to contact')].sort_values('Activity date')
    if(len(curr_deal_emails) > 0):
        df_deals.at[i, 'Emails'] = int(len(curr_deal_emails))
        df_deals.at[i, 'Last Email'] = curr_deal_emails.tail(1)['Activity date'].values[0]

print('Completed in', round((time.time() - start) / 60, 2), 'minutes.')

def get_last_activity_date(last_email, last_dial):
    if(pd.isnull(last_email) and pd.isnull(last_dial)):
        return None
    elif(pd.isnull(last_email) and not pd.isnull(last_dial)):
        return last_dial
    elif(not pd.isnull(last_email) and pd.isnull(last_dial)):
        return last_email
    else:
        return max(last_email, last_dial)

df_deals['Last Activity'] = df_deals.apply(lambda x: get_last_activity_date(x['Last Called'], x['Last Email']), axis=1)
df_deals.loc[:, 'Last Activity'] = df_deals['Last Activity'].dt.strftime('%m/%d/%Y')

df_deals.loc[:, 'properties.closedate'] = pd.to_datetime(df_deals['properties.closedate']).dt.strftime('%m/%d/%Y').fillna('')
df_deals.loc[:, 'properties.createdate'] = pd.to_datetime(df_deals['properties.createdate']).dt.strftime('%m/%d/%Y').fillna('')
df_deals.loc[:, 'properties.target_close_date'] = pd.to_datetime(df_deals['properties.target_close_date']).dt.strftime('%m/%d/%Y').fillna('')

df_deals['URL'] = df_deals['properties.hs_object_id'].apply(lambda x: 'https://app.hubspot.com/contacts/6612994/deal/' + str(x) + '/')

'''
airtable = Airtable('appMTwvItuV7MpwfG', 'Quota', airtable_credentials)
aes = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
aes = remove_fields(aes)

ae_name_to_id = dict(zip(aes.Name, aes.id))
'''

certainty_mapping = {
    'In Play' : 'In Play (50%)',
    'Committed' : 'Committed (90%)',
    'At Risk' : 'At Risk (25%)'
}

df_deals.loc[:, 'Emails'] = df_deals['Emails'].fillna(0).astype(int)
df_deals.loc[:, 'Calls'] = df_deals['Calls'].fillna(0).astype(int)
df_deals.loc[:, 'Days in Stage'] = df_deals['Days in Stage'].fillna(0).astype(int)
df_deals.loc[:, 'properties.closing_status'] = df_deals['properties.closing_status'].apply(lambda x: certainty_mapping.get(x, x))

df_deals.rename(columns={'properties.dealname' : 'Name',
                         'properties.hs_object_id' : 'Deal ID',
                         'properties.policy_effective_date' : 'Effective Date',
                         'properties.hubspot_owner_id' : 'Owner',
                         'properties.carriers_presented' : 'Carrier Presented',
                         'properties.carriers_quote_won' : 'Carrier Won',
                         'properties.amount' : 'Amount',
                         'properties.hs_priority' : 'Priority',
                         'properties.closing_status' : 'Closing Certainty',
                         'properties.target_closing' : 'Target Closing',
                         'properties.dealstage' : 'Stage',
                         'properties.current_status' : 'Status',
                         'properties.next_step' : 'Next Step',
                         'properties.wc_new_governing_class_code' : 'Class Code',
                         'properties.wc_governing_class_description' : 'Class Code Description',
                         'properties.closedate' : 'Close Date',
                         'properties.quote_sent' : 'Quote Sent',
                         'properties.dealtype' : 'Deal Type',
                         'properties.mobile_phone_number' : 'Phone Number',
                         'properties.target_close_date' : 'Target Close Date',
                         'properties.jr__ae' : 'Jr AE',
                         'properties.last_meeting_status' : 'Last Meeting Status',
                         'properties.last_meeting_date' : 'Last Meeting Date',
                         'properties.pipeline' : 'Pipeline',
                         'properties.loss_runs_required' : 'Loss Runs Required',
                         'wcirb_region' : 'WCIRB Region',
                         'region_category' : 'Region Category',
                         'zipcode' : 'Zip Code',
                         'properties.loss_runs_received': 'Loss Runs Received',
                         'properties.createdate' : 'Hubspot Create Date'}, inplace=True)

df_deals.loc[:, 'Last Meeting Date'] = pd.to_datetime(df_deals['Last Meeting Date']).dt.strftime('%m/%d/%Y').fillna('')

df_deals.loc[:, 'Owner'] = df_deals.Owner.apply(lambda x: deal_owner_mapping.get(x, x)).copy()
df_deals.loc[:, 'Amount'] = df_deals.Amount.fillna('').replace('', '0').astype(float)

df_deals = df_deals[[
    'Deal ID', 'Effective Date', 'Stage', 'Amount', 'Priority',
    'Owner', 'Name', 'Class Code',
    'Class Code Description', 'Status', 'Next Step',
    'Carrier Presented', 'Closing Certainty', 'Target Closing',
    'Carrier Won', 'Close Date', 'URL', 'Calls', 'Emails', 'Days in Stage',
    'Last Activity', 'Quote Sent', 'Deal Type', 'Phone Number',
    'Target Close Date', 'Jr AE', 'Last Meeting Status', 'Last Meeting Date',
    'Pipeline', 'Loss Runs Required', 'WCIRB Region', 'Region Category', 'Zip Code',
    'Loss Runs Received', 'Hubspot Create Date'
]]

tz = pytz.timezone('US/Pacific')
today_pst = (datetime.now(tz) + timedelta(days=0)).date() # today PST
this_sunday = today_pst + relativedelta.relativedelta(weekday=relativedelta.SU(1))
last_sunday = this_sunday - relativedelta.relativedelta(days=7)
next_sunday = today_pst + relativedelta.relativedelta(weekday=relativedelta.SU(2))
this_month = today_pst.month
this_month_year = today_pst.year
next_month = (today_pst+relativedelta.relativedelta(months=1)).month
next_month_year = (today_pst+relativedelta.relativedelta(months=1)).year

def derive_target_closing(target_close_date):
    target_closing = 'Not Set'
    if(target_close_date != ''):
        target_close_date = pd.to_datetime(target_close_date)
        if(target_close_date == today_pst):
            target_closing = 'Today'
        elif(target_close_date > last_sunday and target_close_date <= this_sunday):
            target_closing = 'This Week'
        elif(target_close_date > this_sunday and target_close_date <= next_sunday):
            target_closing = 'Next Week'
        elif(target_close_date < today_pst):
            target_closing = 'In Past'
        elif(target_close_date.month == this_month and target_close_date.year == this_month_year):
            target_closing = 'This Month'
        elif(target_close_date.month == next_month and target_close_date.year == next_month_year):
            target_closing = 'Next Month'
        elif(target_close_date > today_pst):
            target_closing = 'Later'
    return target_closing

df_deals.loc[:, 'Target Closing'] = df_deals['Close Date'].apply(lambda x: derive_target_closing(x))

airtable = Airtable('appMTwvItuV7MpwfG', 'Board', airtable_credentials)
old_data = pd.json_normalize(airtable.get_all(fields=['Deal ID', 'Effective Date', 'Stage', 'Amount', 'Priority',
                                                       'Owner', 'Name', 'Class Code', 'Pipeline',
                                                       'Class Code Description', 'Status', 'Next Step',
                                                       'Carrier Presented', 'Closing Certainty', 'Target Closing',
                                                       'Carrier Won', 'Close Date', 'URL', 'Days in Stage',
                                                       'Calls', 'Emails', 'Last Activity', 'Quote Sent', 'Deal Type',
                                                        'Phone Number', 'Target Close Date', 'Jr AE', 'Last Meeting Status',
                                                        'Last Meeting Date', 'Loss Runs Required', 'WCIRB Region',
                                                        'Region Category', 'Zip Code', 'Loss Runs Received', 'Hubspot Create Date'])).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
old_data = remove_fields(old_data)
old_data.loc[:, 'Close Date'] = pd.to_datetime(old_data['Close Date']).dt.strftime('%m/%d/%Y').fillna('')
old_data.loc[:, 'Last Activity'] = pd.to_datetime(old_data['Last Activity']).dt.strftime('%m/%d/%Y').fillna('')
old_data.loc[:, 'Target Close Date'] = pd.to_datetime(old_data['Target Close Date']).dt.strftime('%m/%d/%Y').fillna('')
old_data.loc[:, 'Last Meeting Date'] = pd.to_datetime(old_data['Last Meeting Date']).dt.strftime('%m/%d/%Y').fillna('')
old_data.loc[:, 'Hubspot Create Date'] = pd.to_datetime(old_data['Hubspot Create Date']).dt.strftime('%m/%d/%Y').fillna('')
old_data.fillna('', inplace=True)
df_deals.fillna('', inplace=True)
old_data.loc[:, 'Calls'] = old_data['Calls'].replace('', 0).astype(int)
old_data.loc[:, 'Emails'] = old_data['Emails'].replace('', 0).astype(int)
old_data.loc[:, 'Days in Stage'] = old_data['Days in Stage'].replace('', 0).astype(int)

to_insert = []
to_update = []

def check_fields(old_deal, new_deal):
    for col in new_deal.index:
        curr_new_field = new_deal[col]
        curr_old_field = old_deal[0][col]
        try:
            if(type(curr_new_field) == str):
                curr_old_field = curr_old_field.strip('\n').replace('\\', '')
        except Exception:
            print(curr_new_field, col, new_deal[col], curr_old_field, col, old_deal[0][col])
            raise ValueError
        if(curr_new_field != curr_old_field):
            print('Update:', col, '| new:', new_deal[col], '| old:', old_deal[0][col])
            return True
    return False

for idx, new_deal in df_deals.iterrows():
    old_record = old_data[old_data['Deal ID'] == new_deal['Deal ID']].copy()
    if(len(old_record) > 1):
        print('Multiple deals found.')
        raise ValueError
    elif(len(old_record) == 1):
        needs_update = check_fields(old_record.to_dict('records'), new_deal)
        if(needs_update):
            to_update.append({'id': old_record.head(1).id.item(), 'fields' : new_deal.to_dict()})
    elif(len(old_record) == 0):
        to_insert.append(new_deal.to_dict())
    else:
        print('Corner')
        raise ValueError

min_new_data_date = pd.to_datetime(earliest_effective_date, unit='ms')

to_delete = []
new_deal_ids = df_deals['Deal ID'].unique()
for idx, row in old_data.iterrows():
    if(row['Deal ID'] not in new_deal_ids and pd.to_datetime(row['Effective Date']) >= min_new_data_date):
        to_delete.append(row.id)

if(len(to_delete) > 0):
    airtable.batch_delete(to_delete)

print('Deleted', len(to_delete), 'deals.')

records_to_update = pd.DataFrame(to_update).to_dict('records')
records_to_insert = pd.DataFrame(to_insert).to_dict('records')

if(len(records_to_insert) > 0):
    airtable.batch_insert(records_to_insert, typecast=True)

print('Inserted', len(records_to_insert), 'deals.')

if(len(records_to_update) > 0):
    airtable.batch_update(records_to_update, typecast=True)

print('Updated', len(records_to_update), 'deals.')