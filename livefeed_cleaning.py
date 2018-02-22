import pandas as pd
import csv
import sys

import tkinter 
tkinter.Tk().withdraw()
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

import datetime
i = datetime.datetime.now()
date = '%s%s%s' %( i.day, i.month, i.year)

print("Select Livefeed File..")
livefeed_file = askopenfilename()
livefeed = pd.read_csv(livefeed_file,keep_default_na=False, na_values=['_'],encoding ='latin1')

print("Select Domain Analysis File..Press ESC To Skip")
domain_analysis_file = askopenfilename()

print("Select Output Folder..")
folder = askdirectory()
operations = ''

if livefeed_file.find('TPS') != -1:
    operations = 'TPS'
    print('TPS Detected')
elif livefeed_file.find('RG') != -1:
    operations = 'RG'
    print('RG Detected')
elif livefeed_file.find('LoanOffers4Me') != -1:
    operations = 'LoanOffers4Me'
    print('LoanOffers4Me Detected')
elif livefeed_file.find('PPM') != -1:
    operations = 'PPM'
    print('PPM Detected')
else:
    operations = 'Normal'
    print('Normal Operations')


if operations == 'LoanOffers4Me':
    livefeed = livefeed.reindex_axis(['Email','Title','First_Name','Last_Name','Address1','Address2','ADDRESS3','City','County','Postcode','PHONE','MOBILE','Gender','DOB','Source','IP_Address','Created_DateTime'], axis=1)
    livefeed.columns = ['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE']

elif operations == 'TPS': 
    print("Comencing TPS Functions..")
    england_postcodes1 = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\England1.csv', keep_default_na=False, na_values=['_'],encoding ='latin1')
    england_postcodes2 = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\England2.csv', keep_default_na=False, na_values=['_'],encoding ='latin1')
    england_postcodes3 = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\England3.csv', keep_default_na=False, na_values=['_'],encoding ='latin1')
    scotland_postcodes = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\Scotland.csv', keep_default_na=False, na_values=['_'],encoding ='latin1')
    wales_postcodes = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\Wales.csv', keep_default_na=False, na_values=['_'])

    england_postcodes1['POSTCODE'] = england_postcodes1['POSTCODE'].str.replace('\s+', '').str.lower()
    england_postcodes2['POSTCODE'] = england_postcodes2['POSTCODE'].str.replace('\s+', '').str.lower()
    england_postcodes3['POSTCODE'] = england_postcodes3['POSTCODE'].str.replace('\s+', '').str.lower()
    scotland_postcodes['POSTCODE'] = scotland_postcodes['POSTCODE'].str.replace('\s+', '').str.lower()
    wales_postcodes['POSTCODE'] = wales_postcodes['POSTCODE'].str.replace('\s+', '').str.lower()

    livefeed = pd \
    .merge(livefeed,england_postcodes1, on='POSTCODE', how='left') \
    .merge(england_postcodes2, on='POSTCODE', how='left') \
    .merge(england_postcodes3, on='POSTCODE', how='left') \
    .merge(scotland_postcodes, on='POSTCODE', how='left') \
    .merge(wales_postcodes,on='POSTCODE', how='left')

    livefeed.loc[(livefeed['Scotland In Use?'].notnull()), 'Auto Country'] = 'OK'
    livefeed.loc[(livefeed['England 1 In Use?'].notnull()), 'Auto Country'] = 'OK'
    livefeed.loc[(livefeed['England 2 In Use?'].notnull()), 'Auto Country'] = 'OK'
    livefeed.loc[(livefeed['England 3 In Use?'].notnull()), 'Auto Country'] = 'OK'
    livefeed.loc[(livefeed['Wales In Use?'].notnull()), 'Auto Country'] = 'OK'

    livefeed = livefeed[(livefeed['Auto Country'] == 'OK')]
    livefeed = livefeed.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)

elif operations == 'PPM':
    print("Comencing PPM Functions..")
    print("Importing US ZIP Codes..")
    postcode = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\US_ZIP_Codes.csv')
    postcode['POSTCODE'] = postcode['POSTCODE'].astype(str)
    print("Merging US ZIP Codes..")
    livefeed = pd.merge(livefeed, postcode, on='POSTCODE', how='left')
    print("Removing Null Postcode..")
    livefeed = livefeed[(livefeed['COUNTY_ALIAS'].notnull())]
    livefeed['COUNTY'] = livefeed['COUNTY_ALIAS']
    print("Cleaning Columns")
    livefeed = livefeed.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)


elif operations == 'RG':
    print("Comencing RG Functions..")
    if 'EMAIL' in livefeed.columns:
        print("Importing US Data Source..")
        source = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\us_data_source.csv',keep_default_na=False, na_values=['_'],encoding ='latin1')
        print("Merging US Data Source..")
        livefeed = pd.merge(livefeed, source, on='URL', how='left')
        print("Removing Excluded List Type..")
        livefeed = livefeed[~(livefeed['Source Status'] == 'Exclude')]
        livefeed = livefeed.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Source Status','List Type'], axis=1)  
    else:
        print("Columns Missing..")
        livefeed = livefeed.reindex_axis(['Email','Title','First Name','Last Name','Address1','Address2','ADDRESS3','City','County','Postcode','PHONE','MOBILE','Gender','DOB','Source','IP Address','Received'], axis=1)
        livefeed.columns = ['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE']
        print("Adding Columns..")
        print("Merging US Data Source")
        source = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\us_data_source.csv',keep_default_na=False, na_values=['_'],encoding ='latin1')
        livefeed = pd.merge(livefeed, source, on='URL', how='left')
        print("Removing Excluded List Type..")
        livefeed = livefeed[~(livefeed['Source Status'] == 'Exclude')]
        livefeed = livefeed.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Source Status','List Type'], axis=1)

try:
    print("Gender Check..")
    livefeed.loc[(livefeed['TITLE'].str.contains('Ms|Mrs|Miss', na=False)), 'GENDER'] = 'F'
    livefeed.loc[(livefeed['TITLE'].str.contains('Mr', na=False)), 'GENDER'] = 'M'
    livefeed.loc[~(livefeed['TITLE'].str.contains('Ms|Mrs|Miss|Mr', na=False)), 'GENDER'] = ''    
except:
    print("Tile Column Empty, Skipping..")
    pass

print("Extracting Domains..")
livefeed['Domain'] = livefeed['EMAIL'].str.split('@').str[1]
livefeed['Domain'] = livefeed['Domain'].str.lower()

if domain_analysis_file:
    print('External Domain Import Detected..')
    print("Importing External Domain File..")
    domain_analysis = pd.read_csv(domain_analysis_file,keep_default_na=False, na_values=['_'],encoding ='latin1')
    domain_analysis = domain_analysis.reindex_axis(['Domain','Auto_Check'], axis=1)
    print('Merging...')
    livefeed = pd.merge(livefeed, domain_analysis, on='Domain', how='left')


elif operations == 'PPM' or operations == 'RG':
    print('No External Domain Import..')
    print('US Domain History Imported..')
    domain_analysis = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\US.csv',keep_default_na=False, na_values=['_'],encoding ='latin1')
    domain_analysis = domain_analysis.reindex_axis(['Domain','Domain History'], axis=1)
    domain_analysis.columns = ['Domain','Auto_Check']
    print('Merging...')
    livefeed = pd.merge(livefeed, domain_analysis, on='Domain', how='left')

else:
    print('No External Domain Import..')
    print('UK Domain History Imported..')
    domain_analysis = pd.read_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\UK.csv',keep_default_na=False, na_values=['_'],encoding ='latin1')
    domain_analysis = domain_analysis.reindex_axis(['Domain','Domain History'], axis=1)
    domain_analysis.columns = ['Domain','Auto_Check']
    print('Merging...')
    livefeed = pd.merge(livefeed, domain_analysis, on='Domain', how='left')


print("Improting Secondary Email Strings..")
with open(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\secondary_emails.csv', 'r') as f:
    secondary_email = [row[0] for row in csv.reader(f)]

print("Temp Check..")
livefeed['Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('shop|loan|comp|backup|trash|crap|shit|account|signup|register|free|'
                                'garbage|invalid|test|product|lending|fuck|bastard|pussy|bastard|verification|airport|compare|spree|cash|paid|whatever', na=False)), 'Temp'] = 'TRUE'
livefeed.loc[(livefeed['EMAIL'].str.contains('|'.join(secondary_email), na=False)), 'Temp'] = 'REMOVE'


livefeed.loc[(livefeed['EMAIL'].str.contains('sloan', na=False)) & ((livefeed['LASTNAME'].str.contains('Sloan|SLoanes', na=False)) | (livefeed['FIRSTNAME'].str.contains('Sloan|SLoanes', na=False))), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('bishop', na=False)) & ((livefeed['LASTNAME'].str.contains('Bishop', na=False))  | (livefeed['FIRSTNAME'].str.contains('Bishop', na=False))), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('compton', na=False)) & ((livefeed['LASTNAME'].str.contains('Compton', na=False)) | (livefeed['FIRSTNAME'].str.contains('Compton', na=False))), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('cash', na=False)) & ((livefeed['LASTNAME'].str.contains('Cash', na=False)) | (livefeed['FIRSTNAME'].str.contains('Cash', na=False))), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('paid', na=False)) & ((livefeed['LASTNAME'].str.contains('paid', na=False)) | (livefeed['FIRSTNAME'].str.contains('paid', na=False))), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('money', na=False)) & ((livefeed['LASTNAME'].str.contains('Money', na=False)) | (livefeed['FIRSTNAME'].str.contains('Money', na=False))), 'Temp'] = ''

print("Removing Temp Emails..")
livefeed = livefeed[~(livefeed['Temp'] == 'REMOVE')]

print("Removing Bad Status..")
livefeed = livefeed[(livefeed['Auto_Check'] == 'OK_M') 
                    | (livefeed['Auto_Check'] == 'OK') 
                    | (livefeed['Auto_Check'] == 'Unknown UK')
                    | (livefeed['Auto_Check'] == 'Unknown US')]






if operations == 'RG':
    print("Creating Sub Categories For RGUS...")
    subprime = livefeed[(livefeed['List Type'] == 'Subprime')]
    career = livefeed[(livefeed['List Type'] == 'Career')]
    offers = livefeed[(livefeed['List Type'] == 'Offers')]
    blanks = livefeed[~((livefeed['List Type'] == 'Offers') | (livefeed['List Type'] == 'Subprime') | (livefeed['List Type'] == 'Career'))]

    print("Cleaning Columns...")
    subprime = subprime.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Temp'], axis=1)
    career = career.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Temp'], axis=1)
    offers = offers.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Temp'], axis=1)
    blanks = blanks.reindex_axis(['EMAIL','URL','Source Status','List Type'], axis=1)

    print("Setting Email to Index..")
    subprime.set_index('EMAIL', inplace=True)
    career.set_index('EMAIL', inplace=True)
    offers.set_index('EMAIL', inplace=True)
    blanks.set_index('EMAIL', inplace=True)

    print("Outputting Subprime Category..")
    subprime.to_csv(folder + '/x_Sub_RGUS_x_' + date + '.csv')
    print("Outputting Career Category..")
    career.to_csv(folder + '/x_Car_RGUS_x_' + date + '.csv')
    print("Outputting Offers Category..")
    offers.to_csv(folder + '/x_Off_RGUS_x_' + date + '.csv')
    print("Outputting Blank Category..")
    blanks.to_csv(folder + '/x_Bla_RGUS_x_' + date + '.csv')
    print("Complete!")
else:
    print("Cleaning Columns...")
    livefeed = livefeed.reindex_axis(['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE','Temp'], axis=1)
    
    print("Setting Email to Index..")
    livefeed.set_index('EMAIL', inplace=True)
    
    print("Outputting Results")
    livefeed.to_csv(folder + '/results.csv')
    print("Complete!")