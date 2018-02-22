import pandas as pd
import csv
from fuzzywuzzy import fuzz
import sys
import datetime

i = datetime.datetime.now()

date = '%s/%s/%s' %( i.day, i.month, i.year)

domain_input = sys.argv[1]

# Import Domains CSV.
domain_list = pd.read_csv(domain_input, keep_default_na=False, na_values=['_'])
print("Domain list link successful!")

# Import US History CSV.
domain_history = pd.read_csv(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\UK.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
print("US Domain History link successful!")

# Import US MX History CSV.
mx_history = pd.read_csv(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\uk_mx_history.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
print("MX History link successful!")

# Import Bad Whois String
with open(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\BadWhoIsUpdated.csv", "r") as f:
    bad_whois = [row[0] for row in csv.reader(f)]
    print("Bad Whois string list link successful!")
    
# Import Good MX String, used to find string within Primary MX Column
with open(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\good_uk.csv", "r") as f:
    good_list_uk = [row[0] for row in csv.reader(f)]
    print("Good US string domain list link successful!")

# import us counties string list into variable
with open(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\uk_county.csv", "r") as f:
    county_list = [row[0] for row in csv.reader(f)]
    print("US county string list link successful!")

# import expired domain string list into variable
with open(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\expired.csv", "r") as f:
    expired_list = [row[0] for row in csv.reader(f)]
    print("Expired string domain list link successful!")

# import tld countries
with open(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\tld_uk.csv", "r") as f:
    tld_uk = [row[0] for row in csv.reader(f)]
    print("TLD string list link successful!")

# Reindex and remove all columns apart from Domain & Domain Histiory
domain_history = domain_history.reindex_axis(['Domain', 'Domain History','Last Checked'], axis=1)

# merge domain_list + mx_history + domain_history_uk using JOIN LEFT
df = pd.merge(domain_list, mx_history, on=' Primary MX Server', how='left') \
    .merge(domain_history, on='Domain', how='left')
print("Merge successful!")
print("Analysis Computing...")

# Auto_check, used to produce to populate the results
df['WebScrap'] = ''
print('Auto_Check Column Added!')

# Date comparison
df['Current Date'] = date
df['Temp Current Date'] = pd.to_datetime(df['Current Date'], format="%d/%m/%Y")
df['Temp Last Checked'] = pd.to_datetime(df['Last Checked'], infer_datetime_format=True)
df['No. Days'] = (df['Temp Current Date'] - df['Temp Last Checked']).astype('timedelta64[D]')
df.loc[(df['No. Days'] >= 60), 'WebScrap'] = 'TRUE'
df.loc[(df['No. Days'] < 60), 'WebScrap'] = 'FALSE'

# Clean data fields
df[' Primary MX Server'] = df[' Primary MX Server'].str.lower().str.strip().replace('\s+', '') 
df[' Country'] = df[' Country'].str.lower().str.strip().replace('\s+', '')
df[' Address'] = df[' Address'].str.lower().str.strip()
df['Registrant'] = df['Registrant'].str.lower().str.strip()
df[' A Count'] = df[' A Count'].astype(str)
df['Domain History'] = df['Domain History'].str.strip()
print("Columns Cleaned!")

# Remove N/A
df['Domain History'] = df['Domain History'].str.replace('#N/A,?', '')
print("Replace N/A's Completed!")

# Auto_check, used to produce to populate the results
df['Auto_Check'] = ''
print('Auto_Check Column Added!')

# Good US MX
df.loc[(df[' Primary MX Server'].str.contains('|'.join(good_list_uk), na=False)), 'Good MX'] = 'OK'

# Temp storage for domain comparison
temp = []
temptld = []

# Compare Domain with Primary MX & Extract TLD from Primary MX
for item in df.itertuples():
    domain = item[1].split('.')[0] # Remove TLD from Domain
    mxtld = item[6].split('.')[-1] # Extract TLD from Primary MX
    mx = item[6]
    # Comparison
    if mx.find(domain) == -1:
        temptld.append(mxtld)
        temp.append('FALSE')
    else:
        temptld.append(mxtld)
        temp.append('TRUE')

# Merge Domain comparison with Primary dataframe
df['Comparison'] = temp
print("Domain String Analysis Completed!")

# Merge Primary MX TLD with Primary dataframe
df['Primary MX Server TLD'] = temptld
print("Primary MX Server TLD Extracted!")

df['Primary MX Server TLD'] = df['Primary MX Server TLD'].str.lower().str.strip()
print("Primary MX Server TLD Cleaned!")

# STATUS UNKNOWN
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & ((df[' Primary MX Server'].isnull()) | (df[' Primary MX Server'] == '')), 'Auto_Check'] = 'NoMX'
print("NoMX Analysis Completed!")

# SPAM TRAP
df.loc[(df['MX History'] == 'Spam Trap')
        | df[' Primary MX Server'].str.contains('1bp.com|2bp.com|3bp.com|4bp.com|5bp.com|6bp.com|7bp.com|8bp.com|9bp.com|10bp.com|mb1p|mb2p|mb3p|mb4p|mb5p|mb6p|mb7p|mb8p|mb9p|mb10p|inbound-mx.net'), 'Auto_Check'] = 'Spam Trap'
print("Spam Trap Analysis Completed!")

# BAD MX RECORD
df.loc[(df['MX History'] == 'Bad') 
        & (df['Auto_Check'] == '') & (df['Domain History'] != 'Bad_M'), 'Auto_Check'] = 'Bad'
print("Bad MX Analysis Completed!")

# EXPIRED
df.loc[(df['Domain'].str.contains('|'.join(expired_list), na=False))
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Expired'
print("Expired Domain Analysis Completed!")

# invalid
df.loc[(df['Status'] != 'Invalid')
        & (df['Status'] != 'Blacklisted')
        & ((df['TLD'] == 'cm')
            | (df['TLD'] == 'cim')
            | (df['TLD'] == 'con')
            | (df['TLD'] == 'om')
            | (df[' Primary MX Server'].str.contains('invalid', na=False))
            | (df['Domain'].str.contains('invalid', na=False)))
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Bad'

# invalid
df.loc[(df['Status'] == 'Invalid')
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Invalid'
print("Invalid Analysis Completed!")

# Exclude
df.loc[(df['Status'] == 'Excluded')
        & ((df['Domain History'].isnull()) | (df['Domain History'] == ''))
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Foreign'

# Exclude
df.loc[(df['Auto_Check'] == '')
        & ((df['TLD'].str.contains('gov', na=False)) 
        | (df[' Primary MX Server'].str.contains('.gov', na=False))
        | (df[' Address'].str.contains('.gov', na=False))), 'Auto_Check'] = 'Exclude'
print("Exclude Analysis Completed!")

# Blacklisted
df.loc[(df['Status'] == 'Blacklisted')
        & (((df['Domain History'].isnull()) | (df['Domain History'] == ''))
            | ((df['Domain History'] != 'Blacklisted')
                & (df['Domain History'] != 'Spam Trap')
                & (df['Domain History'] != 'Temp')
                & (df['Domain History'] != 'Expired')))
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Blacklisted'
print("Blacklisted Analysis Completed!")

# badmx
df.loc[(df['Status'] == 'BadMX')
        & (((df['Domain History'].isnull()) | (df['Domain History'] == ''))
            | ((df['Domain History'] != 'Blacklisted')
                & (df['Domain History'] != 'Spam Trap')
                & (df['Domain History'] != 'Temp')
                & (df['Domain History'] != 'Expired')))
        & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Bad'
print("BadMX Status Analysis Completed!")

# check change in mx
df.loc[(df['Domain History'] == 'NoMX') 
        & (df['Auto_Check'] == '')
        & (df[' Primary MX Server'].notnull()) 
        & (df[' Primary MX Server'] != ''), 'Auto_Check'] = 'MXChanged'
print("Check for MX Change Completed!")

# check bad mx
df.loc[(df[' Primary MX Server'].str.contains('checkyouremailaddress|hostnamedoesnotexist', na=False)) 
        & (df['Auto_Check'] == '')
        & (df['Domain History'] != 'Bad_M'), 'Auto_Check'] = 'Bad'
print("Check for Bad MX Completed!")

# Bad Domain
df.loc[(df['Status'] == 'Unknown') 
        & (df['Auto_Check'] == '')
        & (df['Domain'].str.contains('\.co\.com', na=False)), 'Auto_Check'] = 'Bad'

# FOREIGN MX RECORD 
df.loc[(df['MX History'] == 'Foreign') 
        & (df['Auto_Check'] == '')
        & (df['Domain History'] != 'Foreign_M'), 'Auto_Check'] = 'Foreign'
print("Foreign MX Analysis Completed!")

# Bad Registrant & Organisation 
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & (df['Auto_Check'] == '')
        & ((df['Registrant'].str.contains('|'.join(bad_whois)))) | (df[' Organisation'].str.contains('|'.join(bad_whois))), 'Auto_Check'] = 'Bad'

# WHITELISTED & GOODMX STATUS - All status to foriegn, as these fields are only application to US data
df.loc[((df['Status'] == 'Whitelisted') | (df['Status'] == 'GoodMX'))
        & (df['Auto_Check'] == '')
        & (df['Domain History'].isnull()), 'Auto_Check'] = 'OK'
print("1 - Analysis Complete!")

df.loc[(df[' Primary MX Server'].str.contains('mail2world|qq\.com')), 'MX History'] = 'Foreign'

# UNKNOWN STATUS - FOREIGN - Check domain is Foreign by checking the address/countruy
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        & ((df['TLD'] == 'edu') | (df['Primary MX Server TLD'] == 'edu') | (df[' Primary MX Server'].str.contains('mail2world'))), 'Auto_Check'] = 'Foreign'
print("2 - Analysis Complete!")

# UNKNOWN STATUS - FOREIGN - Check domain is Foreign by checking the address/countruy
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        & (df[' Country'] != '')
        & ((df[' Country'] != 'uk') 
            & (df[' Country'] != 'united kingdom') 
            & (df[' Country'] != 'gb')
            & (df[' Country'] != 'ie')
            & (df[' Country'] != 'im')), 'Auto_Check'] = 'Foreign'
print("3 - Analysis Complete!")

df.loc[(df['Status'] == 'Bad WhoIs')
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        & (df['Registrant'].str.contains('privacy|private')), 'Auto_Check'] = 'Foreign'
print("4 - Analysis Complete!")

# UNKNOWN STATUS - OK - Check domains is OK
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        & ((df['Comparison'] == 'TRUE')
            | (df[' Primary MX Server'].str.contains('|'.join(good_list_uk)))
            | (df['MX History'] == 'OK'))
        & (((df[' Country'] == 'uk') 
            | (df[' Country'] == 'united kingdom') 
            | (df[' Country'] == 'gb')
            | (df[' Country'] == 'ie')
            | (df[' Country'] == 'im'))
            | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom')))), 'Auto_Check'] = 'OK'
print("5 - Analysis Complete!")

# UNKNOWN STATUS - UNKNOWN UK - Check domains is Unknown UK
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & (~df[' Primary MX Server'].str.contains('google|outlook'))
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))                      
        & ((df[' Country'] == '') & (df[' Address'] == ''))
        & ((df[' Primary MX Server'].str.contains('|'.join(good_list_uk)))
            | (df['MX History'] == 'OK')
            | (df['TLD'] == 'uk')), 'Auto_Check'] = 'Unknown UK'
print("6 - Analysis Complete!")

# UNKNOWN STATUS - Check Unknown UK has address/country if TRUE status to OK
df.loc[((df['Auto_Check'] == 'Unknown UK'))
        & ((df[' Country'] == 'uk') 
            | (df[' Country'] == 'united kingdom') 
            | (df[' Country'] == 'gb')
            | (df[' Country'] == 'ie')
            | (df[' Country'] == 'im')    
            | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom')))), 'Auto_Check'] = 'OK'
print("7 - Analysis Complete!")

# UNKNOWN STATUS - OK - Check domains is OK
df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
            | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        & ((df['Comparison'] == 'TRUE')
            | (df[' Primary MX Server'].str.contains('|'.join(good_list_uk)))
            | (df['MX History'] == 'OK'))
        & ((df['TLD'] == 'uk') | (df['TLD'] == 'uk.com')), 'Auto_Check'] = 'Unknown UK'
print("8 - Analysis Complete!")
print("<< UK Analysis Complete! >>")
















# combine domain history with auto check
df.loc[(df['Domain History'].notnull()) & (df['Auto_Check'] == '') , 'Auto_Check'] = df['Domain History']

df.loc[(df['WebScrap'] == '') & ((df['Auto_Check'] == '') | (df['Auto_Check'] == 'Unknown') | (df['Auto_Check'] == 'MXChanged') | ((df[' Primary MX Server'].str.contains('secureserver')) & ((df['Auto_Check'] == 'OK') | (df['Auto_Check'] == 'Unknown UK') | (df['Auto_Check'] == 'Unknown US')))), 'Auto_Check'] = 'Blank'

df.loc[(df['WebScrap'] == '') & (((df['MX History'].isnull()) | (df['MX History'] == '')) & (df['Auto_Check'] == 'Foreign') & ((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))) , 'Auto_Check'] = 'Blank'

df.loc[(df['Auto_Check'] == 'Private Non Trade'), 'Auto_Check'] = 'Blank'

df.loc[(df['WebScrap'] == 'TRUE') & (df['Auto_Check'].str.contains('OK|Unknown US|Unknown UK|Unknown|Foreign')), 'Auto_Check'] = 'Blank'

print('Fuzzy Match Computing...')
gmail = []
googlemail = []
hotmail = []
yahoo = []
ymail = []
live = []
msn = []
aol = []
btinternet = []
virginmedia = []      
icloud = []
talktalk = []
tiscali = []
sky = []
blueyonder = []
ntlworld = []
talk21 = []
outlook = []
rocketmail = []

for item in df.itertuples():

    gmail.append(fuzz.partial_ratio(item[1], "gmail"))
    googlemail.append(fuzz.partial_ratio(item[1], "google"))
    hotmail.append(fuzz.partial_ratio(item[1], "hotmail"))
    yahoo.append(fuzz.partial_ratio(item[1], "yahoo"))
    ymail.append(fuzz.partial_ratio(item[1], "ymail"))
    live.append(fuzz.partial_ratio(item[1], "live"))
    msn.append(fuzz.partial_ratio(item[1], "msn"))
    aol.append(fuzz.partial_ratio(item[1], "aol"))
    btinternet.append(fuzz.partial_ratio(item[1], "btinternet"))
    virginmedia.append(fuzz.partial_ratio(item[1], "virginmedia"))
    icloud.append(fuzz.partial_ratio(item[1], "icloud"))
    talktalk.append(fuzz.partial_ratio(item[1], "talktalk"))
    tiscali.append(fuzz.partial_ratio(item[1], "tiscali"))
    sky.append(fuzz.partial_ratio(item[1], "sky"))
    blueyonder.append(fuzz.partial_ratio(item[1], "blueyonder"))
    ntlworld.append(fuzz.partial_ratio(item[1], "ntlworld"))
    talk21.append(fuzz.partial_ratio(item[1], "talk21"))
    outlook.append(fuzz.partial_ratio(item[1], "outlook"))
    rocketmail.append(fuzz.partial_ratio(item[1], "rocketmail"))

df['gmail'] = gmail
df['googlemail'] = googlemail
df['hotmail'] = hotmail
df['yahoo'] = yahoo
df['ymail'] = ymail
df['live'] = live
df['msn'] = msn
df['aol'] = aol
df['btinternet'] = btinternet
df['virginmedia'] = virginmedia
df['icloud'] = icloud
df['talktalk'] = talktalk
df['tiscali'] = tiscali
df['sky'] = sky
df['blueyonder'] = blueyonder
df['ntlworld'] = ntlworld
df['talk21'] = talk21
df['outlook'] = outlook
df['rocketmail'] = rocketmail

print("Fuzzy Match Complete!")       

df = df.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                              ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                              'Registrant',' Organisation',' Address',' Country','Registry',
                              ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','Current Date','No. Days','Web Check','Match','WebScrap','MX History','Good MX',
                              'Primary MX Server TLD','Comparison',
                              'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                              'btinternet','virginmedia','icloud','talktalk','tiscali','sky','blueyonder',
                              'ntlworld','talk21','outlook','rocketmail'], axis=1)

print('Reindex Complete!')

df.set_index('Domain', inplace=True)
print('Index Set!')

df.to_csv(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\domain_analysis\TODO_domains\uk\\" + domain_input)

