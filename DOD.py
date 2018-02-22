import pandas as pd
import csv
from fuzzywuzzy import fuzz
import sys
import datetime


list = r'C:\Users\Elwyne\Downloads\DOD\list.csv'
source = pd.read_csv(list,keep_default_na=False, na_values=['_'],encoding ='latin1')

domain_input = r'C:\Users\Elwyne\Downloads\DOD\DOD_Update_7\DOD_Update_7_cleaned.csv'

df = pd.read_csv(domain_input,keep_default_na=False, na_values=['_'],encoding ='latin1', low_memory=False)


domain_list1 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_1_domain_export.csv'
domain_list2 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_2_domain_export.csv'
domain_list3 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_3_domain_export.csv'
domain_list4 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_4_domain_export.csv'
domain_list5 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_5_domain_export.csv'
domain_list6 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_6_domain_export.csv'
domain_list7 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_7_domain_export.csv'
domain_list8 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_8_domain_export.csv'
domain_list9 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_9_domain_export.csv'
domain_list10 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_10_domain_export.csv'
domain_list11 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_11_domain_export.csv'
domain_list12 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_12_domain_export.csv'
domain_list13 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_13_domain_export.csv'
domain_list14 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_14_domain_export.csv'
domain_list15 = r'C:\Users\Elwyne\Downloads\DOD\Domains\WS_DOD_Domains_All_15_domain_export.csv'

df_list1 = pd.read_csv(domain_list1,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list2 = pd.read_csv(domain_list2,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list3 = pd.read_csv(domain_list3,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list4 = pd.read_csv(domain_list4,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list5 = pd.read_csv(domain_list5,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list6 = pd.read_csv(domain_list6,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list7 = pd.read_csv(domain_list7,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list8 = pd.read_csv(domain_list8,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list9 = pd.read_csv(domain_list9,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list10 = pd.read_csv(domain_list10,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list11 = pd.read_csv(domain_list11,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list12 = pd.read_csv(domain_list12,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list13 = pd.read_csv(domain_list13,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list14 = pd.read_csv(domain_list14,keep_default_na=False, na_values=['_'],encoding ='latin1')
df_list15 = pd.read_csv(domain_list15,keep_default_na=False, na_values=['_'],encoding ='latin1')

frames = [df_list1,
          df_list2,
          df_list3,
          df_list4,
          df_list5,
          df_list6,
          df_list7,
          df_list8,
          df_list9,
          df_list10,
          df_list11,
          df_list12,
          df_list13,
          df_list14,
          df_list15
         ]


df_list = pd.concat(frames)

#df = df[df['Domain Group'] != 'AOL']
#df = df[df['Domain Group'] != 'BT']
#df = df[df['Domain Group'] != 'Yahoo']


df = df.reindex_axis(['Email','Title','Forename','Surname','Ad1','Ad2','Ad3','Town','County','Postcode','PHONE','MOBILE','Gender','DOB','Source','IP','EmailOptInDate','Domain Group','EmailConfidence','EmailSource','Optin Year'], axis=1)
df_list = df_list.reindex_axis(['Domain','Auto_Check'], axis=1)




df.columns = ['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','Source','IP','JOINDATE','Domain Group','EmailConfidence','EmailSource','Optin Year']




df['Domain'] = df['EMAIL'].str.split('@').str[1]

df['Domain'] = df['Domain'].str.lower()

df = pd.merge(df, df_list, on='Domain', how='left').merge(source, on='EmailSource', how='left')

df.loc[(df['EMAIL'].str.contains('penis|junk|spam|survey|shop|loan|comp|backup|trash|crap|shit|account|signup|register|garbage|invalid|test|product|lending|fuck|count|bastard|pussy|bastard', na=False)), 'Temp'] = 'TRUE'
df.loc[(df['EMAIL'].str.contains('penis|junk|spam|survey|backup|trash|crap|shit|signup|register|garbage|invalid|' 
                                 'enquiries|competition|compmail|shopaholic|shoppin|shopper|contest|quote|fake|shopoholic|competions|' 
                                 'lovestoshop|liketoshop|shopofholics|shoppaholic|producttest|cashback|shopalcoholic|testing|trolling|born2shop|bullshit', na=False)), 'Temp'] = 'REMOVE'

df.loc[(df['EMAIL'].str.contains('sloan', na=False)) & (df['LASTNAME'].str.contains('Sloan', na=False)), 'Temp'] = ''
df.loc[(df['EMAIL'].str.contains('compton', na=False)) & (df['Temp'] == "TRUE"), 'Temp'] = ''
df.loc[(df['EMAIL'].str.contains('bishop', na=False)) & (df['LASTNAME'].str.contains('Bishop', na=False)), 'Temp'] = ''
df.loc[(df['Domain'].str.contains('shop', na=False)), 'Temp'] = ''

df['Source'] = df['Source_1']

df.columns = map(str.upper, df.columns)

#df.loc[((df['EMAIL'].str.contains('sloan', na=False)) & (df['LASTNAME'].str.contains('Sloan', na=False))), 'Temp'] = ''
#df.loc[((df['EMAIL'].str.contains('compton', na=False)) & (df['Temp'] == "TRUE")), 'Temp'] = ''
#df.loc[((df['EMAIL'].str.contains('bishop', na=False)) & (df['LASTNAME'].str.contains('Bishop', na=False))), 'Temp'] = ''
#df.loc[(df['Domain'].str.contains('shop', na=False)), 'Temp'] = ''

#df.set_index('EMAIL', inplace=True)
#df.to_csv(r'C:\Users\Elwyne\Downloads\DOD\DOD_Update_7\result.csv')


df1 = df.loc[((df['EMAILCONFIDENCE'] == 'medium') | (df['EMAILCONFIDENCE'] == 'high')) & (df['OPTIN YEAR'] == 2017)]
df2 = df[(df['EMAILCONFIDENCE'] == 'high') & (df['OPTIN YEAR'] != 2017)]
df3 = df[(df['EMAILCONFIDENCE'] == "") & (df['OPTIN YEAR'] == 2017)]

df1.set_index('EMAIL', inplace=True)
df2.set_index('EMAIL', inplace=True)
df3.set_index('EMAIL', inplace=True)

df1.to_csv(r'C:\Users\Elwyne\Downloads\DOD\DOD_Update_7\med_high_2017.csv')
df2.to_csv(r'C:\Users\Elwyne\Downloads\DOD\DOD_Update_7\high_rest.csv')
df3.to_csv(r'C:\Users\Elwyne\Downloads\DOD\DOD_Update_7\null_2017.csv')

