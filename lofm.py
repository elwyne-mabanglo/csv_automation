import pandas as pd
import csv

import sys
import datetime

import tkinter 
tkinter.Tk().withdraw()

from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

#livefeed_file = r'C:\Users\Elwyne\OneDrive - Email Switchboard Ltd\domain_analysis\2_domain_analysis\CHG-General_201712178107\Livefeed_import_H7AGY5_CHG-General_201712178107.csv'
livefeed_file = askopenfilename()
livefeed = pd.read_csv(livefeed_file,keep_default_na=False, na_values=['_'],encoding ='latin1')

domain_analysis_file = askopenfilename()
#domain_analysis_file = r'C:\Users\Elwyne\OneDrive - Email Switchboard Ltd\domain_analysis\2_domain_analysis\CHG-General_201712178107\WS_Livefeed_import_H7AGY5_CHG-General_201712178107_domain_export.csv'
domain_analysis = pd.read_csv(domain_analysis_file,keep_default_na=False, na_values=['_'],encoding ='latin1')
domain_analysis = domain_analysis.reindex_axis(['Domain','Auto_Check'], axis=1)

livefeed = livefeed.reindex_axis(['Email','Title','First_Name','Last_Name','Address1','Address2','ADDRESS3','City','County','Postcode','PHONE','MOBILE','Gender','DOB','Source','IP_Address','Created_DateTime'], axis=1)
livefeed.columns = ['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','Source','IP','JOINDATE']

livefeed['Domain'] = livefeed['EMAIL'].str.split('@').str[1]
livefeed['Domain'] = livefeed['Domain'].str.lower()

livefeed = pd.merge(livefeed, domain_analysis, on='Domain', how='left')

livefeed.loc[(livefeed['EMAIL'].str.contains('penis|junk|spam|survey|shop|loan|comp|backup|trash|crap|shit|account|signup|register|garbage|invalid|test|product|lending|fuck|count|bastard|pussy|bastard', na=False)), 'Temp'] = 'TRUE'
livefeed.loc[(livefeed['EMAIL'].str.contains('penis|junk|spam|survey|backup|trash|crap|shit|signup|register|garbage|invalid|' 
                                 'enquiries|competition|compmail|shopaholic|shoppin|shopper|contest|quote|fake|shopoholic|competions|' 
                                 'lovestoshop|liketoshop|shopofholics|shoppaholic|producttest|cashback|shopalcoholic|testing|trolling', na=False)), 'Temp'] = 'REMOVE'

livefeed.loc[(livefeed['EMAIL'].str.contains('sloan', na=False)) & (livefeed['LASTNAME'].str.contains('Sloan', na=False)), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('compton', na=False)) & (livefeed['Temp'] == "TRUE"), 'Temp'] = ''
livefeed.loc[(livefeed['EMAIL'].str.contains('bishop', na=False)) & (livefeed['LASTNAME'].str.contains('Bishop', na=False)), 'Temp'] = ''
livefeed.loc[(livefeed['Domain'].str.contains('shop', na=False)), 'Temp'] = ''

livefeed.set_index('EMAIL', inplace=True)

folder = askdirectory()
livefeed.to_csv(folder + '/results.csv')