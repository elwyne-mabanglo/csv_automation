import pandas as pd
import csv
import ssl
import re
import requests
import time
import numpy as np
import datetime

i = datetime.datetime.now()

date = '%s/%s/%s' %( i.day, i.month, i.year)

from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

print(30 * '-')
print("   M A I N - M E N U")
print(30 * '-')
print("1. UK Domain Analysis")
print("2. US Domain Analysis")
print("3. Exit")
print(30 * '-')


def domain_analysis(domain_input, country_input, fuzzy_input, web_input):

    # Start clock.
    start_time = time.clock()
    print('\nTimer Start!')

    # Import Domains CSV.
    domain_list = pd.read_csv(domain_input, keep_default_na=False, na_values=['_'])
    print("Domain list link successful!")

    if country_input == 'us':

        # Import US History CSV.
        domain_history = pd.read_csv("../Data/US.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
        print("US Domain History link successful!")

        # Import US MX History CSV.
        mx_history = pd.read_csv("../Data/us_mx_history.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
        print("MX History link successful!")

        # Import Good MX String, used to find string within Primary MX Column
        with open('../Data/good_us.csv', 'r') as f:
            good_list_us = [row[0] for row in csv.reader(f)]
            print("Good US string domain list link successful!")

        # import us counties string list into variable
        with open('../Data/us_county.csv', 'r') as f:
            county_list = [row[0] for row in csv.reader(f)]
            print("US county string list link successful!")

    elif country_input == 'uk':

        domain_history = pd.read_csv("../Data/UK.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
        print("UK Domain History link successful!")

        # import mx history list
        mx_history = pd.read_csv("../Data/uk_mx_history.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
        print("MX History link successful!")

        # import good mx string list into variable
        with open('../Data/good_uk.csv', 'r') as f:
            good_list_uk = [row[0] for row in csv.reader(f)]
            print("Good UK string domain list link successful!")

        # import uk counties string list into variable
        with open('../Data/uk_county.csv', 'r') as f:
            county_list = [row[0] for row in csv.reader(f)]
            print("UK county string list link successful!")

    # import expired domain string list into variable
    with open('../Data/expired.csv', 'r') as f:
        expired_list = [row[0] for row in csv.reader(f)]
        print("Expired string domain list link successful!")

    # import tld countries
    with open('../Data/tld.csv', 'r') as f:
        tld_list = [row[0] for row in csv.reader(f)]
        print("TLD string list link successful!")

    # import tld countries
    with open('../Data/tld_uk.csv', 'r') as f:
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
    df['Current Date'] = pd.to_datetime(df['Current Date'], format="%d/%m/%Y")
    df['Last Checked'] = pd.to_datetime(df['Last Checked'], infer_datetime_format=True)
    df['No. Days'] = (df['Current Date'] - df['Last Checked']).astype('timedelta64[D]')
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

    # Check if Primary contains Good MX String
    if country_input == 'us':
        # Good US MX
        df.loc[(df[' Primary MX Server'].str.contains('|'.join(good_list_us), na=False)), 'Good MX'] = 'OK'
    elif country_input == 'uk':
        # Good UK MX
        df.loc[(df[' Primary MX Server'].str.contains('|'.join(good_list_uk), na=False)), 'Good MX'] = 'OK'
    print("Primary MX Server String Search Completed!")

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
           | df[' Primary MX Server'].str.contains('1bp.com|2bp.com|3bp.com|4bp.com'), 'Auto_Check'] = 'Spam Trap'
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

    # # # # # # # # # # # #
    # COUNTRY SELECTED US #
    # # # # # # # # # # # #

    if country_input == 'us':

        # WHITELISTED & GOODMX STATUS - All status to foriegn, as these fields are only application to US data
        df.loc[(df['Status'] == 'Whitelisted') | (df['Status'] == 'GoodMX')
               & (df['Auto_Check'] == ''), 'Auto_Check'] = 'Foreign'
        print("Whitelisted & Goodmx Status Analysis Completed!")

        # UNKNOWN STATUS - FOREIGN - Check k12 
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
               & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))
               & (df['Auto_Check'] == '')
               & ((df['Domain'].str.contains('k12', na=False))
                | (df[' Primary MX Server'].str.contains('k12', na=False))), 'Auto_Check'] = 'Exclude'

        # UNKNOWN STATUS - FOREIGN - Check domain is Foreign by checking the address/countruy
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
              & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
                 | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
               & (df[' Country'] != '')
               & ((df[' Country'] != 'us') & (df[' Country'] != 'united states') & (df[' Country'] != 'unitedstates')), 'Auto_Check'] = 'Foreign'

        # UNKNOWN STATUS - FOREIGN - Check domain is Foreign by checking the MX TLD
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
              & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
                 | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
               & (df['Primary MX Server TLD'].str.len() == 2)
               & (df['Primary MX Server TLD'].str.contains('|'.join(tld_list))), 'Auto_Check'] = 'Foreign'

        # UNKNOWN STATUS - OK - Check domains is OK
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
              & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
                 | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
               & ((df['Comparison'] == 'TRUE')
                  | (df[' Primary MX Server'].str.contains('|'.join(good_list_us))))
               & (((df[' Country'] == 'us') | (df[' Country'] == 'united states') | (df[' Country'] == 'unitedstates'))
                  | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('states')))), 'Auto_Check'] = 'OK'

        # UNKNOWN STATUS - OK - Check domains is OK
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
              & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
                 | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
               & (((df[' Country'] == 'us') | (df[' Country'] == 'united states') | (df[' Country'] == 'unitedstates'))
                  | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('states')))), 'Auto_Check'] = 'OK'

        ## UNKNOWN STATUS - UNKNOWN US - Check domains is Unknown US
        #df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
        #      & ((((df['Domain History'].isnull()) | (df['Domain History'] == '')) & (df['Auto_Check'] == '')) 
        #         | ((df['Auto_Check'] == 'MXChanged') & ((df[' Primary MX Server'].notnull()) | (df[' Primary MX Server'] != ''))))
        #      & ((df[' Country'] == '') & (df[' Address'] == '')) 
        #      & (((df['Comparison'] == 'TRUE')
        #          | (df[' Primary MX Server'].str.contains('|'.join(good_list_us)))
        #          | (df['MX History'] == 'OK')
        #          | (df['TLD'].str.contains('us'))
        #          | (df['Primary MX Server TLD'] == 'us')
        #          | (df['Primary MX Server TLD'] == 'edu'))), 'Auto_Check'] = 'Unknown US'

        # UNKNOWN STATUS - UNKNOWN US - Check domains is Unknown US
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
               & ((df['Domain History'].isnull()) | (df['Domain History'] == ''))
               & ((df['Registrant'].str.contains('privacy')) & ((df[' Country'] == 'ca') | (df[' Country'] == 'canada'))), 'Auto_Check'] = 'Unknown US'

        # UNKNOWN STATUS - Check Unknown US has address/country if TRUE status to OK
        df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
               & (df['Domain History'] == 'Unknown US')
               & (((df[' Country'] == 'us') | (df[' Country'] == 'united states') | (df[' Country'] == 'unitedstates'))
                   | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('states')))), 'Auto_Check'] = 'OK'
        print("Unknown Status Analysis Complete!")

    # # # # # # # # # # # #
    # COUNTRY SELECTED UK #
    # # # # # # # # # # # #

    if country_input == 'uk':

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

    ## UNKNOWN STATUS - UNKNOWN - Check if the A Count = 0 | Domain = TRUE set status to Unknown
    #df.loc[((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))
    #       & ((df['Domain History'].isnull()) | (df['Domain History'] == ''))
    #       & (df['Auto_Check'] == '')
    #       & ((df[' A Count'] == '0') | (df[' Domain Not Found'] == 'TRUE')), 'Auto_Check'] = 'Unknown'

    # combine domain history with auto check
    df.loc[(df['Domain History'].notnull()) & (df['Auto_Check'] == '') , 'Auto_Check'] = df['Domain History']

    df.loc[(df['WebScrap'] == '') & ((df['Auto_Check'] == '') | (df['Auto_Check'] == 'Unknown') | (df['Auto_Check'] == 'MXChanged') | ((df[' Primary MX Server'].str.contains('secureserver')) & ((df['Auto_Check'] == 'OK') | (df['Auto_Check'] == 'Unknown UK') | (df['Auto_Check'] == 'Unknown US')))), 'Auto_Check'] = 'Blank'

    df.loc[(df['WebScrap'] == '') & (((df['MX History'].isnull()) | (df['MX History'] == '')) & (df['Auto_Check'] == 'Foreign') & ((df['Status'] == 'Unknown') | (df['Status'] == 'Bad WhoIs'))) , 'Auto_Check'] = 'Blank'

    #df.to_csv('../Data/results54.csv')

    df.loc[(df['WebScrap'] == 'TRUE'), 'Auto_Check'] = 'Blank'

    def checkPage(domain,country,stage):
    

        if country == "uk":
            regexlist = [
                         re.compile(r'(\W(\+44\s?\(?\[?0\]?\)?([\s?\d]{4}[\s?\d]{7}|[\d]{2}[\s?\d]{5}[\s?\d]{5}))\W)',re.IGNORECASE),
                         re.compile(r'(\W\(?\+44\s?\)?[\d]{4}\s?[\d]{3}\s?[\d]{3}\W)',re.IGNORECASE),
                         re.compile(r'(\W((\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+44\s[\d]{2}\s[\d]{4}\s[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W((0[\d]{4})((\s[\d]{4}[\d]{3})|(\s[\d]{7})|(\s[\d]{4}[\s\d]{4})))\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+?[\s?]44[\s?]\(?\[?0\]?\)?[\s?\d]{5}[\s?\d]{4}[\s?\d]{4})\W)',re.IGNORECASE),                   
                         re.compile(r'(\W((0800|0808|0843|0844|0845|0870|0871|0872|0873)(\-?\s?([\d]{3}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{4}\-?\s?[\d]{2}|[\d]{2}\-?\s?[\d]{4})|\s?0\-?\s?([\d]{2}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{3}\-?\s?[\d]{3})))\W)',re.IGNORECASE),
                         re.compile(r'(\W(\(?\[?0\]?\)?[\d]{3}\s?[\d]{2}\s?[\d]{2}\s?[\d]{3})\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+\s[\d]{2}\s\(0\)[\d]{4}\s[\d]{6})\W)',re.IGNORECASE),
                         re.compile(r'(\W([\d]{4}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W(0[\d]{3}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W(0[\d]{4}\s[\d]{6})\W)',re.IGNORECASE)  
                         ]

            regexforiegn = [
                         re.compile(r'(united\s?states|america|american|\susa\s)',re.IGNORECASE),
                         re.compile(r'(\W(\(?[\d]{3}\)?(\.|\-)[\d]{3}(\.|\-)[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W[\d]{3}-[\d]{3}-[\d]{4}\W)',re.IGNORECASE)
                         #re.compile(r'(À|Á|Â|Ã|Ä|Å|Æ|Ç|È|É|Ê|Ë|Ì|Í|Î|Ï|à|á|â|ã|ä|å|æ|ç|è|é|ê|ë|ì|í|î|ï|Ð|Ñ|Ò|Ó|Ô|Õ|Ö|Ø|Ù|Ú|Û|Ü|Ý|Þ|ß|ð|ñ|ò|ó|ô|õ|ö|ø|ù|ú|û|ü|ý|þ|ÿ)',re.IGNORECASE | re.VERBOSE | re.UNICODE),
                         #re.compile(r'(ア|イ|ウ|エ|オ|カ|キ|ク|ケ|コ|サ|シ|ス|セ|ソ|ガ|ギ|グ|ゴ|パ|ピ|プ|ペ|ポ)',re.IGNORECASE | re.VERBOSE | re.UNICODE),
                         #re.compile(r'(川|月|木|心|火|左|北|今|名|美|見|外|成|空|明|静|海|雲|新|語|道|聞|強|飛)',re.IGNORECASE | re.VERBOSE | re.UNICODE)
                         #re.compile(r'([\u4e00-\u9fff]+)',re.IGNORECASE | re.VERBOSE), 
                         #re.compile(r'([^\x00-\x7F]+)',re.IGNORECASE | re.VERBOSE), 
                         #re.compile(r'([\u0400-\u04FF])',re.IGNORECASE | re.VERBOSE)
                         ]
        else:
            regexlist = [
                         re.compile(r'(united\s?states|america|american|\susa\s)',re.IGNORECASE),
                         re.compile(r'(\W(\(?[\d]{3}\)?(\.|\-)[\d]{3}(\.|\-)[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W[\d]{3}-[\d]{3}-[\d]{4}\W)',re.IGNORECASE)              
                         ]

            regexforiegn = [
                         re.compile(r'(\W(\+44\s?\(?\[?0\]?\)?([\s?\d]{4}[\s?\d]{7}|[\d]{2}[\s?\d]{5}[\s?\d]{5}))\W)',re.IGNORECASE),
                         re.compile(r'(\W\(?\+44\s?\)?[\d]{4}\s?[\d]{3}\s?[\d]{3}\W)',re.IGNORECASE),
                         re.compile(r'(\W((\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+44\s[\d]{2}\s[\d]{4}\s[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W((0[\d]{4})((\s[\d]{4}[\d]{3})|(\s[\d]{7})|(\s[\d]{4}[\s\d]{4})))\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+?[\s?]44[\s?]\(?\[?0\]?\)?[\s?\d]{5}[\s?\d]{4}[\s?\d]{4})\W)',re.IGNORECASE),                   
                         re.compile(r'(\W((0800|0808|0843|0844|0845|0870|0871|0872|0873)(\-?\s?([\d]{3}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{4}\-?\s?[\d]{2}|[\d]{2}\-?\s?[\d]{4})|\s?0\-?\s?([\d]{2}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{3}\-?\s?[\d]{3})))\W)',re.IGNORECASE),
                         re.compile(r'(\W(\(?\[?0\]?\)?[\d]{3}\s?[\d]{2}\s?[\d]{2}\s?[\d]{3})\W)',re.IGNORECASE),
                         re.compile(r'(\W(\+\s[\d]{2}\s\(0\)[\d]{4}\s[\d]{6})\W)',re.IGNORECASE),
                         re.compile(r'(\W([\d]{4}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE),
                         re.compile(r'(\W(0[\d]{3}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE)
                        # re.compile(r'(À|Á|Â|Ã|Ä|Å|Æ|Ç|È|É|Ê|Ë|Ì|Í|Î|Ï|à|á|â|ã|ä|å|æ|ç|è|é|ê|ë|ì|í|î|ï|Ð|Ñ|Ò|Ó|Ô|Õ|Ö|Ø|Ù|Ú|Û|Ü|Ý|Þ|ß|ð|ñ|ò|ó|ô|õ|ö|ø|ù|ú|û|ü|ý|þ|ÿ)',re.IGNORECASE | re.VERBOSE | re.UNICODE),
                         #re.compile(r'(ア|イ|ウ|エ|オ|カ|キ|ク|ケ|コ|サ|シ|ス|セ|ソ|ガ|ギ|グ|ゴ|パ|ピ|プ|ペ|ポ)',re.IGNORECASE | re.VERBOSE | re.UNICODE),
                         #re.compile(r'(川|月|木|心|火|左|北|今|名|美|見|外|成|空|明|静|海|雲|新|語|道|聞|強|飛)',re.IGNORECASE | re.VERBOSE | re.UNICODE)
                         #re.compile(r'([\u4e00-\u9fff]+)',re.IGNORECASE | re.VERBOSE), 
                         #re.compile(r'([^\x00-\x7F]+)',re.IGNORECASE | re.VERBOSE), 
                         #re.compile(r'([\u0400-\u04FF])',re.IGNORECASE | re.VERBOSE)
                         ]

        regexTemp = [
                    re.compile(r'(domain\s?for\s?sale)',re.IGNORECASE),
                    re.compile(r'(domain\s?expired)',re.IGNORECASE),
                    re.compile(r'(website\s?for\s?sale)',re.IGNORECASE),
                    re.compile(r'(is\s?this\s?your\s?domain)',re.IGNORECASE),
                    re.compile(r'(this\s?domain)',re.IGNORECASE),
                    re.compile(r'(domain\s?name\s?may\s?be\s?for\s?sale)',re.IGNORECASE)         
                    ]

        for regex in regexforiegn:        
            m = re.search(regex, domain)
            if m:
                print('Regex Match: ')                
                print(regex)

                s = domain = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M)

                #print('Match: ' + m.group(1))
                return ['WEB FOREIGN', s]
                break

        for regex in regexlist:           
            m = re.search(regex, domain)
            if m:
                print('Regex Match: ')
                print(regex)
                s = domain = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M)

                #print('Match: ' + m.group(1))
                return ['WEB TRUE', s]
                break

        for regex in regexTemp:           
            m = re.search(regex, domain)
            if m:
                print('Regex Match: ')
                print(regex)
                s = domain = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M)

                #print('Match: ' + m.group(1))
                return ['WEB TEMP', s]
                break

        for county in county_list:
            if re.search(county, data):                              
                print('Country Match: ' + county)
                return ['WEB TRUE',county]
                break
               
        if stage == 1:
            return ['','']
        else:
            return ['WEB UNKNOWN','WEB UNKNOWN']

    if web_input == '1':

        print('Web Check Computing...')
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}  
             
        tempweb = []
        tempMatch = []
        tempDate = []

        for item in df.itertuples():      
            if item[23] == 'Blank':
                tempDate.append(date)
                raw_domain = 'http://www.' + item[1] + '/'
                print('-----------------------')
                print('Domain: ' + raw_domain)
                try:
                    r = requests.get(raw_domain, timeout=5, allow_redirects=True, headers=headers)
                    r.encoding
                    data = r.text
                    soup = BeautifulSoup(data, "lxml")
                except:
                    tempweb.append('FAILED')
                    tempMatch.append('FAILED')
                    print('1 Status: FAILED TO OPEN')
                else:
                    try:
                        element = soup.find('meta', attrs={'http-equiv': 'refresh'})
                        if element:
                            refresh = element['content']
                            url = (refresh.partition('=')[2]).strip()
                        else:
                            url = ''
                    except:
                           tempweb.append('FAILED')
                           tempMatch.append('FAILED')
                           print('2 Status: FAILED TO OPEN')
                    else:
                        if url:
                            if url.find('https') == -1:
                                if url.find('http') == -1:
                                    if url.find('/') == -1:
                                        url = (raw_domain + '/' + url)
                                    else:
                                        url = (raw_domain + url)
                            print('Redirect Page: ' + url)
                            try:
                                r = requests.get(url, timeout=5, allow_redirects=True)
                                r.encoding
                                data = r.text
                                soup = BeautifulSoup(data, "lxml")
                            except:
                                tempweb.append('FAILED')
                                tempMatch.append('FAILED')
                                print('3 Status: FAILED TO OPEN')
                            else:
                                m,x = checkPage(data,country_input,1)
                                if m:
                                    tempweb.append(m)
                                    tempMatch.append(x)
                                    print('4 Match: ' + x)
                                    print(m)
                                else:
                                    link = soup.find_all('a', href=re.compile("contact", re.IGNORECASE), limit=1)
                                    if link:
                                        cleanurl = link[0].get('href')
                                        if cleanurl.find('https') == -1:
                                            if cleanurl.find('http') == -1:
                                                m = re.search('/', cleanurl, re.IGNORECASE)
                                                if m:
                                                    cleanurl = cleanurl[1:]
                                                    cleanurl = cleanurl.lower().strip().replace('\s+', '')
                                                    cleanurl = (raw_domain + cleanurl)
                                                else:
                                                    cleanurl = (raw_domain + '/' + cleanurl)
                                        print('5 Contact Page: ' + cleanurl)
                                        try:
                                            r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                                            data = r.text
                                        except:
                                            tempweb.append('FAILED')
                                            tempMatch.append('FAILED')
                                            print('6 Status: FAILED TO OPEN CONTACT')
                                        else:
                                            m,x = checkPage(data,country_input,1)
                                            if m:
                                                tempweb.append(m)
                                                tempMatch.append(x)
                                                print('7 Match: ' + x)
                                                print('Status: ' + m)
                                            else:
                                                m,x = checkPage(data,country_input,0)
                                                if m:
                                                    tempweb.append(m)
                                                    tempMatch.append(x)
                                                    print('8 Match: ' + x)
                                                    print('Status: ' + m)
                                    else:
                                        m,x = checkPage(data,country_input,1)
                                        if m:
                                            tempweb.append(m)
                                            tempMatch.append(x)
                                            print('9 Match: ' + x)
                                            print('Status: ' + m)
                                        else:
                                            m,x = checkPage(data,country_input,0)
                                            if m:
                                                tempweb.append(m)
                                                tempMatch.append(x)
                                                print('10 Match: ' + x)
                                                print('Status: ' + m)
                        else:
                            m,x = checkPage(data,country_input,1)
                            if m:
                                tempweb.append(m)
                                tempMatch.append(x)
                                print('11 Match: ' + x)
                                print('Status: ' + m)                                
                            else:
                                link = soup.find_all('a', href=re.compile("contact", re.IGNORECASE), limit=1)
                                if link:
                                    cleanurl = link[0].get('href')
                                    if cleanurl.find('https') == -1:
                                        if cleanurl.find('http') == -1:
                                            m = re.search('/', cleanurl, re.IGNORECASE)
                                            if m:
                                                cleanurl = cleanurl[1:]
                                                cleanurl = cleanurl.lower().strip().replace('\s+', '')
                                                cleanurl = (raw_domain + cleanurl)
                                            else:
                                                cleanurl = (raw_domain + '/' + cleanurl)
                                    print('Contact Page: ' + cleanurl)
                                    try:
                                        r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                                        data = r.text
                                    except:
                                        tempweb.append('FAILED')
                                        tempMatch.append('FAILED')
                                        print('12 Status: FAILED TO OPEN CONTACT' + raw_domain)
                                    else:
                                        m,x = checkPage(data,country_input,1)                                      
                                        if m:
                                            tempweb.append(m)
                                            tempMatch.append(x)
                                            print('13 Match: ' + x)
                                            print('Status: ' + m)                                   
                                        else:    
                                            m,x = checkPage(data,country_input,0)
                                            if m:
                                                tempweb.append(m)
                                                tempMatch.append(x)
                                                print('14 Match: ' + x)
                                                print('Status: ' + m)
                                else:
                                    m,x = checkPage(data,country_input,0)
                                    if m:
                                        tempweb.append(m)
                                        tempMatch.append(x)
                                        print('15 Match: ' + x)
                                        print('Status: ' + m)
            else:
                tempweb.append('SKIP')  
                tempMatch.append('SKIP')
                tempDate.append(item[19])

        df['Web Check'] = tempweb
        df['Match'] = tempMatch
        df['Last Checked'] = tempDate

        # Check both auto check = Blank & Web Check = True set Status to Unknown US
        df.loc[(df['Web Check'] == 'WEB TRUE'), 'Auto_Check'] = 'OK'
        df.loc[(df['Web Check'] == 'WEB TEMP'), 'Auto_Check'] = 'Unknown'
        df.loc[(df['Web Check'] == 'WEB FOREIGN'), 'Auto_Check'] = 'Foreign'
        df.loc[(df['Web Check'] == 'FAILED'), 'Auto_Check'] = 'Unknown'
        df.loc[(df['Web Check'] == 'WEB FALSE'), 'Auto_Check'] = 'Unknown'  
        df.loc[(df['Web Check'] == 'WEB UNKNOWN'), 'Auto_Check'] = 'Unknown'  

        if country_input == 'uk':
            df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN') | (df['Web Check'] == 'WEB TEMP'))
                   & (df[' Country'] != '')
                   & ((df[' Country'] != 'uk') 
                      & (df[' Country'] != 'united kingdom') 
                      & (df[' Country'] != 'gb')
                      & (df[' Country'] != 'ie')
                      & (df[' Country'] != 'im')
                      & ~(((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom'))) | (df[' Address'].str.contains('ireland')))), 'Auto_Check'] = 'Foreign'

            df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN') | (df['Web Check'] == 'WEB TEMP'))
                   & (df['Primary MX Server TLD'].str.contains('|'.join(tld_uk))), 'Auto_Check'] = 'Foreign'

            df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN') | (df['Web Check'] == 'WEB TEMP'))
                   & ((df[' Country'].str.contains('uk|united kingdom|gb|ie|im'))
                    | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom')))
                    | (df[' Address'].str.contains('ireland'))
                    | (df['Primary MX Server TLD'].str.contains('uk|united kingdom|gb|ie|im'))), 'Auto_Check'] = 'Unknown UK'

            df.loc[(df['Match'].str.contains("sale")), 'Auto_Check'] = 'Foreign'

        #tempweb1 = []

        #for item in df.itertuples():
        #    if item[19] == 'Blank':
        #        raw_domain = 'http://www.' + item[1] + '/'
        #        try:
        #            r = requests.get(raw_domain, timeout=5, allow_redirects=True)
        #            data = r.text
        #            soup = BeautifulSoup(data, "lxml")
        #        except:
        #            tempweb1.append('WEB FALSE')
        #            print('0  OPEN   FALSE ' + raw_domain)
        #        else:
        #            try:
        #                element = soup.find('meta', attrs={'http-equiv': 'refresh'})
        #                if element:
        #                    refresh = element['content']
        #                    url = (refresh.partition('=')[2]).strip()
        #                else:
        #                    url = ''
        #            except:
        #                   tempweb1.append('WEB FALSE')
        #                   print('1  OPEN   FALSE ' + raw_domain)
        #            else:
        #                if url:
        #                    if url.find('https') == -1:
        #                        if url.find('http') == -1:
        #                            if url.find('/') == -1:
        #                                url = (raw_domain + '/' + url)
        #                            else:
        #                                url = (raw_domain + url)
        #                    try:
        #                        r = requests.get(url, timeout=5, allow_redirects=True)
        #                        data = r.text
        #                        soup = BeautifulSoup(data, "lxml")
        #                    except:
        #                        tempweb1.append('WEB FALSE')
        #                        print('2  OPEN   FALSE ' + raw_domain)
        #                    else:
        #                        found = 'False'
        #                        for county in county_list:
        #                            if re.search(county, data):                              
        #                                found = 'True'
        #                                print('Found: ' + county)
        #                                break
        #                        if found == 'True':
        #                            tempweb1.append('WEB TRUE')
        #                            print('3  COUNTY TRUE  ' + raw_domain)
        #                        else:
        #                            link = soup.find_all('a', href=re.compile("contact", re.IGNORECASE), limit=1)
        #                            if link:
        #                                cleanurl = link[0].get('href')
        #                                if cleanurl.find('https') == -1:
        #                                    if cleanurl.find('http') == -1:
        #                                        m = re.search('/', cleanurl, re.IGNORECASE)
        #                                        if m:
        #                                            cleanurl = cleanurl[1:]
        #                                            cleanurl = cleanurl.lower().strip().replace('\s+', '')
        #                                            cleanurl = (raw_domain + cleanurl)
        #                                        else:
        #                                            cleanurl = (raw_domain + '/' + cleanurl)
        #                                try:
        #                                    r = requests.get(cleanurl, timeout=5, allow_redirects=True)
        #                                    data = r.text
        #                                except:
        #                                    tempweb1.append('WEB FALSE')
        #                                    print('4  OPEN   FALSE ' + raw_domain)
        #                                else:
        #                                    found = 'False'
        #                                    for county in county_list:
        #                                        if re.search(county, data):                              
        #                                            found = 'True'
        #                                            print('Found: ' + county)
        #                                            break
        #                                    if found == 'True':
        #                                        tempweb1.append('WEB TRUE')
        #                                        print('5  COUNTY TRUE  ' + raw_domain)
        #                                    else:
        #                                        tempweb1.append('WEB FALSE')
        #                                        print('6  COUNTY FALSE ' + raw_domain)
        #                            else:
        #                                tempweb1.append('WEB FALSE')
        #                                print('7  COUNTY FALSE ' + raw_domain)
        #                else:
        #                    found = 'False'
        #                    for county in county_list:
        #                        if re.search(county, data):                              
        #                            found = 'True'
        #                            print('Found: ' + county)
        #                            break
        #                    if found == 'True':
        #                        tempweb1.append('WEB TRUE')
        #                        print('8  COUNTY TRUE  ' + raw_domain)
        #                    else:
        #                        link = soup.find_all('a', href=re.compile("contact", re.IGNORECASE), limit=1)
        #                        if link:
        #                            cleanurl = link[0].get('href')
        #                            if cleanurl.find('https') == -1:
        #                                if cleanurl.find('http') == -1:
        #                                    m = re.search('/', cleanurl, re.IGNORECASE)
        #                                    if m:
        #                                        cleanurl = cleanurl[1:]
        #                                        cleanurl = cleanurl.lower().strip().replace('\s+', '')
        #                                        cleanurl = (raw_domain + cleanurl)
        #                                    else:
        #                                        cleanurl = (raw_domain + '/' + cleanurl)
        #                            try:
        #                                r = requests.get(cleanurl, timeout=5, allow_redirects=True)
        #                                data = r.text
        #                            except:
        #                                tempweb1.append('WEB FALSE')
        #                                print('9  OPEN   FALSE ' + raw_domain)
        #                            else:
        #                                found = 'False'
        #                                for county in county_list:
        #                                    if re.search(county, data):                              
        #                                        found = 'True'
        #                                        print('Found: ' + county)
        #                                        break
        #                                if found == 'True':
        #                                    tempweb1.append('WEB TRUE')
        #                                    print('10 COUNTY TRUE  ' + raw_domain)
        #                                else:
        #                                    tempweb1.append('WEB FALSE')
        #                                    print('11 COUNTY FALSE ' + raw_domain)
        #                        else:
        #                            tempweb1.append('WEB FALSE')
        #                            print('12 COUNTY FALSE ' + raw_domain)
        #    else:
        #        tempweb1.append('SKIP')

        #df['Web County'] = tempweb1

        ## Check both auto check = Blank & Web Check = True set Status to Unknown US
        #df.loc[(df['Web County'] == 'WEB TRUE'), 'Auto_Check'] = 'OK'

        ## Remaining Blank set status to Unknown
        #df.loc[(df['Web County'] == 'WEB FALSE'), 'Auto_Check'] = 'Unknown'

        #if country_input == 'uk':
        #    df.loc[((df['Web County'] == 'WEB FALSE') | (df['Web County'] == 'FAILED'))
        #           & (df[' Country'] != '')
        #           & ((df[' Country'] != 'uk') 
        #              & (df[' Country'] != 'united kingdom') 
        #              & (df[' Country'] != 'gb')
        #              & (df[' Country'] != 'ie')
        #              & (df[' Country'] != 'im')
        #              & ~(((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom'))) | (df[' Address'].str.contains('ireland')))), 'Auto_Check'] = 'Foreign'

        #    df.loc[((df['Web County'] == 'WEB FALSE') | (df['Web County'] == 'FAILED'))
        #           & (df['Primary MX Server TLD'].str.contains('|'.join(tld_uk))), 'Auto_Check'] = 'Foreign'

        #    df.loc[(df['Web County'] == 'WEB FALSE') 
        #           & ((df[' Country'].str.contains('uk|united kingdom|gb|ie|im'))
        #            | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom')))
        #            | (df[' Address'].str.contains('ireland'))
        #            | (df['Primary MX Server TLD'].str.contains('uk|united kingdom|gb|ie|im'))), 'Auto_Check'] = 'Unknown UK'
   
    # UK Fuzzy Match
    if fuzzy_input == '1':
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

            print('Checking: ' + item[1])

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

    # US Fuzzy Match
    if fuzzy_input == '2':

        print('Fuzzy Match Computing...')
        gmail = []
        googlemail = []
        hotmail = []
        yahoo = []
        ymail = []
        live = []
        msn = []
        aol = []
        verizon = []
        earthlink = []
        comcast = []
        netzero = []
        juno = []
        icloud = []
        outlook = []
        rocketmail = []
        coxmail = []
        compuserve= []


        for item in df.itertuples():

            print('Checking: ' + item[1])

            gmail.append(fuzz.partial_ratio(item[1], "gmail"))
            googlemail.append(fuzz.partial_ratio(item[1], "google"))
            hotmail.append(fuzz.partial_ratio(item[1], "hotmail"))
            yahoo.append(fuzz.partial_ratio(item[1], "yahoo"))
            ymail.append(fuzz.partial_ratio(item[1], "ymail"))
            live.append(fuzz.partial_ratio(item[1], "live"))
            msn.append(fuzz.partial_ratio(item[1], "msn"))
            aol.append(fuzz.partial_ratio(item[1], "aol"))
            verizon.append(fuzz.partial_ratio(item[1], "verizon"))
            earthlink.append(fuzz.partial_ratio(item[1], "earthlink"))
            comcast.append(fuzz.partial_ratio(item[1], "comcast"))
            netzero.append(fuzz.partial_ratio(item[1], "netzero"))
            juno.append(fuzz.partial_ratio(item[1], "juno"))
            icloud.append(fuzz.partial_ratio(item[1], "icloud"))
            outlook.append(fuzz.partial_ratio(item[1], "outlook"))
            rocketmail.append(fuzz.partial_ratio(item[1], "rocketmail"))
            coxmail.append(fuzz.partial_ratio(item[1], "coxmail"))
            compuserve.append(fuzz.partial_ratio(item[1], "compuserve"))

        df['gmail'] = gmail
        df['googlemail'] = googlemail
        df['hotmail'] = hotmail
        df['yahoo'] = yahoo
        df['ymail'] = ymail
        df['live'] = live
        df['msn'] = msn
        df['aol'] = aol
        df['verizon'] = verizon
        df['earthlink'] = earthlink
        df['comcast'] = comcast
        df['netzero'] = netzero
        df['juno'] = juno
        df['icloud'] = icloud
        df['outlook'] = outlook
        df['rocketmail'] = rocketmail
        df['coxmail'] = coxmail
        df['compuserve'] = compuserve

        print("Fuzzy Match Complete!")       
        
    if country_input == 'uk':

        df = df.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                              ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                              'Registrant',' Organisation',' Address',' Country','Registry',
                              ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','No. Days','Web Check','Match','WebScrap','MX History','Good MX',
                              'Primary MX Server TLD','Comparison',
                              'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                              'btinternet','virginmedia','icloud','talktalk','tiscali','sky','blueyonder',
                              'ntlworld','talk21','outlook','rocketmail'], axis=1)

        print('Reindex Complete!')

    if country_input == 'us':

        df = df.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                              ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                              'Registrant',' Organisation',' Address',' Country','Registry',
                              ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','No. Days','Web Check','Match','WebScrap','MX History','Good MX',
                              'Primary MX Server TLD','Comparison',
                              'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                              'verizon','earthlink','comcast','netzero','juno','icloud','outlook','rocketmail','coxmail','compuserve'], axis=1)

        print('Reindex Complete!')

    df.set_index('Domain', inplace=True)
    print('Index Set!')

    # output results to csv file
    df.to_csv('../Data/results.csv')
    print("Completed! ", time.clock() - start_time, "seconds")


while True:
    selection = input("\nPlease Select:")
    if selection == '1':
        fuzzySelected = input("\nEnable Fuzzy Match? (Yes = 1 | No = 0):")
        webSelected = input("\nEnable Web Match? (Yes = 1 | No = 0):")
        # =input('Please enter domain file name:')
        user_input = '../Data/R.csv'
        country_selected = 'uk'
        domain_analysis(user_input, country_selected, fuzzySelected, webSelected)
    elif selection == '2':
        fuzzySelected = input("\nEnable Fuzzy Match? (Yes = 1 | No = 0):")
        webSelected = input("\nEnable Web Match? (Yes = 1 | No = 0):")
        # domain_input=input('Please enter domain file name:')
        user_input = '../Data/R.csv'
        country_selected = 'us'
        domain_analysis(user_input, country_selected, fuzzySelected, webSelected)
    elif selection == '3':
        break
    else:
        print("Unknown Option Selected!")
