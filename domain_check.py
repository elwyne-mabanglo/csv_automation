import pandas as pd
import csv
import ssl
import re
import requests
import time
import numpy as np

from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

df = pd.read_csv("../Data/domains.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')

country_input = input("\nCountry? (UK = 1| US = 0):")

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}  
        
if country_input == '1':
    regex = re.compile(r'(\W(\+44\s?\(?\[?0\]?\)?([\s?\d]{4}[\s?\d]{7}|[\d]{2}[\s?\d]{5}[\s?\d]{5}))\W)|'
                        '(\W\(?\+44\s?\)?[\d]{4}\s?[\d]{3}\s?[\d]{3}\W)|'
                        '(\W((\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})\W)|'
                        '(\W(\+44\s[\d]{2}\s[\d]{4}\s[\d]{4})\W)|'
                        '(\W((0[\d]{4})(([\s?\d]{4}[\d]{3})|([\s?\d]{7})|([\s?\d]{4}[\s?\d]{4})))\W)|'
                        '(\W(\+?[\s?]44[\s?]\(?\[?0\]?\)?[\s?\d]{5}[\s?\d]{4}[\s?\d]{4})\W)|'
                        '(united\s?kingdom|london|£|\&pound|great\s?britain)|'
                        '(\W((0800|0808|0843|0844|0845|0870|0871|0872|0873)(\-?\s?([\d]{3}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{4}\-?\s?[\d]{2}|[\d]{2}\-?\s?[\d]{4})|\s?0\-?\s?([\d]{2}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{3}\-?\s?[\d]{3})))\W)|'
                        '(\W\(?\[?0\]?\)?[\d]{3}\s?[\d]{2}\s?[\d]{2}\s?[\d]{3}\W)',re.IGNORECASE
                        )

    with open('../Data/uk_county.csv', 'r') as f:
        county_list = [row[0] for row in csv.reader(f)]

    #regex = r'((\W(\+44\s?\(?\[?0\]?\)?([\s?\d]{4}[\s?\d]{7}|[\d]{2}[\s?\d]{5}[\s?\d]{5}))\W)|(\W((0[\d]{4})((\s[\d]{4}[\d]{3})|(\s[\d]{7})|(\s[\d]{4}\s[\d]{4})))\W)|(\W(\+?[\s?]44[\s?]\(?\[?0\]?\)?[\s?\d]{5}[\s?\d]{4}[\s?\d]{4})\W)|(\W(\?+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3}\W)|(united\s?kingdom|london|£|\&pound|great\s?britain)|(\W((0800|0808|0843|0844|0845|0870|0871|0872|0873)(\-?\s?([\d]{3}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{4}\-?\s?[\d]{2}|[\d]{2}\-?\s?[\d]{4})|\s?0\-?\s?([\d]{2}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{3}\-?\s?[\d]{3})))\W)|(\W((\(?\[?0\]?\d{4}\)?\s?\d{3}\s?\d{3})|(\(?\[?0\d{3}\]?\)?\s?\d{3}\s?\d{4})|(\(?\[?0\]?\d{2}\)?\s?\d{4}\s?\d{4}))\W)|(\W\(?\[?0\]?\)?[\d]{3}\s?[\d]{2}\s?[\d]{2}\s?[\d]{3}\W)|(\W\(?\+44\s?\)?[\d]{4}\s?[\d]{3}\s?[\d]{3}\W))'
    print('UK Regex Selected')
else:
    regex = re.compile(r'(united\s?states|america|usa)|'
                        '(\W(\(?[\d]{3}\)?(\.|\-|\s|&nbsp;)?[\d]{3}(\.|\-|\s|&nbsp;)[\d]{4})\W)|'
                        '(\W[\d]{3}-[\d]{3}-[\d]{4}\W)',re.IGNORECASE 
                        )

    with open('../Data/us_county.csv', 'r') as f:
        county_list = [row[0] for row in csv.reader(f)]

    print('US Regex Selected')

domain = r'(([\u4e00-\u9fff]+)|(domain\s?for\s?sale|domain\s?expired|website\s?for\s?sale|is\s?this\s?your\s?domain|this\s?domain|domain\s?name\s?may\s?be\s?for\s?sale))'

tempweb = []

for item in df.itertuples():
    raw_domain = 'http://www.' + item[1] + '/'
    try:
        r = requests.get(raw_domain, timeout=5, allow_redirects=True, headers=headers)
        data = r.text
        soup = BeautifulSoup(data, "lxml")
    except:
        tempweb.append('FAILED')
        print('0  OPEN   FALSE ' + raw_domain)
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
                print('1  OPEN   FALSE ' + raw_domain)
        else:
            if url:
                if url.find('https') == -1:
                    if url.find('http') == -1:
                        if url.find('/') == -1:
                            url = (raw_domain + '/' + url)
                        else:
                            url = (raw_domain + url)
                print(url)
                try:
                    r = requests.get(url, timeout=5, allow_redirects=True)
                    data = r.text
                    soup = BeautifulSoup(data, "lxml")
                except:
                    tempweb.append('FAILED')
                    print('2  OPEN   FALSE ' + raw_domain)
                else:
                    m = regex.search(data)
                    if m:
                        m = re.search(domain, data, re.IGNORECASE)
                        if m:
                            tempweb.append('FAILED')
                            print('3  OPEN   FALSE ' + raw_domain)
                        else:
                            tempweb.append('WEB TRUE')
                            print('4  REGEX  TRUE  ' + raw_domain)
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
                            try:
                                r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                                data = r.text
                            except:
                                tempweb.append('FAILED')
                                print('5  OPEN   FALSE ' + raw_domain)
                            else:
                                m = regex.search(data)
                                if m:
                                    m = re.search(domain, data, re.IGNORECASE)
                                    if m:
                                        tempweb.append('FAILED')
                                        print('6  OPEN   FALSE ' + raw_domain)
                                    else:
                                        tempweb.append('WEB TRUE')
                                        print('7  REGEX  TRUE  ' + raw_domain)
                                else:
                                    tempweb.append('WEB FALSE')
                                    print('8  REGEX  FALSE ' + raw_domain)
                        else:
                            tempweb.append('WEB FALSE')
                            print('9  REGEX  FALSE ' + raw_domain)
            else:
                m = regex.search(data)
                if m:
                    m = re.search(domain, data, re.IGNORECASE)
                    if m:
                        tempweb.append('FAILED')
                        print('10 OPEN   FALSE ' + raw_domain)
                    else:
                        tempweb.append('WEB TRUE')
                        print('11 REGEX  TRUE  ' + raw_domain)
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
                        print(cleanurl)
                        try:
                            r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                            data = r.text
                        except:
                            tempweb.append('FAILED')
                            print('12 OPEN   FALSE ' + raw_domain)
                        else:
                            m = regex.search(data)
                            if m:
                                m = re.search(domain, data, re.IGNORECASE)
                                if m:
                                    tempweb.append('FAILED')
                                    print('13 OPEN   FALSE ' + raw_domain)
                                else:
                                    tempweb.append('WEB TRUE')
                                    print('14 REGEX  TRUE  ' + raw_domain)
                            else:
                                tempweb.append('WEB FALSE')
                                print('15 REGEX  FALSE ' + raw_domain)
                    else:
                        tempweb.append('WEB FALSE')
                        print('16 REGEX  FALSE ' + raw_domain)

df['Web Check'] = tempweb

# Check both auto check = Blank & Web Check = True set Status to Unknown US
df.loc[(df['Web Check'] == 'WEB TRUE'), 'Auto_Check'] = 'OK'

df.loc[(df['Web Check'] == 'FAILED'), 'Auto_Check'] = 'Unknown'

tempweb1 = []

for item in df.itertuples():

    raw_domain = 'http://www.' + item[1] + '/'
    try:
        r = requests.get(raw_domain, timeout=5, allow_redirects=True)
        data = r.text
        soup = BeautifulSoup(data, "lxml")
    except:
        tempweb1.append('WEB FALSE')
        print('0  OPEN   FALSE ' + raw_domain)
    else:
        try:
            element = soup.find('meta', attrs={'http-equiv': 'refresh'})
            if element:
                refresh = element['content']
                url = (refresh.partition('=')[2]).strip()
            else:
                url = ''
        except:
                tempweb1.append('WEB FALSE')
                print('1  OPEN   FALSE ' + raw_domain)
        else:
            if url:
                if url.find('https') == -1:
                    if url.find('http') == -1:
                        if url.find('/') == -1:
                            url = (raw_domain + '/' + url)
                        else:
                            url = (raw_domain + url)
                try:
                    r = requests.get(url, timeout=5, allow_redirects=True)
                    data = r.text
                    soup = BeautifulSoup(data, "lxml")
                except:
                    tempweb1.append('WEB FALSE')
                    print('2  OPEN   FALSE ' + raw_domain)
                else:
                    found = 'False'
                    for county in county_list:
                        if re.search(county, data):                              
                            found = 'True'
                            break
                    if found == 'True':
                        tempweb1.append('WEB TRUE')
                        print('3  COUNTY TRUE  ' + raw_domain)
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
                            try:
                                r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                                data = r.text
                            except:
                                tempweb1.append('WEB FALSE')
                                print('4  OPEN   FALSE ' + raw_domain)
                            else:
                                found = 'False'
                                for county in county_list:
                                    if re.search(county, data):                              
                                        found = 'True'
                                        break
                                if found == 'True':
                                    tempweb1.append('WEB TRUE')
                                    print('5  COUNTY TRUE  ' + raw_domain)
                                else:
                                    tempweb1.append('WEB FALSE')
                                    print('6  COUNTY FALSE ' + raw_domain)
                        else:
                            tempweb1.append('WEB FALSE')
                            print('7  COUNTY FALSE ' + raw_domain)
            else:
                found = 'False'
                for county in county_list:
                    if re.search(county, data):                              
                        found = 'True'
                        break
                if found == 'True':
                    tempweb1.append('WEB TRUE')
                    print('8  COUNTY TRUE  ' + raw_domain)
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
                        try:
                            r = requests.get(cleanurl, timeout=5, allow_redirects=True)
                            data = r.text
                        except:
                            tempweb1.append('WEB FALSE')
                            print('9  OPEN   FALSE ' + raw_domain)
                        else:
                            found = 'False'
                            for county in county_list:
                                if re.search(county, data):                              
                                    found = 'True'
                                    break
                            if found == 'True':
                                tempweb1.append('WEB TRUE')
                                print('10 COUNTY TRUE  ' + raw_domain)
                            else:
                                tempweb1.append('WEB FALSE')
                                print('11 COUNTY FALSE ' + raw_domain)
                    else:
                        tempweb1.append('WEB FALSE')
                        print('12 COUNTY FALSE ' + raw_domain)

df['Web County'] = tempweb1

# Check both auto check = Blank & Web Check = True set Status to Unknown US
df.loc[(df['Web County'] == 'WEB TRUE'), 'Auto_Check'] = 'OK'

# Remaining Blank set status to Unknown
df.loc[(df['Web County'] == 'WEB FALSE'), 'Auto_Check'] = 'Unknown'

df.set_index('Domain', inplace=True)
print('Index Set!')

# output results to csv file
df.to_csv('../Data/results1.csv')