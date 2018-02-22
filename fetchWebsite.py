import pandas as pd
import csv
import ssl
import socket
import re
import lxml

from bs4 import BeautifulSoup

from urllib.request import Request, urlopen
from urllib.error import  URLError


ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


domain_list = pd.read_csv("../Data/R.csv", keep_default_na=False, na_values=['_'])

mx_history = pd.read_csv("../Data/us_mx_history.csv", keep_default_na=False, na_values=['_'])

domain_history = pd.read_csv("../Data/US.csv", keep_default_na=False, na_values=['_'])

df = pd.merge(domain_list, mx_history, on=' Primary MX Server', how='left').merge(domain_history, on='Domain', how='left')

regex = r'(?:()United States()|()[?:0-9]{3}.[?:0-9]{3}.[?:0-9]{4}()|()[?:0-9]{3}-[?:0-9]{3}-[?:0-9]{4}())'
tempWeb = []
tempWebPage = []
for item in df.itertuples():
    #if item[17] == 'Check':
        domain = item[1]
        req = Request('http://' + domain + '/')
        try:
            response = urlopen(req,timeout=1, context=ctx)         
        except:
            contains = 'FALSE'
            print('Not Opened '  + item[1])
            tempWeb.append(contains)
        else:
            try:
                html = response.read().decode('utf-8', errors='ignore').strip()
                soup = BeautifulSoup(html,"lxml")
            except ssl.CertificateError as e:
                print(e)
            else:
                m = re.search(regex, html)
                if m:
                    print('Found Number ' + item[1])
                else:
                    for link in soup.find_all('a',href=re.compile("contact")):
                        if link.get('href'):
                            print(link.get('href'))
                            try:
                                response = urlopen(req,timeout=1, context=ctx)
                                html = response.read().decode('utf-8', errors='ignore').strip()
                            except:
                                print("test1")
                            else:
                                 m = re.search(regex, html)
                                 if m:
                                     print('test4')
                                 else:
                                     print('test2')
                        else:
                            print('empty')
                    #c = soup.find_all('a', href=re.compile('contact'))
                    #if c:
                        #print(c + ' Contact Found' + item[1])
                        #try:
                        #    response = urlopen(req,timeout=1, context=ctx) 
                        #    html = response.read().decode('utf-8', errors='ignore').strip()
                        #    soup = BeautifulSoup(html,"lxml")
                        #except ssl.CertificateError as e:
                        #    print(e)
                    #else:    
                    print('Not Found ' + item[1])

                #if html.find(r'^[0-9]{3}-[0-9]{3}-[0-9]{4}$') == -1:
                #    contains = 'FALSE'
                #    print(domain + ': ' + contains + ' 2 Status: ' + item[17])
                #    tempWeb.append(contains)
                #else:
                #     contains = 'TRUE'
                #     print(domain + ' 3 : ' + contains)
                #     tempWeb.append(contains)
    #else:
        #contains = 'FALSE'
        #tempWeb.append(contains)
        #print(domain + ' 4 : ' + contains)

#df['Web Check'] = tempWeb

#df.to_csv('../Data/results2.csv')