import pandas as pd # Used for dataframe
import csv # Used to import CSV file
import requests # Used to connect webpage
from bs4 import BeautifulSoup # Used to get content of webpage
from multiprocessing import Pool # Used for multiproccesing
import re # Used to do regex search
import time # Used to record time

start_time = time.clock()

domain_history = pd.read_csv("../Data/R.csv", keep_default_na=False, na_values=['_'],encoding ='latin1') 

# import us counties string list into variable
with open('../Data/us_county.csv', 'r') as f:
    county_list = [row[0] for row in csv.reader(f)]


def checkPage(domain):

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
            s = m.group(1).strip()
            return "WEB FOREIGN," + s
            break

    for regex in regexlist:           
        m = re.search(regex, domain)
        if m:
            s = m.group(1).strip()
            return "WEB TRUE," + s
            break

    for regex in regexTemp:           
        m = re.search(regex, domain)
        if m:
            s = m.group(1).strip()
            return 'WEB TEMP,' + s
            break

    for county in county_list:
        if re.search(county, domain):     
            return 'WEB TRUE,' + county
            break
     
    return 'UNKNOWN WEB,None' 


def f(domain):
    if domain == " ":
        print((time.clock() - start_time)/60, "seconds","SKIP")
        return "SKIP,None"
    else:
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'} 
        url = 'http://www.' + domain + '/'
        try:
            r = requests.get(url, headers=headers, timeout=10)
            html = r.text
            #soup = BeautifulSoup(html, 'lxml')
            results = checkPage(html)
            print((time.clock() - start_time)/60, "seconds",results,url)
            return (results)
        except:
            print((time.clock() - start_time)/60, "seconds","UNKNOWN WEB",url)
            return "UNKNOWN WEB,None"


if __name__ == '__main__':

    test = domain_history[['Domain','Status']]

    #print(test)

    test1 = []
    for item in test.itertuples():
        if (item[2] == "Unknown") or (item[2] == "Bad WhoIs"):
            test1.append(item[1])
        else:
            test1.append(" ")

    temp = [] 
    temp2 = []
    with Pool(100) as p:
        domain_history["test"] = (p.map(f, test1))

    
    domain_history['Match'] = domain_history['test'].str.split(',').str[1]
    domain_history['Web Check'] = domain_history['test'].str.split(',').str[0]

    domain_history = domain_history.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                                ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                                'Registrant',' Organisation',' Address',' Country','Registry',
                                ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','No. Days','Web Check','Match','WebScrap','MX History','Good MX',
                                'Primary MX Server TLD','Comparison',
                                'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                                'verizon','earthlink','comcast','netzero','juno','icloud','outlook','rocketmail','coxmail','compuserve'], axis=1)

    domain_history.set_index('Domain', inplace=True)

    #domain_history.loc[(domain_history['test'].str.contains('WEB TRUE')), 'Domain History'] = 'test'
    domain_history.to_csv('../Data/testData2.csv')

