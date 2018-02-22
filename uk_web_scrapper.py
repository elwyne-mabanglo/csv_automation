import sys # Used to get arg from command line
import pandas as pd # Used for dataframe
import csv # Used to import CSV file
import requests # Used to connect webpage
from bs4 import BeautifulSoup # Used to get content of webpage
from multiprocessing import Pool # Used for multiproccesing
import re # Used to do regex search
import time # Used to record time

# import us counties string list into variable
with open('C:/Users/Elwyne/Documents/python_project/CSV_Automation/Data/uk_county.csv', 'r') as f:
    county_list = [row[0] for row in csv.reader(f)]

def checkPage(domain,url):
    
    regexlist = [
                re.compile(r'(\W(\+44\s?\(?\[?0\]?\)?([\s?\d]{4}[\s?\d]{7}|[\d]{2}[\s?\d]{5}[\s?\d]{5}))\W)',re.IGNORECASE),
                re.compile(r'(\W\(?\+44\s?\)?[\d]{4}\s?[\d]{3}\s?[\d]{3}\W)',re.IGNORECASE),
                re.compile(r'(\W((\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})\W)',re.IGNORECASE),
                re.compile(r'(\W(\+44\s[\d]{2}\s[\d]{4}\s[\d]{4})\W)',re.IGNORECASE),
                re.compile(r'(\W((0[\d]{4})((\s[\d]{4}[\d]{3})|(\s[\d]{7})|(\s[\d]{4}[\s\d]{4})))\W)',re.IGNORECASE),
                re.compile(r'(\W(\+?[\s?]44[\s?]\(?\[?0\]?\)?[\s?\d]{5}[\s?\d]{4}[\s?\d]{4})\W)',re.IGNORECASE),                   
                re.compile(r'(\W((0800|0808|0843|0844|0845|0870|0871|0872|0873)(\-?\s?([\d]{3}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{4}\-?\s?[\d]{2}|[\d]{2}\-?\s?[\d]{4})|\s?0\-?\s?([\d]{2}\-?\s?[\d]{2}\-?\s?[\d]{2}|[\d]{3}\-?\s?[\d]{3})))\W)',re.IGNORECASE),
                #re.compile(r'(\W(\(?\[?0\]?\)?[\d]{3}\s?[\d]{2}\s?[\d]{2}\s?[\d]{3})\W)',re.IGNORECASE),
                re.compile(r'(\W(\+\s[\d]{2}\s\(0\)[\d]{4}\s[\d]{6})\W)',re.IGNORECASE),
                #re.compile(r'(\W([\d]{4}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE),
                re.compile(r'(\W(0[\d]{3}\s[\d]{3}\s[\d]{4})\W)',re.IGNORECASE),
                re.compile(r'(\W(0[\d]{4}\s[\d]{6})\W)',re.IGNORECASE)  
                ]

    regexforiegn = [
                    #re.compile(r'(united\s?states|\susa\s)',re.IGNORECASE),
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
            #print("Domain: " + url)
            s = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M).strip('\n').strip()
            x = regex.pattern
            #print("Match: " + s)
            #print(regex)        
            #print("WEB FOREIGN")
            return "WEB FOREIGN," + s + "," + x
            break

    for regex in regexlist:           
        m = re.search(regex, domain)
        if m:
            #print("Domain: " + url)
            s = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M).strip('\n').strip()
            x = regex.pattern
            #print("Match: " + s)
            #print(regex)
            return "WEB TRUE," + s + "," + x
            break

    for regex in regexTemp:           
        m = re.search(regex, domain)
        if m:
            #print("Domain: " + url)
            s = re.sub(r'\n\s*\n', r'\n\n', m.group(1), flags=re.M).strip('\n').strip()
            x = regex.pattern
            #print("Match: " + s)
            #print(regex)
            return 'WEB TEMP,' + s + "," + x
            break

    for county in county_list:
        if re.search(county, domain):   
            #print("Domain: " + url)
            #print("Match: " + county)
            return 'WEB TRUE,' + county
            break
     
    return 'WEB UNKNOWN,None' 

def f(domain):
    if domain == " ":
        return "SKIP,None"
    else:       
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'} 
        url = 'http://www.' + domain + '/'
        try:
            r = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
            data = r.text
            soup = BeautifulSoup(data, "lxml")
        except:
            return "FAILED,None"
        else:
            results = checkPage(data,url)
            if results.find("UNKNOWN") == -1:
                return results
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
                                cleanurl = (url + cleanurl)
                            else:
                                cleanurl = (url + '/' + cleanurl)
                    try:
                        r = requests.get(cleanurl, headers=headers, allow_redirects=True, timeout=10)
                        data = r.text
                    except:
                        return results
                    else:
                        results = checkPage(data,url)
                        return results
                else:
                    return results

if __name__=='__main__':

    file = r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\domain_analysis\TODO_domains\uk\\' + sys.argv[1]

    df = pd.read_csv(file, keep_default_na=False, na_values=['_'],encoding ='latin1') 
   
    date = []
    test1 = []
    for item in df.itertuples():
        if (item[8] == "Blank"):
            test1.append(item[1])
            date.append(item[20])
        else:
            test1.append(" ")
            date.append(item[19])

    with Pool(100) as p:
        df["test"] = p.map(f, test1)

    df['Match'] = df['test'].str.split(',').str[1]
    df['Web Check'] = df['test'].str.split(',').str[0]
    df['Pattern'] = df['test'].str.split(',').str[2]
    df['Last Checked'] = date

    df.loc[(df['Web Check'] == 'WEB TRUE'), 'Auto_Check'] = 'OK'
    df.loc[(df['Web Check'] == 'WEB TEMP'), 'Auto_Check'] = 'Temp'
    df.loc[(df['Web Check'] == 'WEB FOREIGN'), 'Auto_Check'] = 'Foreign'
    df.loc[(df['Web Check'] == 'FAILED'), 'Auto_Check'] = 'Unknown'
    df.loc[(df['Web Check'] == 'WEB FALSE'), 'Auto_Check'] = 'Unknown'  
    df.loc[(df['Web Check'] == 'WEB UNKNOWN'), 'Auto_Check'] = 'Unknown'  

    # UNKNOWN STATUS - OK - Check domains is OK
    df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN'))
            & (((df[' Country'] == 'uk') 
                | (df[' Country'] == 'united kingdom') 
                | (df[' Country'] == 'gb')
                | (df[' Country'] == 'ie')
                | (df[' Country'] == 'im'))
                | ((df[' Address'].str.contains('united')) & (df[' Address'].str.contains('kingdom')))), 'Auto_Check'] = 'Unknown UK'

    df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN'))
        & (df[' Country'] != '')
        & ((df[' Country'] != 'uk') 
            & (df[' Country'] != 'united kingdom') 
            & (df[' Country'] != 'gb')
            & (df[' Country'] != 'ie')
            & (df[' Country'] != 'im')), 'Auto_Check'] = 'Foreign'

    df.loc[((df['Web Check'] == 'WEB FALSE') | (df['Web Check'] == 'FAILED') | (df['Web Check'] == 'WEB UNKNOWN'))
          & (df['Domain History'] == 'Foreign'), 'Auto_Check'] = 'Foreign'

    df.loc[(df['Match'].str.contains("sale")), 'Auto_Check'] = 'Bad'

    df = df.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                              ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                              'Registrant',' Organisation',' Address',' Country','Registry',
                              ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','No. Days','Web Check','Match','Pattern','WebScrap','MX History','Good MX',
                              'Primary MX Server TLD','Comparison',
                              'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                              'btinternet','virginmedia','icloud','talktalk','tiscali','sky','blueyonder',
                              'ntlworld','talk21','outlook','rocketmail'], axis=1)


    df.set_index('Domain', inplace=True)

    df.to_csv(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\domain_analysis\TODO_domains\uk\web\WS_' + sys.argv[1])

