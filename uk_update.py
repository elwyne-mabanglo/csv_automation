import sys
import pandas as pd
import csv

domain_input = sys.argv[1]

domain_list = pd.read_csv(domain_input, keep_default_na=False,  na_values=['_'],encoding ='latin1')

domain_history = pd.read_csv(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\domain_analysis\TODO_domains\uk\web\history\uk_history.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')

df = pd.merge(domain_list, domain_history, on='Domain', how='left')

df.loc[(df['Updates'].notnull()), 'Auto_Check'] = df['Updates']

df = df.reindex_axis(['Domain', 'Status','TLD',' A Count','MX Count',
                            ' Primary MX Server','Domain History','Auto_Check',' Domain Not Found',
                            'Registrant',' Organisation',' Address',' Country','Registry',
                            ' Created',' Updated',' Expires',' WhoIs Error','Last Checked','No. Days','Web Check','Match','Pattern','WebScrap','MX History','Good MX',
                            'Primary MX Server TLD','Comparison',
                            'gmail','googlemail','hotmail','yahoo','ymail','live','msn','aol',
                            'btinternet','virginmedia','icloud','talktalk','tiscali','sky','blueyonder',
                            'ntlworld','talk21','outlook','rocketmail'], axis=1)

df.set_index('Domain', inplace=True)

df.to_csv(r"C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\domain_analysis\TODO_domains\uk\web\\" + domain_input)
