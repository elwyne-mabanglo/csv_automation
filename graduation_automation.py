import pandas as pd
import csv
import glob
import os
import numpy as np

domain_history_uk = pd.read_csv("../Data/UK.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
domain_history_uk = domain_history_uk.reindex_axis(['Domain','Domain History'], axis=1)
domain_history_uk.columns = ['domain', 'domain_history_uk']

domain_history_us = pd.read_csv("../Data/US.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
domain_history_us = domain_history_us.reindex_axis(['Domain','Domain History'], axis=1)
domain_history_us.columns = ['domain', 'domain_history_us']




campaigns = pd.read_csv("../Data/graduation_automation/campaigns.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')

path = r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\graduation_automation\mta_bounce'
all_files = glob.glob(os.path.join(path, "*.csv"))  

df_from_each_file = (pd.read_csv(f) for f in all_files)
bounce_all   = pd.concat(df_from_each_file, ignore_index=True)

bounce_all['email'] = bounce_all['email'].str.lower().str.strip().str.replace('\\','')


bounce_all = bounce_all[~bounce_all['email'].str.contains('https')]
bounce_all[' bounce_text'] = bounce_all[' bounce_text'].str.lower()

with open(r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\graduation_automation\bounce_rules.csv', 'r') as f:
    bounce_rules = [row[0] for row in csv.reader(f)]

bounce_all.loc[(bounce_all[' bounce_text'].str.contains('|'.join(bounce_rules), na=False)), 'hard_bounce'] = 'hard bounce'

bounce_all = bounce_all.reindex_axis(['email','hard_bounce',' bounce_text'], axis=1)



bounce_all.to_csv('../Data/graduation_automation/bounce_all.csv')


table_all_path = r'C:\Users\Elwyne\Documents\python_project\CSV_Automation\Data\graduation_automation\mta_table_all'
table_all_files = glob.glob(os.path.join(table_all_path, "*.csv"))  

dfs_for_table_all = (pd.read_csv(f) for f in table_all_files)
table_all   = pd.concat(dfs_for_table_all, ignore_index=True)

table_all.loc[(table_all['snds'] == '1'), 'status'] = 'SNDS'
table_all = table_all[~table_all['email'].str.contains('sa@spamcheck.fmmcontrol.com')]

table_all.to_csv('../Data/graduation_automation/table_all_not_clean.csv')

table_all = table_all.reindex_axis(['submission_time','last_attempt','email','status','campaign_id','snds','domain','delivery_ip','application','sending_domain'], axis=1)

table_all = table_all[~table_all['campaign_id'].str.contains('ESB')]

df = pd.merge(table_all, bounce_all, on='email', how='left').merge(campaigns, on = 'campaign_id', how='left').merge(domain_history_uk, how='left', on = 'domain').merge(domain_history_us, how='left', on = 'domain')


df['domain_history_uk'] = df['domain_history_uk'].str.lower().str.strip()
df['domain_history_us'] = df['domain_history_us'].str.lower().str.strip()


df = df[~((df['country'].isnull()) | (df['country'] == ''))]

df.loc[(df['hard_bounce'] == 'hard bounce'), 'update_to_uk'] = 'qurantined'
df.loc[(df['hard_bounce'] == 'hard bounce'), 'update_to_us'] = 'qurantined'

df.loc[(df['domain_history_uk'].isnull()) | (df['domain_history_uk'] == '') | (df['domain_history_uk'] == '#n/a'), 'domain_history_uk'] = 'blank'
df.loc[(df['domain_history_us'].isnull()) | (df['domain_history_us'] == '') | (df['domain_history_us'] == '#n/a'), 'domain_history_us'] = 'blank'

df.loc[((df['status'] == 'DELIVERED') & (df['master_filters'] == 'Cleansing Other') & (df['domain_history_uk'].str.contains('ok|unknown uk|private non trade'))), 'update_to_uk'] = 'mailable'
df.loc[((df['status'] == 'DELIVERED') & (df['master_filters'] == 'Cleansing Other') & (df['domain_history_us'].str.contains('ok|unknown us|private non trade'))), 'update_to_us'] = 'mailable'



#df.loc[((df['status'] == 'DELIVERED') & (df['master_filters'] == 'Cleansing Other') & ((df['domain_history_uk'].str.contains('ok|unknown uk|private non trade'))|(df['domain_history_us'].str.contains('ok|unknown us|private non trade')))), 'update_to'] = 'mailable'
 
df.set_index('email', inplace=True)

df.to_csv('../Data/graduation_automation/table_all.csv')



#bounce_all.set_index('email', inplace=True)
#table_all.to_csv('../Data/table_all.csv')