import pandas as pd
import csv

all = pd.read_csv("../Data/us_table_all.csv", keep_default_na=False, na_values=['_'])
hard_bounce = pd.read_csv("../Data/all_bounce.csv", keep_default_na=False, na_values=['_'])
domain_history = pd.read_csv("../Data/US.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')


domain_history = domain_history.reindex_axis(['Domain','Domain History'], axis=1)

with open('../Data/bounce_rules.csv', 'r') as f:
    bounce_rules = [row[0] for row in csv.reader(f)]

hard_bounce[' bounce_text'] = hard_bounce[' bounce_text'].str.lower()

hard_bounce.loc[(hard_bounce[' bounce_text'].str.contains('|'.join(bounce_rules), na=False)), 'HARD BOUNCE'] = 'HARD BOUNCE'


df = pd.merge(all, hard_bounce, on='email', how='left').merge(domain_history, on='Domain', how='left')


#df.to_csv('../Data/merge_bounce.csv')


hard_bounce.to_csv('../Data/hard_bounce.csv')