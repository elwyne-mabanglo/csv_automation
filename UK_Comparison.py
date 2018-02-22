import pandas as pd
import csv

import tkinter 
tkinter.Tk().withdraw()

from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

england_postcodes1 = pd.read_csv("../Data/England1.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
england_postcodes2 = pd.read_csv("../Data/England2.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
england_postcodes3 = pd.read_csv("../Data/England3.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
scotland_postcodes = pd.read_csv("../Data/Scotland.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
wales_postcodes = pd.read_csv("../Data/Wales.csv", keep_default_na=False, na_values=['_'])


#county = pd.read_csv("../Data/county.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')
#doamins = pd.read_csv("../Data/domains.csv", keep_default_na=False, na_values=['_'],encoding ='latin1')

doamins = askopenfilename()
doamins = pd.read_csv(doamins,keep_default_na=False, na_values=['_'],encoding ='latin1')

col_list = [1, 2]

england_postcodes1['Postcode'] = england_postcodes1['Postcode'].str.replace('\s+', '').str.lower()
england_postcodes2['Postcode'] = england_postcodes2['Postcode'].str.replace('\s+', '').str.lower()
england_postcodes3['Postcode'] = england_postcodes3['Postcode'].str.replace('\s+', '').str.lower()
scotland_postcodes['Postcode'] = scotland_postcodes['Postcode'].str.replace('\s+', '').str.lower()
wales_postcodes['Postcode'] = wales_postcodes['Postcode'].str.replace('\s+', '').str.lower()
doamins['POSTCODE'] = doamins['POSTCODE'].str.replace('\s+', '').str.lower()



df = pd \
    .merge(doamins,scotland_postcodes,left_on='POSTCODE', right_on='Postcode', how='left') \
    .merge(england_postcodes1,left_on='POSTCODE', right_on='Postcode', how='left') \
    .merge(england_postcodes2,left_on='POSTCODE', right_on='Postcode', how='left') \
    .merge(england_postcodes3,left_on='POSTCODE', right_on='Postcode', how='left') \
    .merge(wales_postcodes,left_on='POSTCODE', right_on='Postcode', how='left')


df.loc[(df['Scotland In Use?'].notnull()), 'Auto'] = 'OK'
df.loc[(df['England 1 In Use?'].notnull()), 'Auto'] = 'OK'
df.loc[(df['England 2 In Use?'].notnull()), 'Auto'] = 'OK'
df.loc[(df['England 3 In Use?'].notnull()), 'Auto'] = 'OK'
df.loc[(df['Wales In Use?'].notnull()), 'Auto'] = 'OK'
#df.loc[(df['County_x'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['Region_x'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['County_y'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['Region_y'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['In Use?_x'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['In Use?_y'].notnull()), 'Auto_Check'] = 'OK'
#df.loc[(df['In Use?'].notnull()), 'Auto_Check'] = 'OK'

df['GENDER'] == ""
df.loc[(df['TITLE'].str.contains('Mrs|Ms|Miss', na=False)), 'GENDER'] = 'F'
df.loc[(df['TITLE'].str.contains('Mr', na=False)), 'GENDER'] = 'M'


folder = askdirectory()
df.to_csv(folder + '/results.csv')

#df.to_csv('../Data/results1.csv')

#pd.merge(frame_1, frame_2, left_on = 'county_ID', right_on = 'countyid')