import pandas as pd
import csv
import time
import tkinter 
import datetime

i = datetime.datetime.now()

tkinter.Tk().withdraw()


from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

while True:
    try:

        filename = askopenfilename()
        df = pd.read_csv(filename, keep_default_na=False, na_values=[''])

        start_time = time.clock()

        print('Timer Start!')

        df = df[((df.domain_history == 'OK') | (df.domain_history == 'Unknown UK') | (df.domain_history == 'Private Non Trade'))]

        gen = df[(df.persona == 'gen')]
        enest_f = df[(df.persona == 'enest') & (df.GENDER == 'F')]
        enest_m = df[(df.persona == 'enest') & ((df.GENDER == 'M') | (df.GENDER == ''))]
        ret_f = df[(df.persona == 'ret') & (df.GENDER == 'F')]
        ret_m = df[(df.persona == 'ret') & ((df.GENDER == 'M') | (df.GENDER == ''))]
        sing_f = df[(df.persona == 'sing') & (df.GENDER == 'F')]
        sing_m = df[(df.persona == 'sing') & ((df.GENDER == 'M') | (df.GENDER == ''))]
        fam_f = df[(df.persona == 'fam') & (df.GENDER == 'F')]
        fam_m = df[(df.persona == 'fam') & ((df.GENDER == 'M') | (df.GENDER == ''))]
        dinky_f = df[(df.persona == 'dinky') & ((df.GENDER == 'F'))]
        dinky_m = df[(df.persona == 'dinky') & ((df.GENDER == 'M') | (df.GENDER == ''))]

        gen = gen.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        enest_f = enest_f.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        enest_m = enest_m.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        ret_f = ret_f.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        ret_m = ret_m.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        sing_f = sing_f.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        sing_m = sing_m.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        fam_f = fam_f.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        fam_m = fam_m.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        dinky_f = dinky_f.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)
        dinky_m = dinky_m.reindex_axis(['EMAIL', 'TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3','CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE'], axis=1)

        gen.set_index('EMAIL', inplace=True)
        enest_f.set_index('EMAIL', inplace=True)
        enest_m.set_index('EMAIL', inplace=True)
        ret_f.set_index('EMAIL', inplace=True)
        ret_m.set_index('EMAIL', inplace=True)
        sing_f.set_index('EMAIL', inplace=True)
        sing_m.set_index('EMAIL', inplace=True)
        fam_f.set_index('EMAIL', inplace=True)
        fam_m.set_index('EMAIL', inplace=True)
        dinky_f.set_index('EMAIL', inplace=True)
        dinky_m.set_index('EMAIL', inplace=True)
    
        filename = askdirectory()

        list = 'LoanOffers4Me.com'
        listID = '09112017'
        date = listID + '_%s%s%s' %( i.year, i.month, i.day)

        gen.to_csv(filename + '/' + list + '_gen_' + date + '.csv')
        enest_f.to_csv(filename + '/' + list + '_enest_f_' + date + '.csv')
        enest_m.to_csv(filename + '/' + list + '_enest_m_' + date + '.csv')
        ret_f.to_csv(filename + '/' + list + '_ret_f_' + date + '.csv')
        ret_m.to_csv(filename + '/' + list + '_ret_m_' + date + '.csv')
        sing_f.to_csv(filename + '/' + list + '_sing_f_' + date + '.csv')
        sing_m.to_csv(filename + '/' + list + '_sing_m_' + date + '.csv')
        fam_f.to_csv(filename + '/' + list + '_fam_f_' + date + '.csv')
        fam_m.to_csv(filename + '/' + list + '_fam_m_' + date + '.csv')
        dinky_f.to_csv(filename + '/' + list + '_dinky_f_' + date + '.csv')
        dinky_m.to_csv(filename + '/' + list + '_dinky_m_' + date + '.csv')

        print("Completed! ", time.clock() - start_time, "seconds")

        break
    except Exception as e:
        if e:
            print(e)