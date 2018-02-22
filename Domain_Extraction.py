import pandas as pd
import csv
import time
import tkinter 
tkinter.Tk().withdraw()


from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

while True:
    try:
        filename = askopenfilename()
        domains = pd.read_csv(filename, keep_default_na=False, na_values=[''])

        start_time = time.clock()

        domains.columns = map(str.upper, domains.columns)
        print('Timer Start!')
        domains = domains.filter(regex='EMAIL')
        domains.columns.values[0] = 'EMAIL'
        domains = domains.reindex_axis(['EMAIL'], axis=1)
        domains['EMAIL'] = domains['EMAIL'].str.split('@').str[1].str.lower().str.strip()
        domains.drop_duplicates( inplace=True)
        domains.set_index('EMAIL', inplace=True)

        filename = askdirectory()
        domains.to_csv(filename + '/results1.csv' ,header=False)

        print("Completed! ", time.clock() - start_time, "seconds")

        break
    except Exception as e:
        if e:
            print("error")