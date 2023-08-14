import os
from os.path import join as pjoin
import pandas as pd

dir = 'all-data'
for folder in os.listdir(dir):
    fulldir = pjoin(dir,folder)
    for file in os.listdir(fulldir):
        if file.endswith('.csv'):
            fullfile = pjoin(fulldir,file)
            df = pd.read_csv(fullfile)
            for col in df.columns:
                if col.startswith('Unnamed'):
                    del df[col]
                if col.endswith('label'):
                    df[col] = df[col].str.upper()

            df.to_csv(fullfile,index=False)
print('Done upper-casing all files.')
