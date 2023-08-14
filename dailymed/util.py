import pandas as pd
df = pd.read_csv("all_drugs_ade_indications.csv")
oxygen_df = df[df['drug'] == 'Oxygen']
print('\n'.join(df[df['drug'] == 'Oxygen']['filename'].values[:6]))
# foo = df[df['active_ingredient'].str.upper().startswith('ALBUMIN')]