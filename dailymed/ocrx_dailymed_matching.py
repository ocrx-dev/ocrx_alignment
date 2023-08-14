from requests.exceptions import ConnectTimeout
import pandas as pd
import json
import ast
import pickle

def parse_df(df):
    columns_to_convert = ['form','roa','active_ingredient']
    def parse(el):
        if type(el) == str:
            return ast.literal_eval(el)
        return []
    for column in columns_to_convert:
        df[column] = df[column].apply(parse)
    return df

all_dailymed_drugs = parse_df(pd.read_csv("all_drugs_ade_indications_updated.csv"))

def construct_dict(df):
#     import ipdb; ipdb.set_trace()
    df = df.dropna()
    return {row['code_dailymed'] : str(int(row['code_ocrx'])) for _, row in df.iterrows()}
    
def load_matches():
    els = ['active_ingredient','form','roa']
    return {el: construct_dict(pd.read_csv(f"results/new_match_{el}.csv")) for el in els}

matches = load_matches()

def extend_current_df(df):
    def add_ocrx_handler(column):
        def handler(els):
            return [{**el, 'ocrx_code' : matches[column].get(el['code'],None)} for el in els]
        return handler
    
    columns = ['form','roa','active_ingredient']
    new_df = df.copy()
    for column in columns:
        new_df[column] = new_df[column].apply(add_ocrx_handler(column))
    return new_df

ocrx_df = extend_current_df(all_dailymed_drugs)
ocrx_df.to_csv("dailymed_and_ocrx.csv")

# 1. Select rows that only have one active ingredient
# 2. Find that active ingredient in OCRx
'''Use QueryFact.queryM'''
import requests
from tqdm import tqdm
class Pipe:
    def __init__(self,fns):
        self.fns = fns
    def __call__(self,*start):
        args = start
        for fn in self.fns:
            args = fn(*args)
        return args

def select_one_active_ingredient(df): 
    return df.loc[df['active_ingredient'].apply(lambda el: len(el) == 1)]
#     return df.loc[map(lambda val: '|||' not in val,df['active_ingredient'].values)]

def parse_dl_response(dlresponse):
    if 'CCD' not in dlresponse:
        return None
#     return [row for row in dlresponse['CCD']]
    return {el: row['RxCui'] for row in dlresponse['CCD'] for el in row['labels']}
def convert_string(dailymed_string):
#     print(dailymed_string)
    return str(dailymed_string).upper()
def convert_comb_dailymed_ocrx(dailymed_string,field):
    df = matches[field]
    el = df.loc[df['label'] == dailymed_string]
    if el.shape[0] > 0:
        return el.iloc[0]['code_ocrx']
    else:
        return None
#     return matches[field].get(convert_string(dailymed_string),None)

def dlquery_and_dump(form_data,i,drug_name):
    parsed = dlquery(form_data)
    clean_drug_name = drug_name.replace("/","|||")
    filename = f"ocrx-matches/{i:09}_{drug_name}.txt"
    total = {"request" : form_data, "drug_name" : drug_name, "response" : parsed}
    with open(filename,"w") as f:
        f.write(json.dumps(total))
        
def dlquery(form_data):
    
    if form_data['substance'] == '':
        return None
#     import ipdb; ipdb.set_trace()
    multipart_form_data = {key: (None,val) for key,val in form_data.items()}
    try:
        response = requests.post('http://localhost:8080/DLquery',files=multipart_form_data,headers={'Authorization': 'Bearer foo'},timeout=5).json()
        parsed = parse_dl_response(response)
        return parsed
    except Exception as e:
        return "timeout"

def match_with_ocrx(df):
    proper_names = {'active_ingredient': 'substance', 'form' : 'form','roa' : 'route'}
#     lookup_names = {'active_ingredient': 'active_ingredient','form':'form','roa':'roa'}
    fields = ['active_ingredient', 'form', 'roa']
    def make_codes_dict(row,field):
#         import ipdb; ipdb.set_trace()
        codes = [el['ocrx_code'] for el in row[field]]
        filtered_codes = [code for code in codes if code is not None]
        return ' '.join(filtered_codes)
    form_datas = [{proper_names[field] : make_codes_dict(row,field) for field in fields} for _, row in df.iterrows()]         
#     import ipdb; ipdb.set_trace()
    drug_names = df['drug'].values
    dl_results = {drug_name: dlquery_and_dump(comb,i,drug_name) for i, (drug_name, comb) in enumerate(tqdm(zip(drug_names,form_datas))) if i > 1679}
    return dl_results

one_active = select_one_active_ingredient(ocrx_df)
result = match_with_ocrx(one_active)
with open("ocrx_dailymed_match.pickle","wb") as f:
    pickle.dump(result,f)
