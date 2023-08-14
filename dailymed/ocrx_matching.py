import pandas as pd
import pickle
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from joblib import Parallel, delayed
import requests
from tqdm import tqdm


def construct_dict(df):
#     import ipdb; ipdb.set_trace()
    return {row['term']: str(int(row['ocrx_id'])) for _, row in df[df['match'] == 'exact'].iterrows()}
    
def load_matches():
    els = ['drug','form','roa']
    return {el: construct_dict(pd.read_csv(f"results/match_{el}.csv")) for el in els}

def multithread_with_progress(fn,args_list,pool=ThreadPool(15)):
    total_len = len(args_list)
    def wrapper_fn(tup):
        index, *args = tup
        pct = (index / total_len) * 100
        if index % 1000 == 0:
            print(f"[{index}/{total_len}] ({pct:.2f}%)")
        return fn(*args)
    results = Parallel(n_jobs=-1)(delayed(wrapper_fn)(val) for val in enumerate(args_list))
    return results
# 1. Select rows that only have one active ingredient
# 2. Find that active ingredient in OCRx
def select_one_active_ingredient(df): 
    return df.loc[map(lambda val: '|||' not in val,df['active_ingredient'].values)]

def parse_dl_response(dlresponse):
#     import ipdb; ipdb.set_trace()
#     return [row for row in dlresponse['CCD']]
    return {el: row['RxCui'] for row in dlresponse['CCD'] for el in row['labels']}

def convert_string(dailymed_string):
#     print(dailymed_string)
    return str(dailymed_string).upper()

def convert_comb_dailymed_ocrx(dailymed_string,field):
    return matches[field].get(convert_string(dailymed_string),None)

def dlquery(comb):
    clean_comb = {comb_name: (comb_value, comb_value_name) for comb_name, (comb_value, comb_value_name) in comb.items() if comb_value is not None}
    multipart_form_data = {comb_name : (None, comb_value) for comb_name, (comb_value,_) in clean_comb.items()}
    if 'substance' not in multipart_form_data:
        return None
    response = requests.post('http://localhost:8080/DLquery',files=multipart_form_data,headers={'Authorization': 'Bearer foo'}).json()
#     return response
    return parse_dl_response(response), clean_comb

def match_with_ocrx(df):
    proper_names = {'active_ingredient': 'substance', 'form' : 'form','roa' : 'route'}
    lookup_names = {'active_ingredient': 'drug','form':'form','roa':'roa'}
    fields = ['active_ingredient', 'form', 'roa']
    match_dict = {}
    new_combs = [{proper_names[field] : (convert_comb_dailymed_ocrx(row[field],lookup_names[field]),row[field]) for field in fields} for _, row in df.iterrows()]
    drug_names = df['drug'].values
    all_comb_res = multithread_with_progress(dlquery,new_combs)
    dl_results = {drug_name: comb_res for drug_name, comb_res in zip(drug_names,all_comb_res)}
    return dl_results

all_dailymed_drugs = pd.read_csv("all_drugs_ade_indications.csv")
matches = load_matches()
one_active = select_one_active_ingredient(all_dailymed_drugs)
result = match_with_ocrx(one_active)
# result = "hi"
with open("ocrx_dailymed_match.pickle","wb") as f:
    pickle.dump(result,f)
