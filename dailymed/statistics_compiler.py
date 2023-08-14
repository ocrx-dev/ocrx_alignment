import pandas as pd
from collections import Counter
from collections import defaultdict
from pprint import pprint


class Stat:
    def __init__(self, stat_name, stat_eval):
        self.stat_name = stat_name
        self.stat_eval = stat_eval

    def __call__(self, dataset):
        stat_val = self.stat_eval(dataset)
        if type(stat_val) != str:
            print(self.stat_name)
            pprint(stat_val)
        else:
            print(f"{self.stat_name}: {stat_val}")
        return (self.stat_name, stat_val)

def count_files(df):
    return df.shape[0]

def count_drugs(df):
    return df['drug'].unique().shape[0]

def count_drugs_unique_sub(df):
    mask = df['active_ingredient'].apply(lambda el: '|||' in str(el))
    return df.loc[~mask].shape[0]

def count_drugs_multiple_sub(df):
    mask = df['active_ingredient'].apply(lambda el: '|||' in str(el))
    return df.loc[mask].shape[0]

def count_companies_unique(df):
    return df['author'].unique().shape[0]

def count_forms_unique(df):
    return df['form'].unique().shape[0]

def count_strength_unique(df):
    return None

def count_roa_unique(df):
    return df['roa'].unique().shape[0]

def count_active_ingredient_unique(df):
    return len({el_i for el in df['active_ingredient'].values for el_i in el.split(' ||| ')})

def count_active_ingredient_multiple(df):
    return len({el for el in df['active_ingredient'].values if ' ||| ' in el})

def count_indications(df):
    return df['indications'].unique().shape[0]

def count_adverse_events_list(df):
    return df['adverse_events_list'].unique().shape[0]
def count_adverse_events_text(df):
    return df['adverse_events'].unique().shape[0]

dailymed_stats = [
    Stat("Nombre de fichiers", count_files),
    Stat("Nombre de médicaments", count_drugs),
    Stat("Nombre de médicaments à substances uniques", count_drugs_unique_sub),
    Stat("Nombre de médicaments à substances multiples", count_drugs_multiple_sub),
    Stat("Nombre de compagnies uniques", count_companies_unique),
    Stat("Nombre de formes uniques", count_forms_unique),
    Stat("Nombre de strength uniques", count_strength_unique),
    Stat("Nombre de routes d'administration uniques", count_roa_unique),
    Stat("Nombre d'ingrédient actif unique",count_active_ingredient_unique),
    Stat("Nombre d'ingrédients actifs multiples",count_active_ingredient_multiple),
    Stat("Nombre d'indications thérapeutiques",count_indications),
    Stat("Nombre d'effets secondaires (en liste)",count_adverse_events_list),
    Stat("Nombre d'effets secondaires (en texte libre)",count_adverse_events_text),
]

def run_stats(stats,filename):
    df = pd.read_csv(filename)
    df = df.astype(str)
    for stat in stats:
        name,value = stat(df)

dailymed_df_filename = "all_drugs_ade_indications.csv"
run_stats(dailymed_stats,dailymed_df_filename)


def group_clusters(df_exact,src='matched_term',dest='term'):
    src_dict = defaultdict(lambda : [])
    dest_dict = defaultdict(lambda : [])
    # do one pass over OCRx
    for _, row in df_exact.iterrows():
        src_dict[row[src]].append(row[dest])
    # do one pass over DailyMed
    for _, row in df_exact.iterrows():
        dest_dict[row[dest]].append(row[src])

    merge_lists = [] 
    # do merge pass over the two
    for key,vals in src_dict.items():
        # key is ocrx, vals is matching dailymed
        # look in matching dailymed to find other ocrx
        all_keys = {key}
        all_vals = set(vals)
        for val in vals:
            all_keys = all_keys.union(dest_dict[val])
            for src_key in all_keys:
                all_vals = all_vals.union(src_dict[src_key])
        merge_lists.append((all_keys,all_vals))
    # import ipdb; ipdb.set_trace()
    import ipdb; ipdb.set_trace()
    return merge_lists
    
def count_(foo):
    pass
    
def count_common(df):
    return df[df.notna()].shape[0]
def count_common_cardinalities(df,src='matched_term',dest='term'):
    merged_lists = group_clusters(df[df.match == 'exact'],src,dest)
    def filter_by_conds(cond1,cond2):
        return len(list(filter(lambda el: cond1(len(el[0])) and cond2(len(el[1])),merged_lists)))
    c_11_filter = filter_by_conds(lambda el: el == 1, lambda el: el == 1)
    c_1n_filter = filter_by_conds(lambda el: el == 1, lambda el: el > 1)
    c_n1_filter = filter_by_conds(lambda el: el > 1, lambda el: el == 1)
    c_nn_filter = filter_by_conds(lambda el: el >1, lambda el: el > 1)
    return {"1-1" : c_11_filter, "1-n" : c_1n_filter, "n-1": c_n1_filter, "n-n" : c_nn_filter}

def count_cardinalities_ocrx(df):
    # notna_df = df[df.notna()]
    return {df['label'].iloc[0]: (key, list(df['code_dailymed'].values))for key, df in df.groupby("code_ocrx")}
    # return (Counter(notna_df['code_ocrx']))


def count_cardinalities_dailymed(df):
    # notna_df = df[df.notna()]
    # codes = ['code_dailymed','code_ocrx']
    # code1, code2 = codes[0], codes[1]
    return {df['label'].iloc[0]: (key, list(df['code_ocrx'].values))for key, df in df.groupby("code_dailymed")}
    # return (Counter(notna_df['code_dailymed']))
    # return dict(Counter(dict(Counter(notna_df['code_dailymed'])).values()))

def count_common_cardinalities_s(df):
    return count_common_cardinalities(df,'term','matched_term')

# df = pd.read_csv("results/match_drug.csv")
# group_clusters(df[df.match == 'exact'])
match_infos = [
    ("ingrédients actifs", "match_drug.csv"),
    ("formes", "match_form.csv"),
    ("ROA", "match_roa.csv")
]

new_match_infos = [
    ("ingrédients actifs", "new_match_active_ingredient.csv"),
    ("formes", "new_match_form.csv"),
    ("ROA", "new_match_roa.csv")

]
for match_name, match_fn in new_match_infos:
    match_df = pd.read_csv(f'results/{match_fn}').dropna().astype(str).drop_duplicates(subset=['code_ocrx','code_dailymed'])
    match_stats = [
        Stat(f"Nombre de {match_name} en commun",count_common),
        Stat(f"Nombre de {match_name} OCRx avec un match / OCRx (cardinalité 1:1, 1:N, N:1, N:M, cardinalité moyenne et maximum)",count_cardinalities_ocrx),
        Stat(f"Nombre de {match_name} Dailymed avec un match / DailyMed (cardinalité 1:1, 1:N, N:1, N:M, cardinalité moyenne et maximum)",count_cardinalities_dailymed),
    ]
    for stat_fn in match_stats:
        stat_name, stat_val = stat_fn(match_df)
