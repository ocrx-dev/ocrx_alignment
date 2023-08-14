import pandas as pd
from pytrie import Trie
import pickle
import re
import os

def f(mylist,n=2):
    return ([el if len(el) == 2 else (el[0],None) for el in mylist])

def clean_zeros(strength):
    if strength is None:
        return None
    zeros_patt = re.compile('\.0+')
    num_patt = re.compile('[0-9]')
    new_str = ' '.join([tok.replace(',','.') if num_patt.sub('',tok) == ',' else tok for tok in strength.split(' ')])
    return zeros_patt.sub('',new_str)

def make_checker_fn(def_set):
    max_len = max([len(el.split(' ')) for el in def_set])
    def check(rest):
        splitted = rest.split(' ')
        for i in range(min(len(splitted),max_len)):
            curr_term = ' '.join(splitted[:i+1])
            if curr_term in def_set:
                return True
        return False
    return check

def load_any_def_elements(data_name='ocrx'):
    all_dicts = dict()
    path = f'ocrx-owls/all-data/{data_name}'
    file_structure = path + '/' + '{}_{}_{}.csv'
    def_elems = ['active_ingredient','strength','form','roa']
    for lang in ['en','fr']:
        for def_elem in def_elems:
            key  = '_'.join([def_elem,lang])
            full_file = file_structure.format(data_name,def_elem,lang)
            if not os.path.exists(full_file):
                continue
            read_df = pd.read_csv(full_file).dropna()
            df_dict = {row[f'{data_name}_label'].upper() : row[f'{data_name}_code'] for _, row in read_df.iterrows()}
            all_dicts[key] = df_dict
    return all_dicts
def gather_components_ansm(string):
    compos_str = string.split(' | ')
    compos = f([tuple(el.split(' AVEC DOSAGE DE ')) for el in compos_str])
    compos = [compo for compo in compos if len(compo) == 2]
    return compos
def gather_components(string,sep=' | '):
    numb_patt = re.compile('[0-9\.\%]+')
    has_multiplier = False
    multiplier = None
    splitted_string = string.split(' ')
    if len(splitted_string) > 1:
        if numb_patt.sub('',splitted_string[0]) == '' and splitted_string[1] == 'ML':
            has_multiplier = True
    if has_multiplier:
        multiplier = ' '.join(splitted_string[:2])
        string = ' '.join(string.split(' ')[2:])
    components = string.split(sep)
    compo_infos = []
    for component in components:
        found_strength = False
        splitted_component = component.split(' ')
        for i, tok in enumerate(splitted_component):
            if numb_patt.sub('',tok) == '' and i > 0:
                sub = ' '.join(splitted_component[:i])
                dose = ' '.join(splitted_component[i:])
                if has_multiplier:
                    dose += ' * ' + multiplier
                compo_infos.append((sub,dose))
                found_strength = True
                break
        if not found_strength:
            compo_infos.append((component,None)) 
    return compo_infos

def find_form_roa(drug):
    before = ''
    form, roa = None, None
    found_form = False
    toks = drug.split(' ')
    for i, tok in enumerate(drug.split(' ')):
        for j in range(i,len(toks)+1):
            rest = ' '.join(toks[i:j])
            if check_form(rest) or check_roa(rest):
                found_form = True
                before = ' '.join(drug.split(' ')[:i])
                after = ' '.join(drug.split(' ')[i:])
                # if after == 'SOLUTION FOR INJECTION':
                    # import ipdb; ipdb.set_trace()
                form, roa, guessed_form = split_form_roa(after)
                return form, roa, before, found_form
    return None, None, None, False
                

def read_drugs():
    dirs = ['ocrx','snomed','rxnorm','dailymed','ansm']
    all_labels = dict()
    mega_forms = set()
    mega_roas = set()
    for dirname in dirs:
        fulldir = f'ocrx-owls/all-data/{dirname}'
        for path in os.listdir(fulldir):
            if 'drug' in path and '.csv' in path:
                fullfile = fulldir+'/'+path
                df = pd.read_csv(fullfile)
                lang = path.split('.')[0].split('_')[2]
                form_file = fulldir + '/' + f'{dirname}_form_{lang}.csv'
                roa_file = fulldir + '/' + f'{dirname}_roa_{lang}.csv'
                df_form = set(pd.read_csv(form_file).dropna()[dirname+'_label'].str.upper().values)
                df_roa = set(pd.read_csv(roa_file).dropna()[dirname+'_label'].str.upper().values)

                label_col = dirname+'_label'
                code_col = dirname+'_code'

                new_df = df.dropna()[[code_col,label_col]].copy()
                new_df[label_col] = new_df[label_col].str.upper()
                new_df = new_df.drop_duplicates()
                all_labels[(dirname,lang)] = (new_df[label_col].values, new_df[code_col].values)
                
                # labels = list(sorted(set(df.dropna()[dirname+'_label'].str.upper().values)))
                # all_labels[(dirname,lang)] = labels

                mega_forms.update(df_form)
                mega_roas.update(df_roa)


    return all_labels, mega_forms, mega_roas

            
            

drugs, form_set, roa_set = read_drugs()
check_form = make_checker_fn(form_set)
check_roa = make_checker_fn(roa_set)
# Ocrx fr
form_set.add('VAPORISATION')
form_set.add('INHALATION')
form_set.add('LINGETTE')
form_set.add('SYSTÈME')
form_set.add('ÉCOUVILLON')
# Snomed
form_set.update({'PESSARY','LYOPHILISATE'})
roa_set.add('INJECTION')
roa_set.update({'NASAL','EAR'})

# Rxnorm
rxnorm_forms = [
    'METERED DOSE INHALER',
    'JET INJECTOR',
    'CARTRIDGE',
    'PREFILLED SYRINGE',
    'PEN INJECTOR',
    'AUTO-INJECTOR'
]
form_set.update(set(rxnorm_forms))

roa_to_form_map = {
    'INJECTION' : 'SOLUTION'
}
def split_form_roa(text,form_set=form_set,roa_set=roa_set):
    words = text.split(" ")
    form, roa = None, None
    word_len = len(words)
    term = None
    found = 0
    for window_size in range(word_len,0,-1):
        for i in range(word_len-window_size+1):
            word = ' '.join(words[i:i+window_size])
            if word in roa_set:
                term = word
                found = 2
                break
            
        if found != 0:
            break
    if found == 0:
        for window_size in range(word_len,0,-1):
            for i in range(word_len-window_size+1):
                word = ' '.join(words[i:i+window_size])
                if word in form_set:
                    term = word
                    found = 1
                    break

            if found != 0:
                break
                
    if found == 0:
        return None, None, None
    other_term = ' '.join([word for word in words if word != term])
    form, roa = (term, other_term) if found == 1 else (other_term, term)
    splitted_form = form.split(' ')
    if splitted_form[-1] in form_set and splitted_form[0] not in form_set and len(splitted_form) > 1:
        rest = ' '.join(splitted_form[:-1])
        form = splitted_form[-1] + ' ' + f'({rest})'
    if form.endswith(" FOR"):
        form = ' '.join(form.split(' ')[:-1])
    if roa.startswith("FOR "):
        roa = ' '.join(roa.split(' ')[1:])
    
    if form not in form_set and len(form.split(' ')) == 1:
        if roa in roa_to_form_map:
            return roa_to_form_map[roa], roa, None
        else:
            return None, roa, form
    # print(f"Term:{text}\tForm:{form}\tRoa:{roa}")
    return form, roa, None

def create_form_roa(df):
    splitted = [split_form_roa(term) for term in df['rxnorm_label'].values]
    forms = [el[0] for el in splitted]
    roas = [el[1] for el in splitted]
    guessed_forms = [el[2] for el in splitted]
    df['form'] = forms
    df['roa'] = roas
    df['guessed_form'] = guessed_forms
    return df

def parse_ocrx_drug_fr(drug,form_set=form_set):
    # ocrx drug is separated by |.
    before = ''
    form,roa = None, None
    if len(drug.split(" SOUS FORME DE ")) < 2:
        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
    else:
        before = drug.split(' SOUS FORME ')[0]
        lol = drug.split(" SOUS FORME DE ")[1].split(" PAR VOIE ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components(before)
    return Drug(compo_infos,form,roa,drug)


def parse_ocrx_drug_en(drug,form_set=form_set):
    # ocrx drug is separated by |.
    before = ''
    form,roa = None, None
    if len(drug.split(" AS ")) < 2:

        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
        # for i, tok in enumerate(drug.split(' ')):
        #     if check_form(' '.join(toks[i:])):
        #         found_form = True
        #         before = ' '.join(drug.split(' ')[:i])
        #         after = ' '.join(drug.split(' ')[i:])
        #         form, roa, guessed_form = split_form_roa(after)
        # if not found_form:
        #     # import ipdb; ipdb.set_trace()
        #     print(f"Cannot parse: {drug}")
        #     return None
    else:
        before = drug.split(' AS ')[0]
        lol = drug.split(" AS ")[1].split(" IN ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components(before)
    return Drug(compo_infos,form,roa,drug)

def parse_snomed_drug_en(drug,form_set=form_set):
    # ocrx drug is separated by |.
    before = ''
    form,roa = None, None
    # if drug == 'BENRALIZUMAB 30 MG/ML SOLUTION FOR INJECTION':
        # import ipdb; ipdb.set_trace()
    if not (' FOR ' in drug and ' IN ' in drug):
        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
    else:
        before = drug.split(' FOR ')[0]
        lol = drug.split(" FOR ")[1].split(" IN ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components(before)
    return Drug(compo_infos,form,roa,drug)


def parse_rxnorm_drug_en(drug,form_set=form_set):
    # ocrx drug is separated by |.
    before = ''
    form,roa = None, None
    # if drug == 'BENRALIZUMAB 30 MG/ML SOLUTION FOR INJECTION':
        # import ipdb; ipdb.set_trace()
    if not (' FOR ' in drug and ' IN ' in drug):
        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
    else:
        before = drug.split(' FOR ')[0]
        lol = drug.split(" FOR ")[1].split(" IN ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components(before,' / ')
    return Drug(compo_infos,form,roa,drug)
def check_where_dose(drug):
    numb_patt = re.compile('[0-9\.\%]+')
    for i, tok in reversed(list(enumerate(drug.split(' ')))):
        if numb_patt.sub('',tok) == '':
            return i
    return -1
        
def parse_dailymed_drug_en(drug,form_set=form_set):
    before = ''
    form,roa = None, None
    if len(drug.split(" WITH FORM OF ")) < 2:
        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
    else:
        before = drug.split(' WITH FORM OF ')[0]
        lol = drug.split(" WITH FORM OF ")[1].split(" WITH ROA OF ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components(before)
    return Drug(compo_infos,form,roa,drug)

def parse_ansm_drug_fr(drug,form_set=form_set):
    # ocrx drug is separated by |.
    before = ''
    form,roa = None, None
    if len(drug.split(" SOUS FORME DE ")) < 2:
        result = find_form_roa(drug)
        if result[-1]:
            form, roa, before = result[0], result[1], result[2]
        else:
            # import ipdb; ipdb.set_trace()
            print(f"Cannot parse: {drug}")
            return None
    else:
        before = drug.split(' SOUS FORME DE ')[0]
        lol = drug.split(" SOUS FORME DE ")[1].split(" PAR VOIE ")
        if len(lol) >= 2:
            form = lol[0]
            roa = lol[1]
        else:
            form = lol[0]

    compo_infos = gather_components_ansm(before)
    return Drug(compo_infos,form,roa,drug)

class Component:
    def __init__(self,name,strength):
        self.name = name
        self.strength = clean_zeros(strength)
        # self.num, self.denom, self.unit_num, self.unit_denom = Component.parse_strength(strength)
    def set_code(self,code):
        self.code = code
    @staticmethod
    def parse_strength(strength):
        numerator, denominator, unit_num, unit_denom = [None ] * 4
        numbers_patt = re.compile('[0-9\.\,\s]*[\.0-9]')
        nums = numbers_patt.findall(strength)
        if len(nums) == 1:
            numerator = nums[0]
        elif len(nums) == 2:
            numerator, denominator = nums
        remaining_units = numbers_patt.sub('',strength)
        seps = [' PAR ', ' FOR ', '/', ' / ']
        for sep in seps:
            if sep in remaining_units:
                splitted = remaining_units.split(sep)
                if len(splitted) == 2:
                    unit_num, unit_denom = splitted
        if unit_num is None and unit_denom is None:
            unit_num = remaining_units 
        if denominator is None:
            denominator = 1
        return numerator, denominator, unit_num, unit_denom


    def clean_compo(self):
        if self.strength is None:
            return (self.name, None, None)
        numbers_patt = re.compile('\s[0-9]+[\s\.\,0-9]*\s')
        tok = [tok for tok in self.strength.split(' ')]
        space_patt = re.compile('[\,\s]')
        # leave active ingredient as is
        # dose can be 200 or 200.00 MG for example
        all_nums = [float(space_patt.sub('',el.strip())) for el in numbers_patt.findall(self.strength)]
        all_not_nums = ' '.join(numbers_patt.sub('',self.strength).strip().split(' '))
        return (self.name,tuple(all_nums),all_not_nums)
    def __str__(self):
        return f"Strength: {self.strength}. Active ingredient: {self.name}"
    def __repr__(self):
        return str(self)

    def __eq__(self, __value: object) -> bool:
        return str(self) == str(__value)
    
    def align_to_database(self,database='ocrx',lang='en'):
        active_ingredient, strength = self.name, self.strength
        matched_active_ingredient = find_match(active_ingredient,database,'active_ingredient',lang)
        matched_strength = find_match(strength,database,'strength',lang)
        return {'active_ingredient': matched_active_ingredient, 'strength': matched_strength}

    def __hash__(self) -> int:
        return str(self).__hash__()

    
reader_fns = {
    'ocrx_fr' : parse_ocrx_drug_fr,
    'ocrx_en': parse_ocrx_drug_en,
    'snomed_en' : parse_snomed_drug_en,
    'dailymed_en' : parse_dailymed_drug_en,
    'rxnorm_en' : parse_rxnorm_drug_en,
    'ansm_fr' : parse_ansm_drug_fr

}


def drug_is_matched(align_dict):
    non_zero_compos = len(align_dict['components']) > 0
    match_compos = all([el[0] is not None and el[1] is not None for el in align_dict['components']])
    form_match = align_dict['form'] is not None
    return non_zero_compos and match_compos and form_match

def get_prefix(trie,term,limit=0):
    if term is None:
        return None
    words = term.split(' ')
    for i in reversed(range(1,len(words)+1)):
        short_term = ' '.join(words[:i])
        if short_term in trie and i > limit:
            return (term, short_term, trie[short_term])
    return None
databases = ['ocrx','snomed','dailymed','rxnorm','ansm']

mega_mega_dict = {database_name : load_any_def_elements(database_name) for database_name in databases}
def find_match(term,database_name,def_type,lang,mega_dict=mega_mega_dict):
    key = f'{def_type}_{lang}'
    ref_dict = mega_dict.get(database_name,dict()).get(key,dict())
    limit = 1 if key.startswith('strength') else 0
    return get_prefix(ref_dict,term,limit)
    
class Drug:
    def __init__(self,compos,form,route,drug):
        self.components = compos
        self.roa = route
        self.form = form
        self.drug = drug
        self.code = None
        self.align_dict = dict()
    @staticmethod
    def parse_labels(drug_labels,name):
        if name in reader_fns:
            fn = reader_fns[name]
            return [fn(drug_label) for drug_label in drug_labels]
        else:
            return None

    def set_code(self,code):
        self.code = code

    def align_to_database(self,database='ocrx',lang='en'):
        compos_matched = []
        form_matched = None
        roa_matched = None
        for compo in self.components:
            if len(compo) == 1:
                continue
            active_ingredient, strength = compo
            matched_active_ingredient = find_match(active_ingredient,database,'active_ingredient',lang)
            matched_strength = find_match(strength,database,'strength',lang)
            compos_matched.append((matched_active_ingredient,matched_strength))

        form_matched = find_match(self.form,database,'form',lang)
        roa_matched = find_match(self.roa,database,'roa',lang)
        match_dict = {
            'components' : compos_matched,
            'form' : form_matched,
            'roa' : roa_matched
        }
        self.align_dict[f"{database}_{lang}"] = match_dict
        return match_dict

    def __str__(self):
        return f"Original: {self.drug}\nComponents: {self.components}. Form: {self.form}. Route: {self.roa}.\n----------------\n"
    def __repr__(self):
        return str(self)
    
    def __eq__(self, __value: object) -> bool:
        return str(self) == str(__value) 


# key = 'ocrx_en'
# tup = tuple(key.split('_'))
# parsed = Drug.parse_labels(drugs[tup][0],key)
# all_compos = list({Component(compo[0],compo[1]) for drug in parsed for compo in drug.components})
# test = Drug.parse_labels(['ACETAMINOPHEN 650 MG/SACHET | ASCORBIC ACID 250 MG/SACHET | CAFFEINE 30 MG/SACHET | CHLORPHENIRAMINE MALEATE 4 MG/SACHET | DEXTROMETHORPHAN HYDROBROMIDE 20 MG/SACHET AS GRANULES FOR SOLUTION IN ORAL'],'ocrx_en')
if __name__ == '__main__':
    if not os.path.exists('ner-results'):
        os.mkdir('ner-results')

    mega_dict = dict()
    mega_compo_dict = dict()
    for key in reader_fns:
        name, lang = key.split('_')
        parsed = Drug.parse_labels(drugs[(name,lang)][0],key)

        codes = drugs[(name,lang)][1]
        for code,drug in zip(codes,parsed):
            if drug is not None:
                drug.set_code(code)
        clean_parsed = [el for el in parsed if el is not None]
        all_compos = sorted(list({Component(compo[0],compo[1]) for drug in clean_parsed for compo in drug.components}),key=lambda el: el.name)
        for i,compo in enumerate(all_compos):
            compo.set_code(i)
        mega_dict[key] = clean_parsed
        mega_compo_dict[key] = all_compos
        with open(f"ner-results/NER-parsed-{key}.txt","w") as file_writer:
            content = '\n'.join([str(el) for el in clean_parsed])
            file_writer.write(content)

    for key,drug_list in mega_dict.items():
        if key.startswith('ocrx'):
            continue
        lang = key.split('_')[1]
        for drug in drug_list:
            drug.align_to_database(lang=lang)
        print(f'Finished {key}')

    with open('ner-results/all_drugs.pickle',"wb") as file_writer:
        pickle.dump(mega_dict,file_writer)
    with open('ner-results/all_compos.pickle',"wb") as file_writer:
        pickle.dump(mega_compo_dict,file_writer)
    no_form_dict = {key: [el for el in els if el.form is None] for key,els in mega_dict.items()}

