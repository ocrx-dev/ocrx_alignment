'''
main.py

Automates the collection of definitional elements from the current ontology loaded at https://localhost:7200/repositories/ocrx-graphdb
'''
import requests
import os
import pandas as pd

FLAT_QUERY = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?sLabel ?s
WHERE {{
    ?s rdfs:label ?sLabel .
    ?s rdfs:subClassOf ?ss .
    ?ss rdfs:label "{}"@en .
    FILTER (langMatches(lang(?sLabel), "{}"))
}} ''' 

NESTED_QUERY = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?sLabel ?s
WHERE {{
    ?s rdfs:label ?sLabel .
    ?s rdfs:subClassOf ?l .
    ?l rdfs:subClassOf ?hhh .
    ?hhh rdfs:label "{}"@en .
    FILTER (langMatches(lang(?sLabel), "{}"))
}}'''
files = [
    ("form","Form","en"),
    ("form","Form","fr"),
    ("roa","Route of administration","en"),
    ("roa","Route of administration","fr"),
    ("active_ingredient","Substance","en"),
    ("active_ingredient","Substance","fr"),
    ("strength","Strength","en"),
    ("strength","Strength","fr"),
    ("distinction","Distinction","en"),
    ("distinction","Distinction","fr"),
    ("component","Component","en"),
    ("component","Component","fr"),
]

BASE_URL = 'http://localhost:7200/repositories/ocrx-graphdb'
def download_csv(queries,filename):
    content = ''
    for i, query in enumerate(queries):
        params = {'query' : query}
        r = requests.get(BASE_URL,params=params)
        text = r.text
        if i == 0:
            content += text
        else:
            no_header_text = '\n'.join(text.split('\n')[1:])
            content += no_header_text
    with open(filename,"w") as f:
        f.write(content)


if not os.path.exists('match-ocrx'):
    os.mkdir('match-ocrx')
if not os.path.exists("export"):
    os.mkdir("export")
for prefix, name, lang in files:
    filename = f"match-ocrx/ocrx_{prefix}_{lang}.csv"
    query = NESTED_QUERY.format(name,lang)
    flat_query = FLAT_QUERY.format(name,lang)
    download_csv([query,flat_query],filename)

    df = pd.read_csv(filename).dropna().drop_duplicates().sort_values('sLabel')
    if prefix == 'active_ingredient':
        df = df.loc[['OCRx/3' in el for el in df['s']]]
    df.columns = ['ocrx_label','ocrx_code']
    df = df[['ocrx_code','ocrx_label']]
    df.to_csv(filename)
    df.to_csv(f"export/ocrx_{prefix}_{lang}.csv",index=False)


drugs = [
    ("drug","Canadian clinical drug","en"),
    ("drug","Canadian clinical drug","fr"),
    ("drug","Prescriptible Canadian clinical drug","en"),
    ("drug","Prescriptible Canadian clinical drug","fr"),
]
drug_types = ['Canadian clinical drug','Prescriptible Canadian clinical drug']
langs = ['en','fr']
for lang in langs:
    filename = f"match-ocrx/match_drug_{lang}.csv"
    queries = [NESTED_QUERY.format(drug_type,lang) for drug_type in drug_types]
    download_csv(queries,filename)
    df = pd.read_csv(filename).dropna().drop_duplicates().sort_values('sLabel')
    df.columns = ['ocrx_label','ocrx_code']
    df = df[['ocrx_code','ocrx_label']]
    df.to_csv(filename,index=False)
    df.to_csv(f"export/ocrx_drug_{lang}.csv",index=False)

