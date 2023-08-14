import os
from os.path import join as pjoin

from lxml import etree as et
# xml_parser_settings = dict(
    # huge_tree=True, # resolve_entities=False, remove_pis=True, no_network=True
# )

# XMLPARSER = et.XMLParser(xml_parser_settings)
# et.set_default_parser(XMLPARSER)
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

import pickle

print("Starting to read from folder prescription/")
data_dir = "prescription/"
dirs = [pjoin(data_dir,directory) for directory in os.listdir(data_dir) if os.path.isdir(pjoin(data_dir,directory))]
xmls = [[pjoin(dir_name,fn) for fn in os.listdir(dir_name) if fn.endswith(".xml")][0] for dir_name in dirs]


def take_subtree(xml_file):
    with open(xml_file,"r") as f:
        content = f.read()
        author_substr = content
# /home/james/Datasets/dailymed/prescription/20060131_dffb4544-0e47-40cd-9baa-d622075838cc/dffb4544-0e47-40cd-9baa-d622075838cc.xml
def strip_namespace(root):
    ns = '{urn:hl7-org:v3}'
    for elem in root.iter():
        elem.tag = elem.tag.replace(ns, '')
def get_xml_info(xml_file):
    tree = et.parse(xml_file)
    root = tree.getroot()
    strip_namespace(root)
    author_name = root.find("author//name").text
    try:
        drug_name = root.xpath("//manufacturedMedicine/name")[0].text
    except IndexError:
        return None
    active_ingredients = root.xpath("//activeIngredientSubstance/name")
    if len(active_ingredients) != 1:
        return None
    code = root.xpath("//activeIngredientSubstance/code")[0].attrib['code']
    active_ingredient = active_ingredients[0].text
    print(f"finished {drug_name} with {active_ingredient}")
    return dict(author=author_name,drug=drug_name,active_ingredient=active_ingredient,code=code,filename=xml_file)


thread_iterator=ThreadPool(cpu_count()-1).imap_unordered(get_xml_info,xmls)
jsons = [json for json in thread_iterator if thread_iterator is not None]

with open("data.pickle","wb") as f:
    pickle.dump(jsons,f)
