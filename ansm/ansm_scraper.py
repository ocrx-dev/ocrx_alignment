import urllib.request
import pickle
import re
import csv
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool, Pool
import os
import itertools

urlRCP = "http://base-donnees-publique.medicaments.gouv.fr/affichageDoc.php?specid="
param = "&typedoc=R"

urlBase = "http://base-donnees-publique.medicaments.gouv.fr/extrait.php?specid="
urlRCPEuro = "http://ec.europa.eu/health/documents/community-register/html/h_direct_anx.htm#"
paramEuro = "_fr"

cis_ATC = []

def write_to_file(content,fn="output.txt"):
    with open(fn,"w") as f:
        f.write(content+"\n")

def do_row(args):
    # print(f"doing row {row}")
    try:
        row,num,total = args
        if num % 100 == 0:
            print(f"{100 * num / total:.2f}%. ({num}/{total})")
        cur = urlRCP + row[0] + param

        req = urllib.request.Request(cur)
        resp = urllib.request.urlopen(req,timeout=5)
        respData = resp.read()

        if(re.findall("Le document demandé n'est pas disponible pour ce médicament", str(respData))):
            cur = urlBase + row[0]
            code = re.findall('ConfirmLienEurope\\([0-9]+\\)')
            rcpEuropeen = urlRCPEuro + code + paramEuro
            print(rcpEuropeen)

            content = row[0] + ";" + rcpEuropeen
            write_to_file(content,fn=f"data/{num}.txt")
            #print(cis_ATC)
            # write_to_file(row[0]+';'+rcpEuropeen)
        else:
            #atcs = re.findall('((c|Code)|CODE)\s*(atc|ATC)\s*:\s*([A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9])', str(respData), re.IGNORECASE)
            # print("finding atcs...")
            # import ipdb; ipdb.set_trace()
            atcs = re.findall('([A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9])', str(respData), re.IGNORECASE)
            content = '\n'.join([f"{row[0]};{atc}" for atc in atcs])
            write_to_file(content+"\n===================\n"+str(respData),fn=f"data/{num}.txt")
            #print(cis_ATC)
            # for curATC in atcs:
                # write_to_file(row[0]+';'+curATC)
    except Exception as e:
        print(f"ignoring {num}")
        write_to_file(f"error {e}",f"data/{num}.txt")

def dump_pickle(obj,output_fn="data.pickle"):
    with open(output_fn,"wb") as f:
        pickle.dump(obj,f)

with open('CIS_bdpm.txt', 'rt', encoding='latin1') as csvfile:
    spamreader = csv.reader(csvfile, delimiter='\t')
    rows = [row for row in spamreader]
    total = len(rows)
    parsed_rows = [(row,num,total) for num,row in enumerate(rows)]
cpus=cpu_count()

thread_iterator=Pool(processes=200).imap_unordered(do_row,parsed_rows)
# [do_row(row) for row in parsed_rows]
for result in thread_iterator:
    continue
print("Fini!")
