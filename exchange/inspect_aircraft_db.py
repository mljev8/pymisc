"""
Aircraft database downloaded at:
http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz
"""

import json
from tqdm import tqdm 

filepath = './basic-ac-db.json'

db_as_list = [] # list of dicts 
duplicates = []
issue_list = []

db = {} # dict with {hex: designator} items

with open(filepath, 'r') as infile:
    for line in tqdm(infile.readlines()):
        line = line[:-1] # remove \n
        line = line.replace('null','None')
        line = line.replace('false','False').replace('true','True')
        try:
            dict_ = eval(line) # line to dict
            if (dict_['icao'] in db.keys()): # hex already seen 
                duplicates.append(dict_)
            else:
                db_as_list.append(dict_)
                hex_ = dict_['icao']
                designator = dict_['icaotype']
                db[hex_] = designator  
        except:
            issue_list.append(line) # eval() crash, handle later

print(f'{len(db_as_list)} lines from .json file parsed to dicts')
print(f'{len(issue_list)} lines caused issues')
print(f'{len(duplicates)} duplicated hex entries encountered')

#fix issue_list with .replace('\\\\"','').replace('"\\\\','')

# remove useless/uninformative items
uninformative_items = {}
for hex_,designator in db.items():
    if designator is None:
        uninformative_items[hex_] = designator
    if designator == "":
        uninformative_items[hex_] = designator
print(f'{len(uninformative_items)} useless items encountered (no designator)')
for hex_ in uninformative_items.keys():
    del db[hex_]

# dump trimmed database to disk
with open('hex_designator_db.json', 'w') as out:
    out.write(json.dumps(db, indent=4))

# military items
military_list = []
for x in db_as_list:
    if (x['mil'] == True):
        military_list.append(x)
military_designators = [x['icaotype'] for x in military_list]

military_db = {x['icao']:x['icaotype'] for x in military_list if x['icao'] in db}
military_db = {key:value for key,value in sorted(military_db.items())}

print(f"{len(military_db)} military hex id's in database")
print(f"{len(set(military_db.values()))} distinct military designators")

# dump military database to disk
with open('military_hex_designator_db.json', 'w') as out:
    out.write(json.dumps(military_db, indent=4))
