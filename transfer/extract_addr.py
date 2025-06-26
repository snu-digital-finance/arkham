import pandas as pd
import json
import os


root_folder = 'tresher/data_fund'


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        if k in ['predictedEntity']:
            continue

        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


result = {
    'from': [],
    'to': []
}
seen = set()
os.makedirs(root_folder, exist_ok=True)
for file in os.listdir(root_folder):
    if file.endswith('.json'):
        with open(f'{root_folder}/{file}', 'r', encoding='utf-8') as f:
            data = json.load(f)
    if not isinstance(data, list):
        continue
    if not 'transfers_response' in file:
        continue

    for d in data:
        if d['toAddress']['address'] not in seen:
            result['to'].append(flatten_dict(d['toAddress']))
            seen.add(d['toAddress']['address'])
        elif d['fromAddress']['address'] not in seen:
            result['from'].append(flatten_dict(d['fromAddress']))
            seen.add(d['fromAddress']['address'])

        for key in ['arkhamLabel_address', 'arkhamEntity_addresses']:
            if result['to'] and key in result['to'][-1]:
                del result['to'][-1][key]
            if result['from'] and key in result['from'][-1]:
                del result['from'][-1][key]


print(result['from'][:5])
print(result['to'][:5])
print(
    f"From addresses: {len(result['from'])}, To addresses: {len(result['to'])}")

df_from = pd.DataFrame(result['from'])
df_to = pd.DataFrame(result['to'])
df = pd.concat([df_from, df_to], ignore_index=True).drop_duplicates().sort_values(
    by='address')

df.loc[df.arkhamLabel_name.str.contains(
    'on OpenSea', na=False), 'arkhamLabel_name'] = 'OpenSea_User'
df.loc[df.arkhamLabel_name == 'OpenSea User',
       'arkhamLabel_name'] = 'OpenSea_User'
df.loc[df.arkhamLabel_name.str.contains(
    '.eth', na=False), 'arkhamLabel_name'] = 'ethName_User'

df.to_csv(
    f'addresses_{root_folder.split("/")[-1]}.csv', index=False, encoding='utf-8')
print(df.shape, df.address.nunique(),
      df[df.arkhamLabel_name.isna()].address.nunique())
print(df.arkhamLabel_name.unique())
