import pandas as pd
from transfer import fetch_history

tokens = ['usd-coin', 'tether']
funds = pd.read_csv('../fund/output.csv')

targets = funds.url.str.split('/').apply(lambda x: x[-1]).unique().tolist()
print(len(targets))


start = 0
days = 10


for token in tokens:
    for t in targets:
        for i in range(1800 * start, 1800 * (start + days), 1800):
            output = fetch_history(
                offset=i, exchange=t,
                tokens=token, chains='ethereum',  # sortDir='asc',
                root_folder='data_fund')
            if output == 0:
                print(f"No data fetched for {t} with token {token} at offset {i}.")
                break
