import json
import os
import requests


headers_options = {
    'accept': '*/*',
    'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'access-control-request-headers': 'x-payload,x-timestamp',
    'access-control-request-method': 'GET',
    'origin': 'https://intel.arkm.com',
    'priority': 'u=1, i',
    'referer': 'https://intel.arkm.com/',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
}

cookies = {
    '_gcl_au': '1.1.1982854107.1748341240',
    '_ga': 'GA1.1.1017264829.1748341240',
    '_fbp': 'fb.1.1748341240071.46644224240794975',
    'arkham_is_authed': 'true',
    'mp_db580d24fbe794a9a4765bcbfec0e06b_mixpanel': '{"distinct_id":"2871301","$device_id":"82448776-4a11-40e9-b328-4a290444bc8b","$initial_referrer":"https://intel.arkm.com/","$initial_referring_domain":"intel.arkm.com","__mps":{},"__mpso":{},"__mpus":{},"__mpa":{},"__mpu":{},"__mpr":[],"__mpap":[],"$user_id":"2871301"}',
    'arkham_platform_session': 'fbf806d3-be19-4f1a-a0a7-4fc3ab8919b3',
    'mp_f32068aad7a42457f4470f3e023dd36f_mixpanel': '{"distinct_id": "$device:197114229f9179-0ba3428139be18-26011f51-2a941f-197114229f9179","$device_id": "197114229f9179-0ba3428139be18-26011f51-2a941f-197114229f9179","$search_engine": "google","$initial_referrer": "https://www.google.com/","$initial_referring_domain": "www.google.com","__mps": {}, "__mpso": {"$initial_referrer": "https://www.google.com/", "$initial_referring_domain": "www.google.com"},"__mpus": {}, "__mpa": {}, "__mpu": {}, "__mpr": [], "__mpap": []}',
    '_ga_K3BXC51SZE': 'GS2.1.s1748341239$o1$g1$t1748341387$j0$l0$h0',
    '_ga_P74N755GGG': 'GS2.1.s1748341240$o1$g1$t1748341387$j0$l0$h0',
}


def fetch_history(offset, exchange, tokens, chains='ethereum', root_folder='data', sortDir=None):
    """
    Fetch swap and transfer histories from the Arkham API and save as JSON files.

    Parameters:
    - offset: integer offset value for pagination
    - exchange: exchange identifier (e.g., 'upbit')
    - tokens: token filter (e.g., 'usd-coin')
    - chains: chain filter (e.g., 'ethereum')
    - root_folder: folder path to save JSON files (default 'data')
    """
    os.makedirs(root_folder, exist_ok=True)

    domain = 'https://api.arkm.com'
    basic_option = f'sortKey=time&sortDir={sortDir if sortDir else "desc"}&limit=1650&offset={offset}'
    fpath_end = f'response_{offset}_{tokens}_{chains}{"_"+sortDir if sortDir else ""}.json'

    # 1) Fetch swaps history
    history_type = 'swaps'
    url_swaps = f"{domain}/{history_type}?for={exchange}&{basic_option}"

    # Preflight
    options_resp = requests.options(url_swaps, headers=headers_options)
    print('OPTIONS status:', options_resp.status_code)

    headers_swaps = {
        'accept': 'application/json, text/plain, */*',
        'x-payload': '8981b9298f7903f4d0d07180771aabd43c8a41e1c32f87306451a33391b3c0b9',
        'x-timestamp': '1748341403',
    }
    headers_swaps.update(headers_options)

    resp_swaps = requests.get(
        url_swaps, headers=headers_swaps, cookies=cookies)
    print('GET swaps status:', resp_swaps.status_code)
    swaps_data = resp_swaps.json().get(history_type, [])
    if not swaps_data:
        print(f"No swaps data found for {exchange} at offset {offset}.")
    else:
        swaps_file = os.path.join(
            root_folder, f"{exchange}_{history_type}_{fpath_end}")
        with open(swaps_file, 'w', encoding='utf-8') as f:
            json.dump(swaps_data, f, ensure_ascii=False, indent=2)
        print(f"Fetched {len(swaps_data)} swaps, saved to {swaps_file}")

    # 2) Fetch transfers history
    history_type = 'transfers'
    basic_option_tokens = basic_option + f'&tokens={tokens}&chains={chains}'

    headers_transfers = {
        'accept': 'application/json, text/plain, */*',
        'x-payload': '2c3ed752485ce22e9b3beb56e8d144f54c9ef987917618af2e95375e1b336377',
        'x-timestamp': '1748343398',
    }
    headers_transfers.update(headers_options)

    # mark session cookie expired if needed
    cookies['_dd_s'] = 'isExpired=1'

    all_transfers = []
    all_chain_transfers = []
    seen = set()

    for flow in ['in', 'out']:
        for usd_gte in [1]:
            url_transfers = (
                f"{domain}/{history_type}?base={exchange}&flow={flow}&usdGte={usd_gte}&{basic_option_tokens}"
            )
            # Preflight
            requests.options(url_transfers, headers=headers_options)
            resp = requests.get(
                url_transfers, headers=headers_transfers, cookies=cookies)
            data = resp.json().get(history_type, [])
            if not data:
                continue

            print(
                f"{flow} >=${usd_gte}: fetched {len(data)} items, status {resp.status_code}")

            for item in data:
                txid = item.get('txid') or item.get('transactionHash')
                if txid and txid not in seen:
                    seen.add(txid)
                    if 'txid' in item:
                        all_chain_transfers.append(item)
                    else:
                        all_transfers.append(item)

    # Save transfers
    out_file = os.path.join(
        root_folder, f"{exchange}_{history_type}_{fpath_end}")
    chain_file = os.path.join(
        root_folder, f"{exchange}_{history_type}_chain_{fpath_end}{'_'+sortDir if sortDir else ''}.json")
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(all_transfers, f, ensure_ascii=False, indent=2)
    with open(chain_file, 'w', encoding='utf-8') as f:
        json.dump(all_chain_transfers, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(all_transfers)} transfers to {out_file}")
    print(f"Saved {len(all_chain_transfers)} chain transfers to {chain_file}, ")
    return len(all_transfers) + len(all_chain_transfers)


if __name__ == '__main__':
    # 예시 호출
    fetch_history(offset=1650 * 2,
                  exchange='upbit',
                  tokens='usd-coin',
                  chains='ethereum')
