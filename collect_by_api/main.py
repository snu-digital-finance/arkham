import requests
import time
import os
import pandas as pd
from dotenv import load_dotenv

# .env에서 ARKHAM_API_KEY 불러오기
load_dotenv()
API_KEY = os.getenv("ARKHAM_API_KEY")
INPUT_FILE = '2025_03_over1000.csv'  # 여기서 파일 이름 변경됨

def call_api(url, params=None):
    headers = {
        "accept": "application/json",
        "api-key": API_KEY
    }
    try:
        response = requests.get(url, headers=headers, params=params)
    except:
        time.sleep(3)
    time.sleep(1)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ 요청 실패. 상태 코드: {response.status_code}")
        print(f"❌ 오류 메시지: {response.text}")
        return None

def get_metadata(address):
    url = f"https://api.arkhamintelligence.com/intelligence/address/{address}"
    return call_api(url)

def get_transfer(address, offset=0):
    url = f"https://api.arkhamintelligence.com/transfers"
    params = {"base": address, 'usdGte': 1, 'limit': 1000, 'offset': offset}  
    return call_api(url, params)

def extract_metadata(info):
    entity = info.get("arkhamEntity", {})
    label = info.get("arkhamLabel", {})
    return {
        "Address": info.get("address"),
        "Chain": info.get("chain"),
        "Entity Name": entity.get("name"),
        "Entity Type": entity.get("type"),
        "Wallet Label": label.get("name"),
        "Website": entity.get("website", ""),
        "Twitter": entity.get("twitter", ""),
        "LinkedIn": entity.get("linkedin", ""),
        "Crunchbase": entity.get("crunchbase", ""),
        "isUser": info.get("isUserAddress", ""),
        "isContract": info.get("contract", ""),
    }

def collect_metadata(addresses):
    rows = []
    for addr in addresses:
        print(f"🔍 {addr} 정보 가져오는 중...")
        data = get_metadata(addr)
        if not data:
            continue
        rows.append(extract_metadata(data))
    return rows

def collect_transfer(addresses):
    rows = []
    entity_rows = []
    for addr in addresses:
        print(f"🔍 {addr} 거래 정보 가져오는 중...")
        data = get_transfer(addr)
        if not data:
            continue

        for transfer in data.get("transfers", []):
            rows.append({
                "Address": addr,
                "ID": transfer.get("id"),
                "transactionHash": transfer.get("transactionHash"),
                "Chain": transfer.get("chain"),
                "From": transfer.get("fromAddress").get("address"),
                "To": transfer.get("toAddress").get("address"),
                "Amount": transfer.get("amount"),
                "Token": transfer.get("token"),
                "TokenAddress": transfer.get("tokenAddress"),
                "TokenName": transfer.get("tokenName"),
                "TokenSymbol": transfer.get("tokenSymbol"),
                "TokenDecimals": transfer.get("tokenDecimals"),
                "Timestamp": transfer.get("blockTimestamp"),
            })
            entity_rows.append(extract_metadata(transfer.get("fromAddress", {})))
            entity_rows.append(extract_metadata(transfer.get("toAddress", {})))

    return rows, entity_rows

def main():
    # with open(INPUT_FILE, 'r') as file:
    #    addresses = [line.strip() for line in file if line.strip()]
    addresses = pd.read_csv(INPUT_FILE).address
    #print(addresses.columns)
    #addresses = addresses.loc[
            #(addresses['count'] > 20) & \
            #(addresses.name.str.len() > 20) & \
            #(~addresses.name.str.contains('000000000000000000000000'))
    #    ].name

    # entity_rows = collect_metadata(addresses)
    rows, entity_rows = collect_transfer(addresses)
    if rows:
        print("📊 수집된 데이터:", len(rows))
        pd.DataFrame(rows).drop_duplicates().to_csv('arkham_transfers_onchain.csv', index=False)
    if entity_rows:
        print("📊 수집된 엔티티 데이터:", len(entity_rows))
        pd.DataFrame(entity_rows).drop_duplicates().to_csv('arkham_entities_onchain.csv', index=False)

if __name__ == "__main__":
    main()