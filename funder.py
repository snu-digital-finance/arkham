from bs4 import BeautifulSoup
import csv

with open("funders.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

with open("output.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["name", "balance", "tag"])

    for entity_block in soup.select("a.Address_link__wE164"):
        # 줄바꿈 제거 + 공백 정리
        name_raw = entity_block.get_text()
        name = " ".join(name_raw.split())

        balance_div = entity_block.find_next("div", class_="tags_resultMiddleColumn__WHM09")
        tag_div = entity_block.find_next("div", class_="Header_tagBrighten__aPR4_")

        balance = balance_div.get_text(strip=True) if balance_div else ""
        tag = tag_div.get_text(strip=True) if tag_div else ""

        balance_clean = balance.replace("$", "").replace(",", "")
        writer.writerow([name, balance_clean, tag])
