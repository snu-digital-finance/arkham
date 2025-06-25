from bs4 import BeautifulSoup

with open("funders.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

# 테스트 출력
print("ENTITY NAME:")
for span in soup.select("div.Address_link__wE164 span"):
    print("-", span.text.strip())

print("\nBALANCE:")
for bal in soup.select("div.tags_resultMiddleColumn__WHM09"):
    print("-", bal.text.strip())

print("\nTAGS:")
for tag in soup.select("div.Header_tagBrighten__aPR4_ > div"):
    print("-", tag.text.strip())
