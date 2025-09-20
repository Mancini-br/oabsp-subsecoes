import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

BASE = "https://www.oabsp.org.br"
SUBSECOES_INDEX = "https://www.oabsp.org.br/subsecoes"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OAB-SP-scraper/1.0; +https://example.org)"
}

PHONE_RE = re.compile(r'(\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4})')
WHATSAPP_RE = re.compile(r'(?:WhatsApp|WhatsApp:|WhatsApp[\s\-]*)\s*[:\-]?\s*(\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4})', re.I)
EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.6, status_forcelist=[429,500,502,503,504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def get_soup(url):
    r = session.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def extract_subsecao_links():
    soup = get_soup(SUBSECOES_INDEX)
    links = set()
    for a in soup.select("a[href^='/subsecao/']"):
        href = a.get('href').strip()
        full = urljoin(BASE, href)
        links.add(full)
    if not links:
        for a in soup.find_all('a', href=True):
            if '/subsecao/' in a['href']:
                links.add(urljoin(BASE, a['href']))
    return sorted(list(links))

def extract_contact_from_page(url):
    soup = get_soup(url)
    text = soup.get_text(" ", strip=True)

    name = ""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)
    elif soup.title:
        name = soup.title.get_text(" ", strip=True)

    emails = sorted(set(EMAIL_RE.findall(soup.text)))
    phones = list(dict.fromkeys(PHONE_RE.findall(soup.text)))
    wa_matches = list(dict.fromkeys(WHATSAPP_RE.findall(soup.text)))

    if not wa_matches:
        for p in phones:
            if re.search(r'\d{5}[-\s]?\d{4}', p):
                wa_matches.append(p)
                break

    address = ""
    addr_tag = soup.find("address")
    if addr_tag:
        address = addr_tag.get_text(" ", strip=True)
    else:
        for candidate in soup.find_all(text=re.compile(r'CEP|Endereço|Rua|Av\.|Avenida', re.I)):
            parent = candidate.parent
            txt = parent.get_text(" ", strip=True)
            if len(txt) > 10:
                address = txt
                break

    city = ""
    m = re.search(r'OAB\s+(.+?)(?:\s+-|\s+\d+ª|\s+Subseção|$)', name, re.I)
    if m:
        city = m.group(1).strip()
    else:
        m2 = re.search(r'([A-Za-zÀ-ú \-]+)\(SP\)', text)
        if m2:
            city = m2.group(1).strip()

    return {
        "Subseção": name,
        "Cidade": city,
        "Telefone": "; ".join(phones),
        "Whatsapp": "; ".join(wa_matches),
        "E-mail": "; ".join(emails),
        "Endereço": address,
        "URL": url
    }

def main():
    links = extract_subsecao_links()
    print(f"Links encontrados: {len(links)} — iniciando coleta...")
    results = []
    for idx, link in enumerate(links, start=1):
        try:
            print(f"[{idx}/{len(links)}] {link}")
            info = extract_contact_from_page(link)
            results.append(info)
        except Exception as e:
            print("Erro:", e, "em", link)
            results.append({"Subseção":"","Cidade":"","Telefone":"","Whatsapp":"","E-mail":"","Endereço":"","URL":link, "Erro": str(e)})
        time.sleep(0.6)
    df = pd.DataFrame(results)
    df.to_csv("oabsp_subsecoes_full.csv", index=False, encoding="utf-8-sig")
    df.to_excel("oabsp_subsecoes_full.xlsx", index=False)
    print("Concluído. Arquivos gerados.")

if __name__ == "__main__":
    main()
