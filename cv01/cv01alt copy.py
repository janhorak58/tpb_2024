"""

PROGRAM idnes.cz web scraper
- Program stahuje články z webu idnes.cz a ukládá je do JSON souboru.
- Program stahuje články z archivu, který má 36 článků na stránku.
- Program stahuje články do souboru, dokud není velikost souboru alespoň 1 GB.
- Program používá requests a BeautifulSoup pro stahování a zpracování HTML.
- Program používá Selenium pro nastavení cookies.
- Program používá ThreadPoolExecutor pro paralelní zpracování článků.
- Program používá session pro ukládání cookies.
- Program obsahuje ošetření chyb při stahování a zpracování článků.
- Program obsahuje logování chyb do souboru.
- Program obsahuje ošetření chyb při zápisu do souboru.
- Program obsahuje ošetření chyb při zpracování stránky.
- Program obsahuje ošetření chyb při zpracování článku.
- Program obsahuje ošetření chyb při zpracování detailů článku.

- Program umí načíst soubor, který vytvořil a umožní dále s daty pracovat.

"""


import requests
from bs4 import BeautifulSoup
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

print("Nastavuji prohlížeč...")

options = Options()
options.add_argument("--headless")  # Spustí prohlížeč bez grafického rozhraní
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

driver.get("https://www.idnes.cz/zpravy")
try:
    driver.execute_script("Didomi.setUserAgreeToAll();")
except:
    pass

print("Prohlížeč nastaven.")
print("Přidávám cookies do session...")

# Přidání cookies do session
session = requests.Session()
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
for cookie in driver.get_cookies():
    session.cookies.set(cookie['name'], cookie['value'])
driver.quit()

print("Cookies přidány do session.")

OUTPUT_FILE = 'articles_final.json'

def save_to_file(data):
    """Uloží data do JSON souboru."""
    try:
        with open(OUTPUT_FILE, 'a') as f:
            if os.path.getsize(OUTPUT_FILE) == 0:
                f.write('[')
            else:
                f.write(',')
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.write('\n')
    except Exception as e:
        print(f"Chyba při zápisu do souboru: {e}")
        print("Následující data nebudou uložena.")
        print(data)

def get_article_details_request(url):
    """Stáhne detaily článku pomocí requests a cookies."""
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Unknown Title'
                content = ' '.join(p.get_text(strip=True) for p in soup.select('div[itemprop="articleBody"] p')) or 'Unknown Content'
                category = soup.select_one("ul.iph-breadcrumb li:last-child").get_text(strip=True) if soup.select_one("ul.iph-breadcrumb") else 'Unknown Category'
                image_count = int(soup.select_one("div.more-gallery b").get_text(strip=True)) if soup.select_one("div.more-gallery") else 0
                date_published = soup.select_one("span.time-date")["content"] if soup.select_one("span.time-date") else 'Unknown Date' 
                comments_count = int(soup.select_one("li.community-discusion span").get_text(strip=True).split()[0][1:]) if soup.select_one("li.community-discusion span") else 0

            except Exception as e:
                print(f"Chyba při zpracování {url}: {e}")
                return None

            if not content:
                print(f"Chybí obsah článku: {url}")
                return None

            return {
                'title': title,
                'content': content,
                'category': category,
                'image_count': image_count,
                'date_published': date_published,
                'comments': comments_count
            }
    except requests.exceptions.RequestException as e:
        print(f"Chyba při stahování {url}: {e}")
    return None

def get_articles_from_page(page):
    """Získá URL článků z jedné stránky."""
    page_source = session.get(f"https://www.idnes.cz/zpravy/archiv/{page}").text
    soup = BeautifulSoup(page_source, 'html.parser')
    articles = soup.find_all('div', class_='art')
    article_urls = [article.find('a')['href'] for article in articles]
    return article_urls

def process_article(article_url):
    """Zpracuje článek a uloží ho do souboru."""
    details = get_article_details_request(article_url)
    if details:
        return details
    return None

def scrape_articles_by_page(num_pages=0):
    """Prochází stránky archivu a po každé stránce ukládá články."""
    goal_size = 1024 * 1024 * 1024  # 1 GB
    all_articles = []
    loop_pages = 10

    with ThreadPoolExecutor(max_workers=20) as executor:
        for page in range(num_pages*loop_pages+1, num_pages*loop_pages+loop_pages+1):
            print(f"Načítám stránku {page}")
            article_urls = get_articles_from_page(page)  # Získá URL článků z aktuální stránky

            futures = [executor.submit(process_article, url) for url in article_urls]
            for future in as_completed(futures):
                details = future.result()
                if details:
                    all_articles.append(details)
                
                # Kontrola velikosti souboru po zpracování každé stránky
                if page % 10 == 1:
                    print(f"Počet článků: {len(all_articles)}")


    # Zápis všech článků do souboru na konci
    with open(OUTPUT_FILE, 'a') as f:
        for article in all_articles:
            try:
                json.dump(article, f, ensure_ascii=False, indent=4)
                f.write(',')
                f.write('\n')
            except Exception as e:
                print(f"Chyba při zápisu do souboru: {e}")
                print("Následující data nebudou uložena.")
                print(article)

        print(f"Počet článků: {len(all_articles)}")
        print(f"Výsledky uloženy do {OUTPUT_FILE}")
        print(f"Velikost souboru: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024} MB")


# Kontrola, zda soubor existuje a je dostatečně velký
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'w') as f:
        f.write('[')
    scrape_articles_by_page()
i = 141
while os.path.getsize(OUTPUT_FILE) < 1024 * 1024 * 1024:
    try:
        print(f"Velikost souboru: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024} MB")
        scrape_articles_by_page(i)
    except Exception as e:
        print(f"Chyba při zpracování stránky {i}: {e}")

    i += 1

with open(OUTPUT_FILE, 'a') as f:
    if os.path.getsize(OUTPUT_FILE) > 0:
        f.seek(f.tell() - 2, os.SEEK_SET)
        f.truncate()
    f.write(']')

print("Dosaženo 1 GB dat.")

# Po konci programu je potřeba v souboru vymazat všechny řádky obsahující "content": {


# Výsledky uloženy do articles.json