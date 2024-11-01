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
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options



def log_message(message, error=False):
    """Zapíše zprávu do příslušného log souboru."""
    file = 'error.log' if error else 'stat.log'
    with open(file, 'a') as log_file:
        log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def get_session():
    print("Nastavuji prohlížeč...")
    log_message("Nastavuji prohlížeč...")

    options = Options()
    options.add_argument("--headless")  # Spustí prohlížeč bez grafického rozhraní
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get("https://www.idnes.cz/zpravy")
    try:
        driver.execute_script("Didomi.setUserAgreeToAll();")
    except:
        print("Chyba při přidávání cookies.")
        log_message("Chyba při přidávání cookies.", error=True)
        return None

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
    log_message("Prohlížeč nastaven a cookies přidány do session.")

    return session

def save_to_file(all_articles):
    """Uloží articles do JSON souboru."""
    try:
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
                    log_message(f"Chyba při zápisu do souboru: {e}", error=True)
                    log_message("Následující data nebudou uložena.", error=True)
                    log_message(str(article), error=True)

            print(f"Počet článků: {len(all_articles)}")
            print(f"Výsledky uloženy do {OUTPUT_FILE}")
            print(f"Velikost souboru: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024} MB")
            log_message(f"Počet článků: {len(all_articles)}")
            log_message(f"Výsledky uloženy do {OUTPUT_FILE}")
            log_message(f"Velikost souboru: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024} MB")

    except Exception as e:
        print(f"Chyba při zápisu do souboru: {e}")
        print("Následující data nebudou uložena.")
        print(all_articles)

        log_message(f"Chyba při zápisu do souboru: {e}", error=True)
        log_message("Následující data nebudou uložena.", error=True)
        log_message(str(all_articles), error=True)



def get_article_details_request(url, session):
    """Stáhne detaily článku pomocí requests a cookies."""
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else None
                content = ' '.join(p.get_text(strip=True) for p in soup.select('div[itemprop="articleBody"] p')) or 'Unknown Content'
                category = soup.select_one("ul.iph-breadcrumb li:last-child").get_text(strip=True) if soup.select_one("ul.iph-breadcrumb") else None
                image_count = int(soup.select_one("div.more-gallery b").get_text(strip=True)) if soup.select_one("div.more-gallery") else 0
                date_published = soup.select_one("span.time-date")["content"] if soup.select_one("span.time-date") else 'Unknown Date' 
                comments_count = int(soup.select_one("li.community-discusion span").get_text(strip=True).split()[0][1:]) if soup.select_one("li.community-discusion span") else 0

            except Exception as e:
                print(f"Chyba při zpracování {url}: {e}")
                return None

            if not content:
                print(f"Chybí obsah článku: {url}")
                return None
            
            if not title:
                print(f"Chybí titulek článku: {url}")
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

def get_articles_from_page(page, session):
    """Získá URL článků z jedné stránky."""
    page_source = session.get(f"https://www.idnes.cz/zpravy/archiv/{page}").text
    soup = BeautifulSoup(page_source, 'html.parser')
    articles = soup.find_all('div', class_='art')
    article_urls = [article.find('a')['href'] for article in articles]
    return article_urls

def process_article(article_url, session):
    """Zpracuje článek a uloží ho do souboru."""
    details = get_article_details_request(article_url, session)
    if details:
        return details
    return None

def scrape_articles_by_page(session, iter=0):
    """Prochází stránky archivu a po každé stránce ukládá články."""
    all_articles = []
    log_every_pages = 10



    with ThreadPoolExecutor(max_workers=20) as executor:
        for page in range(iter*log_every_pages+1, iter*log_every_pages+log_every_pages+1):
            print(f"Načítám stránku {page}")
            article_urls = get_articles_from_page(page, session)  # Získá URL článků z aktuální stránky

            futures = [executor.submit(process_article, url, session) for url in article_urls]
            for future in as_completed(futures):
                details = future.result()
                if details:
                    all_articles.append(details)


    # Zápis všech článků do souboru na konci
    save_to_file(all_articles)



# Hlavní program
if __name__ == '__main__':

    # Nastavení session
    session = get_session()

    if not session:
        print("Nepodařilo se nastavit session.")
        log_message("Nepodařilo se nastavit session.", error=True)
        exit(1)


    # Kontrola, zda soubor existuje a je dostatečně velký
    OUTPUT_FILE = 'articles_final.json'
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        with open(OUTPUT_FILE, 'w') as f:
            f.write('[')
        scrape_articles_by_page()

    # Zpracování stránek, dokud není velikost souboru alespoň 1 GB    
    i = 1
    while os.path.getsize(OUTPUT_FILE) < 1024 * 1024 * 2:
        try:
            print(f"Velikost souboru: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024} MB")
            scrape_articles_by_page(i)
        except Exception as e:
            print(f"Chyba při zpracování stránky {i}: {e}")

        i += 1

    # Uzavření JSON pole
    with open(OUTPUT_FILE, 'a') as f:
        if os.path.getsize(OUTPUT_FILE) > 0:
            f.seek(f.tell() - 2, os.SEEK_SET)
            f.truncate()
        f.write(']')

    print("Dosaženo 1 GB dat.")

    # Po konci programu je potřeba v souboru vymazat všechny řádky obsahující "content": {
    with open(OUTPUT_FILE, 'r') as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if '"content": {' not in line:
                f.write(line)
        f.truncate()