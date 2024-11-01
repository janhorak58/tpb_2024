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

OUTPUT_FILE = 'articles.json'

def save_to_file(data):
    """Uloží data do JSON souboru."""
    with open(OUTPUT_FILE, 'a') as f:
        if os.path.getsize(OUTPUT_FILE) == 0:
            f.write('[')
        else:
            f.write(',')
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.write('\n')

def get_article_details_request(url):
    """Stáhne detaily článku pomocí requests a cookies."""
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Unknown Title'
            content = ' '.join(p.get_text(strip=True) for p in soup.select('div[itemprop="articleBody"] p')) or 'Unknown Content'
            category = soup.select_one("ul.iph-breadcrumb li:last-child").get_text(strip=True) if soup.select_one("ul.iph-breadcrumb") else 'Unknown Category'
            image_count = int(soup.select_one("div.more-gallery b").get_text(strip=True)) if soup.select_one("div.more-gallery") else 0
            date_published = soup.select_one("span.time-date")["content"] if soup.select_one("span.time-date") else 'Unknown Date'
            comments_count = int(soup.select_one("li.community-discusion span").get_text(strip=True).split()[0][1:]) if soup.select_one("li.community-discusion span") else 0

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
        save_to_file(details)

def scrape_articles_by_page(num_pages=300):
    """Prochází stránky archivu a po každé stránce ukládá články."""
    goal_size = 1024 * 1024 * 1024  # 1 GB

    with ThreadPoolExecutor(max_workers=20) as executor:
        for page in range(1, num_pages + 1):
            

            print(f"Načítám stránku {page}")
            article_urls = get_articles_from_page(page)  # Získá URL článků z aktuální stránky

            futures = [executor.submit(process_article, url) for url in article_urls]
            try:
                last_size = os.path.getsize(OUTPUT_FILE)
            except FileNotFoundError:
                last_size = 0
            print(f"Velikost souboru: {last_size / 1024 / 1024} MB")
            # Kontrola velikosti souboru po zpracování každé stránky
            for future in as_completed(futures):
                
                if last_size >= goal_size:
                    print("Dosaženo 1 GB dat.")
                    return  # Ukončí scraping, jakmile je dosaženo 1 GB
            

# Spuštění scraperu
scrape_articles_by_page()
