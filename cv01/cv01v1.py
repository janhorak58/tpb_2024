import requests
from bs4 import BeautifulSoup
import json
import pymongo
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless")




class Article:
    def __init__(self, title, url):
        self.title = title
        self.content = ""
        self.category = ""
        self.photos_count = 0
        self.published_date = None
        self.comments_count = 0
        self.url = url
        self.get_article_details_from_web()
    def get_json(self):
        return json.dumps(self.__dict__)
    

    def get_article_details_from_web(self):
        DRIVER = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # Otevření webové stránky iDnes.cz
        print(f"Otevírám stránku {self.url}")
        DRIVER.get(self.url)
        click_accept_cookies(DRIVER)
        DRIVER.get(self.url)

        # Získání zdrojového kódu stránky
        page_source = DRIVER.page_source

        # Ukončení práce s prohlížečem
        DRIVER.quit()

        # zpracování obsahu stránky
        soup = BeautifulSoup(page_source, "html.parser")

        # získání obsahu článku (má itemprop="articleBody")
        self.content = soup.find("div", itemprop="articleBody")

        # získání kategorie článku
        self.category = soup.find("ul", class_="iph-breadcrumb").find_all("li")[-1].text

        # získání počtu fotek
        more_gallery = soup.find("div", class_="more-gallery")
        if more_gallery:
            self.photos_count = int(more_gallery.find("b").text)

        # získání data publikace
        self.published_date = soup.find("span", class_="time-date")["content"]

        # získání počtu komentářů
        comments = soup.find("li", class_="community-discusion")
        if comments:
            self.comments_count = int(comments.find("span").text.split()[0][1:])

    def __str__(self):
        return f"Title: {self.title}\nCategory: {self.category}\nPublished: {self.published_date}\nPhotos: {self.photos_count}\nComments: {self.comments_count}\nURL: {self.url}\nContent: {self.content} "

    def __repr__(self):
        return self.__str__()


def get_articles():
    DRIVER = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    # Otevření webové stránky iDnes.cz
    DRIVER.get("https://www.idnes.cz/zpravy")

    click_accept_cookies(DRIVER)

    DRIVER.get("https://www.idnes.cz/zpravy")

    # Získání zdrojového kódu stránky
    page_source = DRIVER.page_source

    # Ukončení práce s prohlížečem
    DRIVER.quit()

    # zpracování obsahu stránky
    soup = BeautifulSoup(page_source, "html.parser")

    # získání všech článků
    return soup.find_all("div", class_="art")


def get_article_data(article):
    # získání názvu článku
    title = article.find("h3").text.strip()

    # získání URL článku
    url = article.find("a")["href"]

    # vytvoření instance třídy Article
    new_article = Article(title, url)

    return new_article
def click_accept_cookies(driver):
    # Pokud je potřeba, klikneme na tlačítko "Souhlasím"
    try:
        consent_button = driver.find_element(
            By.CSS_SELECTOR, 'a[class="btn-cons contentwall_ok"]'
        )
        consent_button.click()
    except:
        print("Tlačítko pro souhlas nebylo nalezeno")

main_page_articles = get_articles()
articles = []
# for a in get_articles():
#     articles.append(get_article_data(a))
#     print(articles[-1])

# test pro 1 článek
articles.append(get_article_data(main_page_articles[0]))
print(articles[-1])


['https://www.lidovky.cz/nazory/valka-ukrajina-rusko-usa-britove-strely-zbrane-atacms-storm-shadow.A240909_113330_ln_nazory_lgs?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/domaci/policie-skoly-anonymni-vyhrozovani.A240910_085809_domaci_vank', 'https://www.idnes.cz/ekonomika/zahranicni/apple-irsko-soudni-dvur-eu-slevy-na-dani.A240910_110125_eko-zahranicni_rie', 'https://www.idnes.cz/ekonomika/domaci/lihoviny-unie-vyrobcu-ceny-mezirocni-pokles-trh-maloobchod-cesko.A240910_105941_ekonomika_jadv', 'https://www.idnes.cz/kultura/film-televize/oscar-vlny-madl-film-ceskoslovensky-rozhlas.A240909_181551_filmvideo_spm', 'https://www.example.com', 'https://www.idnes.cz/zpravy/zahranicni/rusko-ukrajina-dron-utok-letiste.A240910_062246_zahranicni_vajo', 'https://www.idnes.cz/zpravy/zahranicni/izrael-gaza-utok-mrtvi-uprchlicky-tabor.A240910_061407_zahranicni_vlc', 'https://www.idnes.cz/zpravy/zahranicni/francie-soud-dominique-pelicot-manzelka-gisele-vypoved-znasilneni.A240905_121117_zahranicni_kha', 'https://www.idnes.cz/kultura/film-televize/james-earl-jones-smrt.A240910_060856_filmvideo_vajo', 'https://www.idnes.cz/ekonomika/doprava/zeleznice-letiste-ppp-trat-praha-ruzyne-projekt-draha.A240909_195002_eko-doprava_cfr?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/domaci/krajske-a-senatni-volby-zari-kampan-utrata.A240826_093031_domaci_vank', 'https://www.idnes.cz/zpravy/domaci/ukrajina-rusko-obchazeni-sankci-ponorka-technologie-armada.A240909_201532_domaci_krd?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/zahranicni/rusko-juan-ciana-rubl-ekonomika.A240909_114239_zahranicni_aha', 'https://www.idnes.cz/ekonomika/domaci/hospoda-restaurace-hladinka-podmirak-coi-pokuty.A240909_172313_ekonomika_vebe', 'https://www.idnes.cz/finance/investovani/penzijni-sporeni-prispevek-duchodci-zhodnoceni-penez-rady-tipy.A240909_113734_inv_frp', 'https://www.idnes.cz/zpravy/zahranicni/podkarpatska-rus-david-svoboda-ukrajina.A240902_094805_zahranicni_aha?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/zahranicni/nemecko-faeserova-kontroly-hranice-migrace.A240909_154258_zahranicni_rtn', 'https://www.idnes.cz/olomouc/zpravy/prostejov-infekcni-krvaciva-horecka-dengue-prujmy-zloutenka.A240909_817101_olomouc-zpravy_stk', 'https://www.idnes.cz/zpravy/revue/spolecnost/adam-misik-herec-zpevak-jedna-rodina-kubelkova-jiraskova.A240902_101026_lidicky_sub?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/zahranicni/moskva-rusko-iran-rakety.A240909_211109_zahranicni_kurl', 'https://www.idnes.cz/zpravy/zahranicni/ruske-sahed-drony-lotyssko-rumunsko.A240909_220521_zahranicni_blp', 'https://www.idnes.cz/plzen/zpravy/halze-david-kratochvil-paralympiada-pariz-medaile-plavani.A240909_190912_plzen-zpravy_pari', 'https://www.idnes.cz/brno/zpravy/objev-sintrove-jezirko-jeskynari-kralova-jeskyne-tisnov.A240909_104425_brno-zpravy_krut', 'https://www.idnes.cz/ekonomika/zahranicni/bill-gates-warren-buffett-duchod-dovolena.A240909_094840_eko-zahranicni_klak', 'https://www.idnes.cz/ekonomika/domaci/avon-rossmann-expanze-volne-prodejne-osobni-prodej-zakaznici.A240909_145545_ekonomika_vebe', 'https://www.idnes.cz/brno/zpravy/brno-pruvod-protest-lanovka-dominikanske-namesti-ornitologove.A240909_192313_domaci_zof', 'https://www.idnes.cz/ekonomika/zahranicni/rusko-stinova-flotila-asie-naklad-zemni-plyn-lng-dodavky.A240909_112337_eko-zahranicni_jla', 'https://www.idnes.cz/zpravy/zahranicni/slovensko-zuzana-caputova-kariera-stanford-univerzita-hostovani.A240909_150926_zahranicni_kha', 'https://www.idnes.cz/hradec-kralove/zpravy/akupunktura-janske-lazne-lekar-olsak-postizeni-lecba.A240909_150756_hradec-zpravy_pos', 'https://www.idnes.cz/praha/zpravy/nahlaseni-bomby-obchodni-centrum-butovice-zasah-policie-soud.A240909_160246_praha-zpravy_iri', 'https://www.idnes.cz/zpravy/zahranicni/rusky-vojak-lapil-dron-ktery-ho-pronasledoval-bouchl-mu-v-ruce.A240909_153511_zahranicni_aha', 'https://www.idnes.cz/kultura/film-televize/marek-dobes-film-dablova-sbirka-cirkev-restituce-propagace.A240820_141807_filmvideo_vals?zdroj=zpravodaj_hp', 'https://www.idnes.cz/olomouc/zpravy/becva-obnova-prirozeneho-koryta-memorandum-povodne-ochrana.A240909_171116_olomouc-zpravy_stk', 'https://www.idnes.cz/brno/zpravy/socha-svaty-krystof-presun-marius-kotrba-vystava.A240909_154651_brno-zpravy_krut', 'https://www.idnes.cz/zpravy/domaci/duchodova-reforma-az-po-senatnich-a-krajskych-volbach-rozhovor-jakob-stavebni-rizeni-xaver-vesely.A240909_152643_domaci_kop', 'https://www.idnes.cz/zpravy/zahranicni/new-york-migrace-azylovy-dum-pomoc.A240908_194548_zahranicni_mejt?zdroj=zpravodaj_hp', 'https://www.idnes.cz/zpravy/zahranicni/manspreading-zena-letadlo-cestujici-nohy-ozkrocene.A240909_145832_zahranicni_kurl']


def get_article_details_selenium(url):
    """Stáhne detaily článku pomocí Selenium."""
    driver.get(url)

    
    # Získání zdrojového kódu stránky po načtení článku
    page_source = driver.page_source

    # Zpracování pomocí BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')

    # Získání informací z článku
    title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Unknown Title'
    content = ' '.join(p.get_text(strip=True) for p in soup.select('div[itemprop="articleBody"] p')) or 'Unknown Content'
    category = soup.select_one("ul.iph-breadcrumb li:last-child").get_text(strip=True) if soup.select_one("ul.iph-breadcrumb") else 'Unknown Category'
    image_count = int(soup.select_one("div.more-gallery b").get_text(strip=True)) if soup.select_one("div.more-gallery") else 0
    date_published = soup.select_one("span.time-date")["content"] if soup.select_one("span.time-date") else 'Unknown Date'
    comments_count = int(soup.select_one("li.community-discusion span").get_text(strip=True).split()[0][1:]) if soup.select_one("li.community-discusion span") else 0
    

    # Vrátíme slovník s daty
    return {
        'title': title,
        'content': content,
        'category': category,
        'image_count': image_count,
        'date_published': date_published,
        'comments': comments_count
    }
