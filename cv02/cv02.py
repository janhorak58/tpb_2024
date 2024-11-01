import json

def load_articles_from_file(file):
    """Načte články ze souboru."""
    with open(file, 'r') as f:
        articles = json.load(f)
    return articles


if __name__ == "__main__":
    articles = load_articles_from_file("articles.json")

    # 1. Výpis počtu článků
    print(f"Počet článků: {len(articles)}")

    # 2. Výpis počtu duplicitních článků
    # Mají stejný název
    titles = [article["title"] for article in articles]
    duplicates = len(titles) - len(set(titles))
    print(f"Počet duplicitních článků: {duplicates}")

    # 3. Vypsat datum nejstaršího článku
    # "date_published": "2006-08-21T12:12:52+02:00"
    import datetime
    dates = [article["date_published"] for article in articles]
    datetime_dates = []
    errcount = 0
    for i, date in enumerate(dates):
        try:
            datetime_dates.append(datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z"))
        except:
            pass
    oldest_date = min(datetime_dates)
    print(f"Nejstarší článek: {oldest_date.strftime('%d.%m.%Y')}")

    # Vypsat jméno článku s nejvíce komentáři
    """
    {
    "title": "Saddám trucoval, k soudu ale nakonec přišel",
    "content": "Saddám bojkotem vyhrožoval už v úterý. Tým advokátů, který má jeho případ na starosti, mu výhrůžky rozmlouval, ale on si nedal říct. Spolu s ním stávkovalo i dalších sedm souzených představitelů bývalého iráckého režimu. Předseda tribunálu prohlásil, že v přelíčení lze pokračovat i za nepřítomnosti obžalovaných. Saddámovi obhájci nakonec soudce přemluvili k tomu, aby ještě vyčkal. Po necelých dvou hodinách nakonec všichni obžalovaní do soudní síně dorazili. Saddám se ihned začal soudu dotazovat na spravedlnost celého procesu. Stěžoval si také na údajně nelidské podmínky ve vězení. Před tribunál pak předstoupil další svědek. Vypovídal za látkovou zástěnou a soudce ho zásadně oslovoval jako \"Svědka F\". Vylíčil praktiky, které Saddámovi spolupracovníci používali při výsleších vězňů v 80. letech minulého století. Jeho výpověď však exprezident opět neslyšel - po přestávce na oběd se totiž do soudní síně nevrátil. Předsedající soudce Rizgar Muhammad Amín další jednání odložil na 21. prosince, tedy necelý týden po parlamentních volbách. Bývalý irácký vůdce se zpovídá z mnoha zločinů proti lidskosti. V prvním procesu čelí obvinění za třiadvacet let starý masakr 148 šíitů v Dudžailu. Exprezidentovi hrozí trest smrti.",
    "category": "Zahraničí",
    "image_count": 0,
    "date_published": "2005-12-07T12:22:00+01:00",
    "comments": 31
},
    """
    comments = [article["comments"] for article in articles]
    max_comments = max(comments)
    article_index = comments.index(max_comments)
    article_title = articles[article_index]["title"]
    print(f"Článek s nejvíce komentáři: {article_title} má {max_comments} komentářů")

    # Vypište nejvyšší počet přidaných fotek v jednom článku
    images = [article["image_count"] for article in articles]
    max_images = max(images)
    article_index = images.index(max_images)
    article_title = articles[article_index]["title"]
    print(f"Článek s nejvíce fotkami: {article_title} má {max_images} fotek")


    # Vypsat počty článků podle roku publikace
    import plotly
    from collections import defaultdict
    years_count = defaultdict(int)
    for date in datetime_dates:
        years_count[date.year] += 1
    print("Počty článků podle roku publikace:")
    for year, count in years_count.items():
        print(f"{year}: {count}")

    # Vytvořte graf počtu článků podle roku publikace
    import plotly.graph_objects as go
    fig = go.Figure(data=go.Scatter(x=list(years_count.keys()), y=list(years_count.values())))
    

    
    
