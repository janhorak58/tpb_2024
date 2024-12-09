"""
Bonus:

Vhodně navrhněte vstupní tabulku.
Kafka streamuje json objekty ve formátu:
{"title":"Německá Levice má nové vedení. Stranu válcuje AfD a Wagenknechtová","content":"Po svém zvolení van Aken a Schwerdtnerová řekli, že v příštích týdnech budou s dobrovolníky obcházet statisíce domácností, aby mluvili s voliči a voličkami. Podle van Akena následně budou systematicky analyzovat odpovědi, aby podle nich strana vybrala dvě hlavní témata pro kampaň k volbám do Spolkového sněmu v příštím roce. V roce 2021 bylo za stranu do Spolkového sněmu zvoleno celkem 39 poslanců. V prosinci 2023 však její poslanecký klub zanikl poté, co z něj vystoupilo deset poslanců z křídla Sahry Wagenknechtové a klub neměl minimální požadovaný počet členů. Wagenknechtová založila levicově populistickou stranu Spojenectví Sahry Wagenknechtové (BSW), která se profiluje zejména odmítáním pomoci napadené Ukrajině a protiruských sankcí. Levice je od té doby v krizi. Předvolební průzkumy jí přisuzují mezi třemi a čtyřmi procenty. Proč nyní mladí Němci fandí AfD a opouštějí levici. Rázná řešení přitahují  V evropských volbách obdržela 2,7 procenta hlasů a získala tři z 96 poslanců. Vypadla také ze zemského sněmu v Braniborsku, kde byla do té doby ve vládě. V Durynsku, kde byla dosud nejsilnější stranou a měla premiéra, se letos propadla na čtvrté místo a má 12 zemských poslanců místo dosavadních 29. Strana vznikla sloučením Strany demokratického socialismu (PDS), nástupkyně východoněmecké vládnoucí totalitní Sjednocené socialistické strany Německa (SED), a strany WASG, kterou založili bývalí radikální členové německé sociální demokracie (SPD). Před vznikem BSW a vzestupem krajně pravicové Alternativy pro Německo (AfD) se jí dlouhodobě dařilo zejména v takzvaných nových spolkových zemích, tedy v bývalém Východním Německu. 3. února 2024 3. února 2024        ","category":"Zahraničí","image_count":24,"date_published":"2024-10-19T19:24:00+02:00","comments":9}

Vytvořte tabulku, která bude obsahovat sloupce:
- title (STRING)
- content (STRING)
- category (STRING)
- image_count (INT)
- date_published (TIMESTAMP)
- comments (INT)

Přidejte časovou informaci (čas přijetí Flinkem)

Do konzole vypište název zpracovávaného článku

- Detekujte články s více než 100 komentáři
 -> zapisujte je do souboru ve formátu {název článku};{počet komentářů}

- Detekujte články s datem přidání na iDNES mimo pořadí
    	-> články jsou kafkou stramovány sekvenčně
        -> uvažujte čas přidání článků sestupný
        -> detekujte články, které toto pravidlo porušují
        -> zapisujte je do souboru ve formátu název článku;datum článku;předchozí datum

- Data také zpracovávejte posuvným oknem vámi přidaným časem s délkou 1 minuta  aposuvem 10 vteřin
- počítejte celkový počet článků přidaných v daném okně a kolik z nich obsahuje v textu článku výraz "válka"
    -> zapisujte do souboru ve formátu {začátek okna};{konec okna};{počet článků};{počet článků s výrazem "válka"}
Všechny výpisy jsou prováděny zároveň jedním skriptem (1x konzole, 3x soubory) (použijte pipeline)
`
pipeline = env.create_statement_set()
pipeline.add_insert("article_count_sink", article_counts)
pipeline.add_insert("high_priority_sink", high_priority_articles)
pipeline.add_insert("article_title_console_sink", artile_titles)

pipeline.execute().wait()


{
    "title": "Z vězně v USA ministrem ve Venezuele. Maduro do vlády povolal věrného muže",
    "content": "Informoval o tom sever televize Deutsche Welle (DW). Spojené státy Saaba propustily a vydaly do Venezuely loni v prosinci výměnou za deset Američanů. „Jsem si jistý, že se svými velkými manažerskými schopnostmi a oddaností našemu lidu pomůže rozvoji průmyslu ve Venezuely v rámci procesu budování nového ekonomického modelu,“ napsal ke jmenování Saaba Maduro na sociální síti X. Venezuela se už řadu let potýká s ekonomickou krizí, k níž přispěla špatná hospodářská politika socialistické vlády, korupce vládních činitelů, ale i americké sankce zavedené kvůli porušování lidských práv, svobody a demokracie tamním režimem. Pro prezidentskou funkci si přijedu, vzkázal do Venezuely González z exilu  V posledních asi třech letech zažívají některá odvětví tamní ekonomiky oživení a HDP po letech poklesu opět roste. V zemi jsou ale stále velké ekonomické rozdíly mezi bohatými a chudými. Saab býval dříve blízkým Madurovým spolupracovníkem a v minulosti vydělal peníze zejména obchodováním s venezuelskou vládou, od níž získával státní zakázky, mimo jiné na dovoz potravin pro přídělový systém. Potravinové balíčky vláda zavedla v roce 2016 kvůli krizi, systém ale využívá i k získávání voličů. V roce 2020 byl Saab zadržen na Kapverdách při mezipřistání na cestě do Íránu, kam letěl vyjednávat jménem Madurovy vlády dodávky ropy do Venezuely. Venezuela má sice největší zásoby ropy na světě, ale v posledních letech její těžba výrazně klesla kvůli ekonomické krizi, americkým sankcím i dlouhodobě špatnému stavu rafinérii. Opozice viní Madurovu vládu, že neinvestovala do údržby a modernizace sítě rafinérií a část peněz zpronevěřila. 4. září 2024 4. září 2024        ",
    "category": "Zahraničí",
    "image_count": 15,
    "date_published": "2024-10-19T17:06:00+02:00",
    "comments": 10
},
`
"""


#  ============= VYTVORENI TABULEK =================
from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment, 
    DataTypes, 
    Schema, 
    FormatDescriptor, 
    TableDescriptor, 
    StreamTableEnvironment,
    )
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col, current_timestamp
from pyflink.table.udf import udtf
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment

# from pyflink.datastream.watermark_strategy import WatermarkStrategy
from datetime import timedelta
from datetime import datetime


# Nastavení prostředí
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
stream_env = StreamTableEnvironment.create(env)
# Create the Kafka source table
stream_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())  
                .column('content', DataTypes.STRING())
                .column('category', DataTypes.STRING())
                .column('image_count', DataTypes.INT())
                .column('date_published', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .column_by_expression("ts", "CAST(NOW() AS TIMESTAMP(3))")
                .watermark("ts", "ts - INTERVAL '3' SECOND").build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('json')
                .option('ignore-parse-errors', 'true')  # správný formát volby
                .build())
        .build())

# from datetime import datetime
# from pyflink.common.time import Instant


# Read data from Kafka source
data = stream_env.from_path("kafka_source")
# init timestamp
# # Při použití `current_timestamp()` není potřeba explicitně převádět `datetime.now()`
# from pyflink.table.expressions import call

# # Přidání sloupce "formatted_time", který bude obsahovat čas ve formátu "YYYY-MM-DD HH:mm:ss"
# data = data.add_columns(
#     call('DATE_FORMAT', col('t'), lit('yyyy-MM-dd HH:mm:ss')).alias('formatted_time')
# )

# @udtf(result_types=[DataTypes.ROW()])
# def set_time(line:Row):
#     # line.t = Instant.of_epoch_milli(datetime.now().timestamp())
#     line.add_column(Instant.of_epoch_milli(datetime.now().timestamp()))
# data = data.flat_map(set_time)

# print 3 lines
# data.execute().print()



# Přidání sloupce s aktuálním časem
pipeline = stream_env.create_statement_set()


# ======== Výpis názvu zpracovávaného článku =========

stream_env.create_temporary_table(
    'print_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .column('ts', DataTypes.TIMESTAMP())
                .build())
        .build())
titles = data.select(data.title, data.comments, data.ts)
pipeline.add_insert("print_sink", titles)


# ======== Detekce článků s více než 100 komentáři =========
stream_env.create_temporary_table(
    'high_priority_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .build())
        .option('path', '/files/output/high_priority_articles.csv')
        .format(FormatDescriptor.for_format("csv")
                .option('field-delimiter', ';')
                .build())
        .build())
high_priority_articles = data.filter(data.comments > 100) \
                            .select(data.title, data.comments)
pipeline.add_insert("high_priority_sink", high_priority_articles)


# ======== Detekce článků s datem přidání na iDNES mimo pořadí =========
# - Detekujte články s datem přidání na iDNES mimo pořadí
#     	-> články jsou kafkou stramovány sekvenčně
#         -> uvažujte čas přidání článků sestupný
#         -> detekujte články, které toto pravidlo porušují
#         -> zapisujte je do souboru ve formátu název článku;datum článku;předchozí datum

global DATE_PUBLISHED
DATE_PUBLISHED = datetime.now().strftime("%Y-%m-%d%T%H:%M:%S%z")

# UDTF, které vrací výstup s názvy sloupců
@udtf(result_types=[DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()])
def is_bad_order(line: Row):
    # line.date published in this format: "2024-09-30T10:51:00+02:00" (as a string) 2024-09-28 13:30:00+02:00

    global DATE_PUBLISHED
    date_published = line.date_published
    previous_date = DATE_PUBLISHED
    DATE_PUBLISHED = date_published
    try:
        # print(f"Date published: {date_published}, Previous date: {previous_date}")
        # Porovnání data s předchozím datem jako datetime
        date_published = datetime.strptime(date_published, "%Y-%m-%dT%H:%M:%S%z")
        previous_date = datetime.strptime(previous_date, "%Y-%m-%dT%H:%M:%S%z")
        is_bad_order = date_published < previous_date
        if is_bad_order:
            print(f"Bad order: {line.title}, {line.date_published}, {previous_date}")
        return is_bad_order, line.title, date_published.strftime("%Y/%m/%d %H:%M"), previous_date.strftime("%Y/%m/%d %H:%M")

    except ValueError as E:
        print(E)
        print(f"Invalid date format: {date_published}")
        return False, line.title, line.date_published, previous_date
    
    except TypeError as E:
        print(E)
        print(f"Invalid date format: {date_published}, {previous_date}")
        return False, line.title, line.date_published, previous_date
    


stream_env.create_temporary_table(
    'date_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('date_published', DataTypes.STRING())
                .column('previous_date', DataTypes.STRING())
                .build())
        .option('path', '/files/output/date_published.csv')
        .format(FormatDescriptor.for_format("csv")
                .option('field-delimiter', ';')
                .build())
        .build())
# Aplikace UDTF funkce pro detekci špatného pořadí
bad_order = data.flat_map(is_bad_order)
bad_order = bad_order.filter(col('f0') == True)

# Používáme lit() pro nastavení výchozí hodnoty DATE_PUBLISHED s explicitním typem
# Zajistíme, že i pro hodnotu None (null) bude explicitně nastavený typ TIMESTAMP
bad_order = bad_order.select(col('f1').alias('title'), col('f2').alias('date_published'),
                             col("f3").alias("previous_date"))

# Přidání výsledků do výstupní tabulky
pipeline.add_insert("date_sink", bad_order)


# ======== Data zpracovávejte posuvným oknem vámi přidaným časem s délkou 1 minuta  aposuvem 10 vteřin =========
# - Data také zpracovávejte posuvným oknem vámi přidaným časem s délkou 1 minuta  aposuvem 10 vteřin
# - počítejte celkový počet článků přidaných v daném okně a kolik z nich obsahuje v textu článku výraz "válka"


from pyflink.table.window import Tumble, Slide, Session




@udtf(input_types=[DataTypes.STRING()], result_types=[DataTypes.ROW()])
def contains_war(text):
    print(f"Text: {text}")
    return "válka" in text




stream_env.create_temporary_table(
    'window_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('start', DataTypes.TIMESTAMP_LTZ())
                .column('end', DataTypes.TIMESTAMP_LTZ())
                .column('article_count', DataTypes.BIGINT())
                .column('war_count', DataTypes.INT())
                .build())
        .option('path', '/files/output/window.csv')
        .format(FormatDescriptor.for_format("csv")
                .option('field-delimiter', ';')
                .build())
        .build())

windowed_data = data.window(Slide.over(lit(1).minute).every(lit(10).second).on(col('ts')).alias('w')) \
    .group_by(col('w')) \
    .select(
        col('w').start.alias('start'),
        col('w').end.alias('end'),
        lit(1).count.alias('total_articles'),
        col('content').like('%válka%').cast(DataTypes.INT()).sum.alias('valka_occurrences')        
    )
# data.execute().print()
pipeline.add_insert("window_sink", windowed_data)

"""
+I[2024-12-08T21:41:40Z, 2024-12-08T21:42:40Z, 19, 0]
+I[2024-12-08T21:41:50Z, 2024-12-08T21:42:50Z, 19, 0]
+I[2024-12-08T21:42:00Z, 2024-12-08T21:43:00Z, 18, 0]
+I[2024-12-08T21:42:10Z, 2024-12-08T21:43:10Z, 19, 0]
+I[2024-12-08T21:42:20Z, 2024-12-08T21:43:20Z, 19, 2]
+I[2024-12-08T21:42:30Z, 2024-12-08T21:43:30Z, 19, 2]
+I[2024-12-08T21:42:40Z, 2024-12-08T21:43:40Z, 19, 2]
+I[2024-12-08T21:42:50Z, 2024-12-08T21:43:50Z, 19, 2]
+I[2024-12-08T21:43:00Z, 2024-12-08T21:44:00Z, 19, 2]
+I[2024-12-08T21:43:10Z, 2024-12-08T21:44:10Z, 18, 3]
+I[2024-12-08T21:43:20Z, 2024-12-08T21:44:20Z, 18, 2]
+I[2024-12-08T21:43:30Z, 2024-12-08T21:44:30Z, 19, 3]
+I[2024-12-08T21:43:40Z, 2024-12-08T21:44:40Z, 19, 3]
+I[2024-12-08T21:43:50Z, 2024-12-08T21:44:50Z, 19, 3]
"""



pipeline.execute().wait()