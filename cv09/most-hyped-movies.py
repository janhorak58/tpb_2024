"""
Pomocí Flink Table API vyberte top 10 filmů s nejvíce 5* hodnoceními 

Vstupní data:
- u.data
- u-mod.items

Výsledný seznam vraťte setříděný podle počtu nejlepších hodnocení
- film, počet 5* hodnocení

ze souboru u-mod.items zjistěte název filmu podle ID

k finální tabulce přidejte sloupec počítající poměr 5* hodnocení
daného filmu vůči všem hodnocením daného filmu


u.data vypada takto:
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
166	346	1	886397596
...

tzn. user_id, item_id, rating, timestamp

u-mod.items vypada takto:
1|Toy Story (1995)
2|GoldenEye (1995)
3|Four Rooms (1995)
4|Get Shorty (1995)
5|Copycat (1995)
6|Shanghai Triad (Yao a yao yao dao waipo qiao) (1995)

tzn. item_id|film
"""
from pyflink.table import TableDescriptor, Schema, DataTypes, FormatDescriptor, EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

td_ratings = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("userID", DataTypes.INT())
            .column("movieID", DataTypes.INT())
            .column("rating", DataTypes.INT())
            .column("timestamp", DataTypes.STRING())  # pokud timestamp nepotřebujete, můžete ho vynechat
            .build()) \
    .option("path", "/files/data/u.data") \
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", "\t")  # data oddělená tabulátorem
            .build()) \
    .build()

table_env.create_temporary_table("ratings", td_ratings)

ratings = table_env.from_path("ratings")

# with ratings.execute().collect() as results:
#     for r in results:
#         print(r)

td_movies = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .build()) \
    .option("path", "/files/data/u-mod.item") \
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", "|")  # data oddělená svislítkem (pipe)
            .build()) \
    .build()

table_env.create_temporary_table("movies", td_movies)

movies = table_env.from_path("movies")

# with movies.execute().collect() as results:
#     for r in results:
#         print(r)

# ratings.execute().print()
# movies.execute().print()


# Filtrace a agregace 5* hodnocení
grouped = ratings.where(ratings.rating == 5) \
    .group_by(ratings.movieID) \
    .select(ratings.movieID.alias("movieID"), 
            ratings.rating.count.alias("cnt5"))

# Celkový počet hodnocení
total_grouped = ratings.group_by(ratings.movieID) \
    .select(ratings.movieID.alias("movieID_total"),  # Přidání unikátního aliasu
            ratings.rating.count.alias("cnt"))

# Spojení tabulek na základě movieID
combined = grouped.join(total_grouped).where(grouped.movieID == total_grouped.movieID_total) \
    .select(grouped.movieID, grouped.cnt5, total_grouped.cnt)

# Zbytek kódu zůstává stejný
with_ratio = combined.select(
    combined.movieID,
    combined.cnt5,
    combined.cnt,
    (combined.cnt5.cast(DataTypes.FLOAT()) / combined.cnt.cast(DataTypes.FLOAT())).alias("ratio")
)

sorted = with_ratio.order_by(with_ratio.cnt5.desc).fetch(10)

final_result = sorted.join(movies).where(sorted.movieID == movies.id) \
    .select(
        movies.name.alias("film"),
        sorted.cnt5.alias("5_star_count"),
        sorted.cnt.alias("total_count"),
        sorted.ratio.alias("5_star_ratio")
    )

final_result.execute().print()

"""
root@226d1c0f567d:/files# python most-hyped-movies.py
+--------------------------------+----------------------+----------------------+--------------------------------+
|                           film |         5_star_count |          total_count |                   5_star_ratio |
+--------------------------------+----------------------+----------------------+--------------------------------+
|               Star Wars (1977) |                  325 |                  583 |                      0.5574614 |
|            Pulp Fiction (1994) |                  188 |                  394 |                     0.47715735 |
| Silence of the Lambs, The (... |                  181 |                  390 |                     0.46410257 |
|                   Fargo (1996) |                  227 |                  508 |                      0.4468504 |
|          Godfather, The (1972) |                  214 |                  413 |                      0.5181598 |
| Empire Strikes Back, The (1... |                  172 |                  367 |                     0.46866485 |
| Raiders of the Lost Ark (1981) |                  202 |                  420 |                     0.48095238 |
|      Return of the Jedi (1983) |                  171 |                  507 |                      0.3372781 |
|                 Titanic (1997) |                  179 |                  350 |                      0.5114286 |
|        Schindler's List (1993) |                  186 |                  298 |                     0.62416106 |
+--------------------------------+----------------------+----------------------+--------------------------------+

"""