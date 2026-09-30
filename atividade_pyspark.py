"""Rocket Lab Dados 2026.2 - Atividade PySpark (resolvida)


# ======================================================================
# Setup e carga de metal_bands
# ======================================================================
import os, sys
# Faz o Spark usar o mesmo Python que está rodando o script (evita o atalho da Microsoft Store no Windows)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

#Criar sessão spark
spark = SparkSession.builder.appName("AtividadePraticaSpark").getOrCreate()

# Pasta onde estão os .csv (ajuste se necessário)
DATA_DIR = "./data/"

# Compatibilidade: fora do Databricks não existem display() nem DataFrame.display()
try:
    display
except NameError:
    def display(obj):
        if isinstance(obj, DataFrame):
            obj.show(truncate=False)
        else:
            print(obj)
if not hasattr(DataFrame, "display"):
    DataFrame.display = lambda self: self.show(truncate=False)

# Execute esta célula para carregar o dataframe metal_bands com dados de bandas de metal
metal_bands = spark.read.csv(DATA_DIR + "metal_bands.csv", header=True, inferSchema=True)

metal_bands.printSchema()
display(metal_bands.limit(5))

# ======================================================================
# 1. Juntando DataFrames (exemplos)
# ======================================================================
# ano de formação e país das bandas
bands_origin = metal_bands.select('id','band_name','formed','origin')

# estilo das bandas
bands_style = metal_bands.select('id','band_name','style') # estilo das bandas

# bandas que se separaram
bands_split = (metal_bands
               .select('id','band_name','split')
               .where(F.column('split') != "-")
               )

# bandas com mais de 4000 fans
bands_4000_fans = (metal_bands
                   .select('id','band_name','fans')
                   .where(F.column('fans') > 4000)
                   )

# bandas formadas nos EUA
bands_USA = (metal_bands
             .select('id','band_name','formed','origin')
             .where(F.column('origin') == "USA")
             )

# bandas formadas na Suécia
bands_Sweden = (metal_bands
                .select('id','band_name','formed','origin')
                .where(F.column('origin') == 'Sweden')
                )

# ======================================================================
# Inner join
# ======================================================================
origin_split = (bands_origin # o DataFrame da esquerda
                .join(bands_split, # o DataFrame da direita
                      on=['id', 'band_name'], # baseado em quais valores em comum (chave)
                      how='inner' # o tipo de join que queremos fazer
                      )
                )
display(origin_split.limit(5))

# ======================================================================
# Left join
# ======================================================================
left_origin_split = (bands_origin
                     .join(bands_split,
                           on= ['id', 'band_name'],
                           how="left"
                           )
                     )
display(left_origin_split.limit(5))

# ======================================================================
# Right join
# ======================================================================
right_origin_split = (bands_origin
                     .join(bands_split,
                           on= ['id', 'band_name'],
                           how="right"
                           )
                     )
display(right_origin_split.limit(5))

# ======================================================================
# Outer join
# ======================================================================
print('Numero de linhas do DataFrame bands_4000_fans:', bands_4000_fans.count())
print('Numero de linhas do DataFrame bands_USA:', bands_USA.count())
print('----------------------------------------------')

outer_origin_split = (bands_4000_fans
                     .join(bands_USA,
                           on= ['id', 'band_name'],
                           how="outer"
                           )
                     )

print('Numero de linhas do DataFrame após Outer entre bands_4000_fans & bands_USA:', outer_origin_split.count())
display(outer_origin_split.limit(5))

# ======================================================================
# Union
# ======================================================================
# concatenando bandas formadas nos EUA e bandas formadas na Suécia
USA_Sweden = bands_USA.union(bands_Sweden)

print('Numero de linhas do DataFrame bands_USA:', bands_USA.count())
print('Numero de linhas do DataFrame bands_Sweden:', bands_Sweden.count())
print('Numero de linhas do DataFrame após union entre bands_USA & bands_Sweden:', USA_Sweden.count())
display(USA_Sweden.limit(5))

# ======================================================================
# Exercício 1.1 - Leitura do fut_players
# ======================================================================
# Faça a leitura do arquivo fut_players
# (multiLine/quote/escape: as colunas traits e specialities têm vírgulas dentro de aspas)
fut_players = spark.read.csv(
    DATA_DIR + "fut_players_data.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"',
    multiLine=True
)

# Retorne as 5 primeiras linhas do DF
display(fut_players.limit(5))

# ======================================================================
# Exercício 1.2 - Jogadores The Bests + nacionalidade
# ======================================================================
# Aplique os filtros para retornar os jogadores the bests (dribbling e shooting superiores a 90)
the_best = (fut_players
            .select('player_id', 'player_name', 'position', 'dribbling', 'shooting', 'overall')
            .where((F.col('dribbling') > 90) & (F.col('shooting') > 90))
            )

# nationalities é um DataDrame da nacionalidade dos jogadores
nationalities = (fut_players.select('player_id', 'player_name', 'nationality'))

# faça um join dos dois DataDrames, mantendo todos os jogadores de the_best e obtendo suas nacionalidades (dica: a chave é o id)
the_best_nationality = (the_best
                        .join(nationalities,
                              on=['player_id', 'player_name'],
                              how='left')
                        .select('player_id', 'player_name', 'nationality',
                                'position', 'dribbling', 'shooting', 'overall')
                        )

the_best_nationality.display()

# ======================================================================
# 2. Alterando o dataframe - carga do pokemon
# ======================================================================
pkmn = spark.read.csv(DATA_DIR + "pokemon_data.csv", header=True, inferSchema=True)

# Renomeando as colunas (os exemplos abaixo usam Type_1, Sp_Atk, etc.)
pkmn = (
    pkmn
    .withColumnRenamed("Type 1", "Type_1")
    .withColumnRenamed("Type 2", "Type_2")
    .withColumnRenamed("Sp. Atk", "Sp_Atk")
    .withColumnRenamed("Sp. Def", "Sp_Def")
)

display(pkmn.limit(5))

# ======================================================================
# Nova coluna
# ======================================================================
# Criando a coluna desejada
pkmn = pkmn.withColumn("Sum_Attack_Speed", F.col("Attack") + F.col("Speed"))
display(pkmn.limit(5))

# ======================================================================
# Valores únicos Type_1 (Speed > 100)
# ======================================================================
# Observe os valores unicos da coluna Type_1 para os Pokémons com mais de 100 de velocidade
pkmn.filter(pkmn["Speed"] > 100).select("Type_1").distinct().display()

# ======================================================================
# Alterando linhas com when/otherwise
# ======================================================================
# Vamos alterar os casos onde Speed é superior a 100 para Fire
pkmn = pkmn.withColumn(
    "Type_1",
    F.when(pkmn["Speed"] > 100, "Fire").otherwise(pkmn["Type_1"])
)

# ======================================================================
# Conferindo alteração
# ======================================================================
# Observe como os valores mudaram
pkmn.filter(pkmn["Speed"] > 100).select("Type_1").distinct().display()

# ======================================================================
# Relendo o arquivo pokemon
# ======================================================================
pkmn = spark.read.csv(DATA_DIR + "pokemon_data.csv", header=True, inferSchema=True)

# Renomeando as colunas
pkmn = (
    pkmn
    .withColumnRenamed("Type 1", "Type_1")
    .withColumnRenamed("Type 2", "Type_2")
    .withColumnRenamed("Sp. Atk", "Sp_Atk")
    .withColumnRenamed("Sp. Def", "Sp_Def")
)

# ======================================================================
# 3. Operações em grupo - lendários por geração
# ======================================================================
pkmn_soma = (pkmn
            .groupBy("Generation") # Campo que sera agrupado
            .agg(
                F.sum(F.col("Legendary").cast("int")) # Converte a coluna "Legendary" em inteiro e faz a soma
                .alias("Qtd_Legendary") # Nomeando a coluna que receberá o resultado da soma
                )
            )
pkmn_soma.display()

# ======================================================================
# Médias por tipo
# ======================================================================
pkmn_media = (pkmn
                .groupBy("Type_1")
                .agg(
                    F.mean("HP").alias("HP_medio"),
                    F.mean("Attack").alias("Attack_medio"),
                    F.mean("Defense").alias("Defense_medio")
                    )
                )
pkmn_media.display()

# ======================================================================
# Exercício 2 - Melhor overall médio por país
# ======================================================================
country_avg_overall = (fut_players
                       .groupBy("nationality")
                       .agg(
                           F.max("overall").alias("overall"),        # melhor overall do país
                           F.mean("overall").alias("avg_overall")    # overall médio do país
                           )
                       )

# Retornar a nacionalidade com maior overall médio e o overall médio do brasil
melhor = (
    country_avg_overall
    .orderBy(F.col("avg_overall").desc())
    .limit(1)
    .collect()[0]
)

brasil = (
    country_avg_overall
    .filter(F.col("nationality") == "Brazil")
    .collect()[0]
)

display({
    "Melhor overall médio": f"{melhor['nationality']}: {melhor['avg_overall']:.2f}",
    "Overall médio do Brasil": round(brasil['avg_overall'], 2)
})

# ======================================================================
# Exercício 2.1 - Classificação por overall
# ======================================================================
"""
    Através do overall do jogador retorne a classificação conforme a seguir:
    Overall -> classification
    -50     -> "Amador"
    51-60   -> "Ruim"
    61-70   -> "Ok"
    71-80   -> "Bom"
    81-90   -> "Ótimo"
    91+     -> "Lenda"
    
    I: int overall
    O: string
"""
# Dica utilize as clasulas when e otherwise
# (a coluna de id na base é player_id; usamos um novo DF para não sobrescrever fut_players)
fut_classified = (fut_players
               .select('player_id', 'overall')
               .withColumn('classification',
                           F.when(F.col('overall') <= 50, "Amador")
                            .when(F.col('overall') <= 60, "Ruim")
                            .when(F.col('overall') <= 70, "Ok")
                            .when(F.col('overall') <= 80, "Bom")
                            .when(F.col('overall') <= 90, "Ótimo")
                            .otherwise("Lenda"))
               )

# Contar quantos jogadores há em cada classificação
fut_classified.groupBy("classification").count().orderBy("count", ascending=False).display()

# ======================================================================
# Desafio - Dream Team do Brasil (4-4-2)
# ======================================================================
#############################################
########## Construa sua Query Aqui ##########
#############################################

# Realize novamente o import da base
fut_players = spark.read.csv(
    DATA_DIR + "fut_players_data.csv",
    header=True, inferSchema=True, quote='"', escape='"', multiLine=True
)

# Agrupamento das posições
brasil = (fut_players
          .where(F.col("nationality") == "Brazil")
          .withColumn("position_group",
                      F.when(F.col("position") == "GK", "Goleiro")
                       .when(F.col("position").isin("CB", "LB", "RB", "LWB", "RWB"), "Defesa")
                       .when(F.col("position").isin("CM", "CDM", "CAM", "LM", "RM"), "Meio")
                       .when(F.col("position").isin("ST", "CF", "LW", "RW", "LF", "RF"), "Ataque")
                       .otherwise("Outros"))
          .where(F.col("position_group") != "Outros")
          )

# Formação 4-4-2: quantos jogadores por grupo
vagas = (spark.range(1)
         .select(F.explode(F.array(
             F.struct(F.lit("Goleiro").alias("position_group"), F.lit(1).alias("vagas")),
             F.struct(F.lit("Defesa").alias("position_group"), F.lit(4).alias("vagas")),
             F.struct(F.lit("Meio").alias("position_group"), F.lit(4).alias("vagas")),
             F.struct(F.lit("Ataque").alias("position_group"), F.lit(2).alias("vagas"))
         )).alias("v"))
         .select("v.position_group", "v.vagas"))

# Ranking por overall dentro de cada grupo (player_id desempata de forma determinística)
w = Window.partitionBy("position_group").orderBy(F.col("overall").desc(), F.col("player_id"))

dream_team = (brasil
              .withColumn("rank", F.row_number().over(w))
              .join(vagas, on="position_group", how="inner")
              .where(F.col("rank") <= F.col("vagas"))
              .select("nationality", "position_group", "player_name", "overall")
              .orderBy(F.when(F.col("position_group") == "Goleiro", 1)
                        .when(F.col("position_group") == "Defesa", 2)
                        .when(F.col("position_group") == "Meio", 3)
                        .otherwise(4),
                       F.col("overall").desc())
              )

dream_team.display()

# ======================================================================
# Desafio Bônus - Dream Team sem repetição
# ======================================================================
#############################################
########## Construa sua Query Aqui ##########
#############################################

# Mantém só a carta de maior overall de cada jogador.
# Todas as cartas do mesmo jogador compartilham o mesmo base_id (player_id é único por carta).
w_unico = Window.partitionBy("base_id").orderBy(F.col("overall").desc(), F.col("player_id"))

brasil_unico = (brasil
                .withColumn("rank_carta", F.row_number().over(w_unico))
                .where(F.col("rank_carta") == 1)
                .drop("rank_carta")
                )

# Reaplica a lógica do 4-4-2
w_pos = Window.partitionBy("position_group").orderBy(F.col("overall").desc(), F.col("player_id"))

dream_team_unico = (brasil_unico
                    .withColumn("rank", F.row_number().over(w_pos))
                    .join(vagas, on="position_group", how="inner")
                    .where(F.col("rank") <= F.col("vagas"))
                    .select("nationality", "position_group", "player_name", "overall")
                    .orderBy(F.when(F.col("position_group") == "Goleiro", 1)
                              .when(F.col("position_group") == "Defesa", 2)
                              .when(F.col("position_group") == "Meio", 3)
                              .otherwise(4),
                             F.col("overall").desc())
                    )

dream_team_unico.display()
