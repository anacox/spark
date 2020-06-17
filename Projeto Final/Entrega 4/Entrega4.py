# Databricks notebook source
# DBTITLE 1,Business Case - Qual o valor do frete ideal?
# MAGIC %md 
# MAGIC O Business Case visa entender de que forma as features existentes, e as possiveis novas features a serem criadas para responder melhor o modelo, quanto ao valor de frete que vem sendo empregado, no histórico de vendas e entregas.
# MAGIC 
# MAGIC Considerando o resultado de nossas métricas de medição quanto ao valor de frete, como propriedade intelectual de posse da OLIST:
# MAGIC  1. Propomos a utilização destas informações como insumo para negociação com o mercado de serviços de transporte.
# MAGIC  2. Indicar as os melhores locais de origem para postagem de um produto especifico.

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 1,As bases de dados disponíveis para esse projeto são:
display(dbutils.fs.ls('/mnt/datasets/brazilian-ecommerce/'))

# COMMAND ----------

# DBTITLE 1,Importação das bases
closed_deals = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_closed_deals_dataset.csv', header = True, sep = ',', inferSchema=True)
customers = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_customers_dataset.csv', header = True, sep = ',', inferSchema=True)
geolocation = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_geolocation_dataset.csv', header = True, sep = ',', inferSchema=True)
marketing = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_marketing_qualified_leads_dataset.csv', header = True, sep = ',', inferSchema=True)
order_items = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_order_items_dataset.csv', header = True, sep = ',', inferSchema=True)
order_payments = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_order_payments_dataset.csv', header = True, sep = ',', inferSchema=True)
order_review = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_order_reviews_dataset.csv', header = True, sep = ',', inferSchema=True)
orders = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_orders_dataset.csv', header = True, sep = ',', inferSchema=True)
products = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_products_dataset.csv', header = True, sep = ',', inferSchema=True)
sellers = spark.read.csv('/mnt/datasets/brazilian-ecommerce/olist_sellers_dataset.csv', header = True, sep = ',', inferSchema=True)
product_category = spark.read.csv('/mnt/datasets/brazilian-ecommerce/product_category_name_translation.csv', header = True, sep = ',', inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "order_items"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_items.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_items.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "orders"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in orders.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(orders.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento das variáveis nas variáveis do tipo data da base "orders"
orders = orders.dropDuplicates()

from pyspark.sql.functions import unix_timestamp
orders = orders.withColumn('order_approved_at', unix_timestamp('order_approved_at'))
orders = orders.withColumn('order_delivered_carrier_date', unix_timestamp('order_delivered_carrier_date'))

orders = orders.fillna('NA', subset=['order_approved_at'])
orders = orders.fillna('NA', subset=['order_delivered_carrier_date'])
orders = orders.fillna('NA', subset=['order_delivered_customer_date'])

# COMMAND ----------

# DBTITLE 1,Remoção de linhas que contém NA
orders = orders.dropna()
orders.count()

# COMMAND ----------

# DBTITLE 1,Verificação da quantidade de missing na base "orders" após o tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in orders.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(orders.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "order_payments"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_payments.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_payments.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missings da order_payments
order_payments = order_payments.dropDuplicates()

installments = order_payments[['payment_installments']].agg({'payment_installments': 'mean'}).collect()[0][0]
order_payments = order_payments.fillna(installments, subset=['payment_installments'])

valor_pagamentos = order_payments[['payment_value']].agg({'payment_value': 'mean'}).collect()[0][0]
order_payments = order_payments.fillna(valor_pagamentos, subset=['payment_value'])

order_payments = order_payments.dropna()
order_payments.count()

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "products"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in products.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(products.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missing das variáveis quantitativas da base "products"
lenght = products[['product_name_lenght']].agg({'product_name_lenght': 'mean'}).collect()[0][0]
products = products.fillna(lenght, subset=['product_name_lenght'])

descrip = products[['product_description_lenght']].agg({'product_description_lenght': 'mean'}).collect()[0][0]
products = products.fillna(descrip, subset=['product_description_lenght'])

photo = products[['product_photos_qty']].agg({'product_photos_qty': 'mean'}).collect()[0][0]
products = products.fillna(photo, subset=['product_photos_qty'])

weight = products[['product_weight_g']].agg({'product_weight_g': 'mean'}).collect()[0][0]
products = products.fillna(weight, subset=['product_weight_g'])

lenght_cm = products[['product_length_cm']].agg({'product_length_cm': 'mean'}).collect()[0][0]
products = products.fillna(lenght_cm, subset=['product_length_cm'])

height = products[['product_height_cm']].agg({'product_height_cm': 'mean'}).collect()[0][0]
products = products.fillna(height, subset=['product_height_cm'])

width = products[['product_width_cm']].agg({'product_width_cm': 'mean'}).collect()[0][0]
products = products.fillna(width, subset=['product_width_cm'])

# COMMAND ----------

# DBTITLE 1,Tratamento de missings nas variáveis categóricas da base "products"
products = products.fillna('NA', subset=['product_category_name'])

# COMMAND ----------

# DBTITLE 1,Remoção das linhas que contém NA da base "products"
products = products.dropDuplicates()
products = products.dropna()
products.count()

# COMMAND ----------

# DBTITLE 1,Verificação da quantidade de missing na base "products"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in products.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(products.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "customers"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in customers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(customers.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "sellers"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in sellers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(sellers.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "product_category"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in product_category.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(product_category.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "geolocation"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in geolocation.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(geolocation.select(aux))

# COMMAND ----------

# DBTITLE 1,Preparação da base "Geolocation" para "Customers" e "Sellers"
#if geolocation.count() - geolocation.dropDuplicates().count() > 0:

#Customers
df_Geo_Customers = geolocation
df_Geo_Customers = df_Geo_Customers.repartition(2).cache()
df_Geo_Customers.count()
  
df_Geo_Customers = df_Geo_Customers.fillna('NA', subset=['geolocation_zip_code_prefix'])
df_Geo_Customers = df_Geo_Customers.fillna('NA', subset=['geolocation_lat'])
df_Geo_Customers = df_Geo_Customers.fillna('NA', subset=['geolocation_lng'])
  
df_Geo_Customers = df_Geo_Customers.dropna()
df_Geo_Customers = df_Geo_Customers.dropDuplicates()
df_Geo_Customers = df_Geo_Customers.dropDuplicates(['geolocation_zip_code_prefix'])
  
#Sellers  
df_Geo_Sellers   = geolocation
df_Geo_Sellers = df_Geo_Sellers.repartition(2).cache()
df_Geo_Sellers.count()
  
df_Geo_Sellers = df_Geo_Sellers.fillna('NA', subset=['geolocation_zip_code_prefix'])
df_Geo_Sellers = df_Geo_Sellers.fillna('NA', subset=['geolocation_lat'])
df_Geo_Sellers = df_Geo_Sellers.fillna('NA', subset=['geolocation_lng'])
  
df_Geo_Sellers = df_Geo_Sellers.dropna()
df_Geo_Sellers = df_Geo_Sellers.dropDuplicates()
df_Geo_Sellers = df_Geo_Sellers.dropDuplicates(['geolocation_zip_code_prefix'])

#----------------------------------------------------------------------------------------------------------------
#Customers
df_Geo_Customers = df_Geo_Customers.select('geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng')
df_Geo_Customers = df_Geo_Customers.withColumnRenamed("geolocation_zip_code_prefix", "customer_zip_code_prefix")
df_Geo_Customers = df_Geo_Customers.withColumnRenamed("geolocation_lat"            , "geolocation_lat_customer")
df_Geo_Customers = df_Geo_Customers.withColumnRenamed("geolocation_lng"            , "geolocation_lng_customer")

#Sellers
df_Geo_Sellers = df_Geo_Sellers.select('geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng')
df_Geo_Sellers = df_Geo_Sellers.withColumnRenamed("geolocation_zip_code_prefix", "seller_zip_code_prefix")
df_Geo_Sellers = df_Geo_Sellers.withColumnRenamed("geolocation_lat"            , "geolocation_lat_seller")
df_Geo_Sellers = df_Geo_Sellers.withColumnRenamed("geolocation_lng"            , "geolocation_lng_seller")

display(df_Geo_Sellers)


# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base df_Geo_Customers
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in df_Geo_Customers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(df_Geo_Customers.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base df_Geo_Sellers
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in df_Geo_Sellers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(df_Geo_Sellers.select(aux))

# COMMAND ----------

# DBTITLE 1,Criação da base "tabelaUF" via request de API do IBGE para merge com tabelas customer e sellers
#Com estes dados eu crio o codigo do estado para melhorar a acuracia de nosso modelo, podendo utilizar assim o estado como feature na regressao linear
import json
import requests
import copy

from pyspark.sql.functions import *
from pyspark.sql.types import *

def jsonToDataFrame(json, schema=None):
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

response = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/estados')

dfUF = jsonToDataFrame(response.json())

display(dfUF)

# COMMAND ----------

# DBTITLE 1,Merge UF base "Customers" e "Sellers"
#Customers
tabelaUF = dfUF.select('id', 'sigla')
tabelaUF = tabelaUF.withColumnRenamed("id", "codigo_UF_customers")
tabelaUF = tabelaUF.withColumnRenamed("sigla", "customer_state")
tabelaUF_customers = tabelaUF.select('codigo_UF_customers', 'customer_state')

#Sellers
tabelaUF = dfUF.select('id', 'sigla')
tabelaUF = tabelaUF.withColumnRenamed("id", "codigo_UF_sellers")
tabelaUF = tabelaUF.withColumnRenamed("sigla", "seller_state")
tabelaUF_sellers = tabelaUF.select('codigo_UF_sellers', 'seller_state')

# COMMAND ----------

# DBTITLE 1,União (left join) das bases no dataframe "data_merge"
#União das bases "orders" com "order_items" utilizando a coluna "order_id".
data_merge = order_items.join(orders, "order_id",how='left')

data_merge = data_merge.repartition(2).cache()
data_merge.count()
data_merge = data_merge.dropDuplicates()

#União das bases "data_merge" com "order_items" utilizando a coluna "order_id".
data_merge = data_merge.join(order_payments, "order_id",how='left')

#União das bases "data_merge" com "products" utilizando a coluna "product_id".
data_merge = data_merge.join(products, "product_id",how='left')

#União das bases "data_merge" com "customers" utilizando a coluna "customer_id".
data_merge = data_merge.join(customers, "customer_id",how='left')

#União das bases "data_merge" com "df_Geo_Customers" utilizando a coluna "customer_zip_code_prefix".
data_merge = data_merge.join(df_Geo_Customers, "customer_zip_code_prefix" ,how='left')

#União das bases "data_merge" com "sellers" utilizando a coluna "seller_id".
data_merge = data_merge.join(sellers, "seller_id" ,how='left')

#União das bases "data_merge" com "df_Geo_Sellers" utilizando a coluna "seller_zip_code_prefix".
data_merge = data_merge.join(df_Geo_Sellers, "seller_zip_code_prefix" ,how='left')

#União das bases "data_merge" com "Tabelas UF" utilizando a coluna "UF".
data_merge = data_merge.join(tabelaUF_customers, "customer_state" ,how='left')

#União das bases "data_merge" com "Tabelas UF" utilizando a coluna "UF".
data_merge = data_merge.join(tabelaUF_sellers, "seller_state" ,how='left')

data_merge = data_merge.repartition(2).cache()
data_merge.count()

display(data_merge)

# COMMAND ----------

# DBTITLE 1,Numero de linhas e colunas do dataframe "data_merge"
((data_merge.count(), len(data_merge.columns)))
#117604

# COMMAND ----------

# DBTITLE 1,Quantidade de missing do dataframe "data_merge"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in data_merge.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(data_merge.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missings nas variáveis categóricas do dataframe "data_merge" 

data_merge = data_merge.fillna('NA', subset=['customer_id'])
data_merge = data_merge.fillna('NA', subset=['order_status'])
data_merge = data_merge.fillna('NA', subset=['order_purchase_timestamp'])
data_merge = data_merge.fillna('NA', subset=['order_approved_at'])
data_merge = data_merge.fillna('NA', subset=['order_delivered_carrier_date'])
data_merge = data_merge.fillna('NA', subset=['order_delivered_customer_date'])
data_merge = data_merge.fillna('NA', subset=['order_estimated_delivery_date'])
data_merge = data_merge.fillna('NA', subset=['payment_sequential'])
data_merge = data_merge.fillna('NA', subset=['payment_type'])
data_merge = data_merge.fillna('NA', subset=['product_category_name'])
data_merge = data_merge.fillna('NA', subset=['customer_unique_id'])
data_merge = data_merge.fillna('NA', subset=['customer_zip_code_prefix'])
data_merge = data_merge.fillna('NA', subset=['customer_city'])
data_merge = data_merge.fillna('NA', subset=['customer_state'])


# COMMAND ----------

# DBTITLE 1,Exclusão das linhas duplicadas
data_merge = data_merge.dropna()
data_merge = data_merge.dropDuplicates()
((data_merge.count(), len(data_merge.columns)))

# COMMAND ----------

# DBTITLE 1,Cálculo da coluna "Volume"
data_merge = data_merge.withColumn("volume", col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))
display(data_merge)

# COMMAND ----------

# DBTITLE 1,Quantidade de missing do dataframe "data_merge"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in data_merge.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(data_merge.select(aux))

# COMMAND ----------

# DBTITLE 1,Quais os tipos de dados que o dataframe "data_merge" possui?
data_merge.dtypes

# COMMAND ----------

# DBTITLE 1,Quantas linhas possui o dataframe "data_merge"?
data_merge = data_merge.repartition(2).cache()
data_merge.count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico do dataframe "data_merge"
display(data_merge.select('*')\
        .summary('count',
                'min',
                '25%',
                'mean',
                'stddev',
                '50%',
                '75%',
                '85%',
                '95%',
                '99%',
                'max')
       )   
   

# COMMAND ----------

# DBTITLE 1,Remoção das linhas que tem NA na base "data_merge" 
data_merge = data_merge.dropna()
data_merge.count()

# COMMAND ----------

# DBTITLE 1,Conversão .toPandas
pandas_data_merge = data_merge.toPandas()

# COMMAND ----------

# DBTITLE 1,Heatmap da Correlação
# MAGIC %python
# MAGIC import seaborn as sns
# MAGIC import matplotlib.pyplot as plt
# MAGIC sns.set(rc={'figure.figsize':(20,20)})
# MAGIC 
# MAGIC #fig, ax = plt.figure()
# MAGIC sns.heatmap(pandas_data_merge.corr(), xticklabels=pandas_data_merge.corr().columns, yticklabels=pandas_data_merge.corr().columns,cmap='RdBu_r',annot=True, cbar=False)
# MAGIC display()

# COMMAND ----------

# DBTITLE 1,Heatmap da Covariância
# MAGIC %python
# MAGIC import seaborn as sns
# MAGIC import matplotlib.pyplot as plt
# MAGIC sns.set(rc={'figure.figsize':(20,20)})
# MAGIC 
# MAGIC #fig, ax = plt.figure()
# MAGIC sns.heatmap(pandas_data_merge.cov(), xticklabels=pandas_data_merge.cov().columns, yticklabels=pandas_data_merge.cov().columns,cmap='RdBu_r',annot=True, cbar=False)
# MAGIC display()

# COMMAND ----------

# DBTITLE 1,Criação do campo Distancia entre vendedores e compradores
def get_distance(longit_a, latit_a, longit_b, latit_b):
  # Transform to radians
  longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
  dist_longit = longit_b - longit_a
  dist_latit = latit_b - latit_a
  
  # Calculate area
  area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
  
  # Calculate the central angle
  central_angle = 2 * asin(sqrt(area))
  
  radius = 6371
  # Calculate Distance
  
  distance = central_angle * radius
  
  return abs(round(distance, 2))

#	geolocation_lat_customer	geolocation_lng_customer	seller_city	geolocation_lat_seller	geolocation_lng_seller
data_merge = data_merge.withColumn('distancia_sellers_customers', get_distance('geolocation_lng_seller', 'geolocation_lat_seller', 'geolocation_lng_customer', 'geolocation_lat_customer'))
display(data_merge)

# COMMAND ----------

pandas_data_merge = data_merge.toPandas()

# COMMAND ----------

# DBTITLE 1,Heatmap da Correlação
# MAGIC %python
# MAGIC import seaborn as sns
# MAGIC import matplotlib.pyplot as plt
# MAGIC sns.set(rc={'figure.figsize':(20,20)})
# MAGIC 
# MAGIC #fig, ax = plt.figure()
# MAGIC sns.heatmap(pandas_data_merge.corr(), xticklabels=pandas_data_merge.corr().columns, yticklabels=pandas_data_merge.corr().columns,cmap='RdBu_r',annot=True, cbar=False)
# MAGIC display()

# COMMAND ----------

# MAGIC %md Verificamos via HeatMap que existe multicolinearidade entre as features order_approved_at e order_delivered_carrier_date, portanto retiramos a featutre order_approved_at do modelo

# COMMAND ----------

# DBTITLE 1,Criação do dataframe "df", que contém apenas variáveis numéricas
df = data_merge.select('volume',
                       'distancia_sellers_customers',
                       'order_delivered_carrier_date',
                       'price', 
                       'payment_value',                
                       'product_weight_g', 
                       'codigo_UF_customers',
                       'codigo_UF_sellers',
                       'customer_zip_code_prefix',
                       'geolocation_lat_customer',
                       'geolocation_lng_customer',
                       'seller_zip_code_prefix',
                       'geolocation_lat_seller',
                       'geolocation_lng_seller',
                       'freight_value'
              )  

df = df.repartition(2).cache()
df.count()

display(df)

# COMMAND ----------

# DBTITLE 1,Exclusão das linhas duplicadas
df = df.dropDuplicates()
df.count()

# COMMAND ----------

df.dtypes

# COMMAND ----------

# DBTITLE 1,Analisando status das vendas
display(data_merge.select('order_status').groupBy('order_status').count())

# COMMAND ----------

# DBTITLE 1,Eliminando vendas canceladas
data_merge = data_merge.where(data_merge['order_status'] == 'delivered')
data_merge.count()

# COMMAND ----------

# DBTITLE 1,Media de preços de frete por estado
display(data_merge.select('*').orderBy('customer_state', ascending=False, ))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "freight_value"
#Verificamos aqui que há muitos outiliers de valor maiores que 40, portanto para este caso especifico nos focaremos nos fretes com valores menores que 
display(data_merge.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Box-Plot da variável "freight_value" 
display(data_merge.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "freight_value"
res_estat = data_merge.select('freight_value').where(data_merge['freight_value'] >= 5)\
        .summary('count',
                'min',
                '25%',
                'mean',
                'stddev',
                '50%',
                '75%',
                '85%',
                '90%',
                '95%',
                '99%',
                'max')

#No intuito de eliminar os outiliers que neste modelo em especifico, são um dos maiores detratores dos melhores resultado de previsao do modelo
#foram selecionados 85% de toda a base de dados, que são os valores de frete entre R$ 5,00 e 26.72   

#nMin = res_estat.select(res_estat['freight_value']).where(res_estat['summary'] == 'min' ).collect()[0]['freight_value']
nMin = 5
n85 = res_estat.select(res_estat['freight_value']).where(res_estat['summary'] == '85%' ).collect()[0]['freight_value']
#nMaxM = res_estat.select(res_estat['freight_value']).where(res_estat['summary'] == 'max' ).collect()[0]['freight_value']

nMax = n85
#nMax = nMaxM

print('Valor minimo de frete definido: ')
print(nMin)
print('Valor máximo de frete definido: ')
print(nMax)
          
display(res_estat)      

# COMMAND ----------

# DBTITLE 1,Tratamento dos outliers no tocante ao valor do frete
data_merge_total = df

data_merge_total = data_merge_total.repartition(2).cache()
data_merge_total.count()

data_merge_Slice = df.where((df['freight_value'] >= nMin) & (df['freight_value'] <= nMax))

data_merge_Slice = data_merge_Slice.repartition(2).cache()
data_merge_Slice.count()

#Antes verifico a quantidade registros que serão descartados
print(data_merge_total.count() - data_merge_Slice.count())

df = data_merge_Slice


# COMMAND ----------

# DBTITLE 1,Histograma da variável "freight_value" na base sem outiliers
#Verificamos aqui que há muitos outiliers de valor maiores que 40, portanto para este caso especifico nos focaremos nos fretes com valores menores que 
display(data_merge_Slice.select('freight_value'))

# COMMAND ----------

display(data_merge_Slice.select('freight_value'))

# COMMAND ----------

display(data_merge_Slice.select('freight_value')\
        .summary('count',
                'min',
                '25%',
                'mean',
                'stddev',
                '50%',
                '75%',
                '85%',
                '90%',
                '95%',
                '99%',
                'max'))


# COMMAND ----------

# DBTITLE 1,Conversão do dataframe "df" para Pandas
pandas_df = data_merge_Slice.toPandas()

# COMMAND ----------

# DBTITLE 1,Heatmap da Correlação
# MAGIC %python
# MAGIC import seaborn as sns
# MAGIC import matplotlib.pyplot as plt
# MAGIC sns.set(rc={'figure.figsize':(20,20)})
# MAGIC 
# MAGIC #fig, ax = plt.figure()
# MAGIC sns.heatmap(pandas_df.corr(), xticklabels=pandas_df.corr().columns, yticklabels=pandas_df.corr().columns,cmap='RdBu_r',annot=True, cbar=False)
# MAGIC display()

# COMMAND ----------

# DBTITLE 1,Heatmap da Covariância
# MAGIC %python
# MAGIC import seaborn as sns
# MAGIC import matplotlib.pyplot as plt
# MAGIC sns.set(rc={'figure.figsize':(20,20)})
# MAGIC 
# MAGIC #fig, ax = plt.figure()
# MAGIC sns.heatmap(pandas_df.cov(), xticklabels=pandas_df.cov().columns, yticklabels=pandas_df.cov().columns,cmap='RdBu_r',annot=True, cbar=False)
# MAGIC display()

# COMMAND ----------

# DBTITLE 1,Tamanho da tabela ABT após limpeza dos outiliers
df.count()

# COMMAND ----------

# DBTITLE 1,Tamanho da tabela ABT com os outiliers
data_merge_total.count()

# COMMAND ----------

# DBTITLE 1,Importação da biblioteca para vetorização
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

# DBTITLE 1,Criação da coluna "features" no dataframe "df"
assembler    = VectorAssembler(inputCols = df.columns[:-1], outputCol  = 'features')
assembler_Tot = VectorAssembler(inputCols = data_merge_total.columns[:-1], outputCol  = 'features')

# COMMAND ----------

# DBTITLE 1,Transformação do dataframe "df" e criação do dataframe "df_assembler"
df_assembler = assembler.transform(df)
df_assembler_Tot = assembler_Tot.transform(data_merge_total)
display(df_assembler)

# COMMAND ----------

# DBTITLE 1,Qual a quantidade de linhas e colunas do dataframe "df_assembler"?
(df_assembler.count(), len(df_assembler.columns))
(df_assembler_Tot.count(), len(df_assembler_Tot.columns))

# COMMAND ----------

# DBTITLE 1,Divisão da base em treino e teste
treino, teste = df_assembler.randomSplit([0.7, 0.3], seed=36)
treino_Tot, teste_Tot = df_assembler_Tot.randomSplit([0.7, 0.3], seed=36)

treino = treino.repartition(2).cache()
treino.count()

teste = teste.repartition(2).cache()
teste.count()

treino_Tot = treino_Tot.repartition(2).cache()
treino_Tot.count()

teste_Tot = teste_Tot.repartition(2).cache()
teste_Tot.count()

# COMMAND ----------

# DBTITLE 1,Regressão Linear para o frete
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol= 'freight_value')

modelo = lr.fit(treino)
modelo_Tot = lr.fit(treino_Tot)

# COMMAND ----------

# DBTITLE 1,Qual os valores para o Coeficiente, pValor e Intercepto do modelo?
print(f'Coeficientes: {modelo.coefficients}')
print(f'pValues: {modelo.summary.pValues}')
print(f'Intercepto: {modelo.intercept}')

# COMMAND ----------

# DBTITLE 1,Qual o erro absoluto, erro quadrado, raiz quadrada do erro, r quadrado ajustado do modelo?
print(f'MAE: {modelo.summary.meanAbsoluteError}') #Erro absoluto do modelo
print(f'RMSE: {modelo.summary.rootMeanSquaredError}') #R2 ao quadrado error em valor
print(f'r2 Ajustado: {modelo.summary.r2adj *100}') #As variaveis que nos temos no modelo explicam 53.33% do modelo

'''
Base Line Metrics

MAE: 5.964145570151102
MSE: 119.27973085177
RMSE: 10.921526031272828
r2 Ajustado: 53.32880323011593'
'''

'''
Após a inclusão do codigo do CEP do vendedor.
Antes do corte de dados, dos outiliers do valor do frete

MAE: 5.742249298528482
MSE: 108.08284389752195
RMSE: 10.396289910228646
r2 Ajustado: 56.504091413487714
'''

maeLR_train  = modelo.summary.meanAbsoluteError
rmseLR_train = modelo.summary.rootMeanSquaredError
r2LR_train   = modelo.summary.r2adj

# COMMAND ----------

# DBTITLE 1,Testando o modelo
resultado_teste = modelo.evaluate(teste)
resultado_teste_Tot = modelo_Tot.evaluate(teste_Tot)

# COMMAND ----------

# DBTITLE 1,Avaliação do modelo
print(f'MAE: {resultado_teste.meanAbsoluteError}')
print(f'RMSE: {resultado_teste.rootMeanSquaredError}')
print(f'r2 Ajustado: {resultado_teste.r2adj}')

maeLR_teste  = resultado_teste.meanAbsoluteError
rmseLR_teste = resultado_teste.rootMeanSquaredError
r2LR_teste   = resultado_teste.r2adj

maeLR_teste_Tot  = resultado_teste_Tot.meanAbsoluteError
rmseLR_teste_Tot = resultado_teste_Tot.rootMeanSquaredError
r2LR_teste_Tot   = resultado_teste_Tot.r2adj

# COMMAND ----------

# DBTITLE 1,ÁRVORE DE REGRESSÃO


# COMMAND ----------

# DBTITLE 1,Modelo da Árvore de Regressão
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

featureIndexer = VectorIndexer(inputCol="features", 
                              outputCol="featureIndexer",
                              maxCategories=100).fit(df_assembler)

featureIndexer_Tot = VectorIndexer(inputCol="features", 
                              outputCol="featureIndexer",
                              maxCategories=100).fit(df_assembler_Tot)

dt = DecisionTreeRegressor(featuresCol="featureIndexer", labelCol="freight_value", maxBins=128)
#dt = DecisionTreeRegressor(featuresCol="featureIndexer", labelCol="freight_value", maxBins=50000)

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])
pipeline_Tot = Pipeline(stages=[featureIndexer_Tot, dt])

modelarvore = pipeline.fit(treino)
modelarvore_Tot = pipeline_Tot.fit(treino_Tot)

predictions = modelarvore.transform(teste)
predictions_Tot = modelarvore_Tot.transform(teste_Tot)

# COMMAND ----------

# DBTITLE 1,Exibição das colunas "prediction", "freight_value" e "features"
display(predictions.select("prediction", "freight_value", "features"))

# COMMAND ----------

# DBTITLE 1,Resultado do modelo "ÁRVORE DE REGRESSÃO"
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R2 = %g" % r2)

maeAR_teste  = mae
rmseAR_teste = rmse
r2AR_teste   = r2

treeModel = modelarvore.stages[1]
#Base cheia
#----------------------------------------------------------------------------
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_Tot)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions_Tot)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions_Tot)
print("R2 = %g" % r2)

maeAR_teste_Tot  = mae
rmseAR_teste_Tot = rmse
r2AR_teste_Tot   = r2

# COMMAND ----------

# DBTITLE 1,Visualização da Árvore de Regressão
display(treeModel)

# COMMAND ----------

# DBTITLE 1,GRADIENTE BOOST TREE REGRESSION


# COMMAND ----------

# DBTITLE 1,Modelo GRADIENTE BOOST TREE REGRESSION
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(df_assembler)

featureIndexer_Tot =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(df_assembler_Tot)

# Train a GBT model.
gbt = GBTRegressor(featuresCol="indexedFeatures", labelCol="freight_value",  maxIter=10)

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, gbt])
pipeline_Tot = Pipeline(stages=[featureIndexer_Tot, gbt])

# Train model.  This also runs the indexer.
modelgbt = pipeline.fit(treino)
modelgbt_Tot = pipeline_Tot.fit(treino_Tot)

predictions = modelgbt.transform(teste)
predictions_Tot = modelgbt_Tot.transform(teste_Tot)

# COMMAND ----------

# DBTITLE 1,Exibição das colunas "prediction", "freight_value" e "features"
display(predictions.select("prediction", "freight_value", "features"))

# COMMAND ----------

# DBTITLE 1,Resultados do modelo "GRADIENTE BOOST TREE REGRESSION"
# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R2 = %g" % r2)

maeGBT_teste  = mae
rmseGBT_teste = rmse
r2GBT_teste   = r2

gbtModel = modelgbt.stages[1]
print(gbtModel)  # summary only

#Base Cheia
#------------------------------------------------------------------------------
# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions_Tot)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_Tot)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions_Tot)
print("R2 = %g" % r2)

maeGBT_teste_Tot  = mae
rmseGBT_teste_Tot = rmse
r2GBT_teste_Tot   = r2


# COMMAND ----------

# DBTITLE 1,RANDOM FOREST REGRESSION


# COMMAND ----------

# DBTITLE 1,Modelo RANDOM FOREST REGRESSION
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(df_assembler)

featureIndexer_Tot =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(df_assembler_Tot)

# Train a RandomForest model.
rf = RandomForestRegressor(featuresCol="indexedFeatures", labelCol="freight_value")

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, rf])
pipeline_Tot = Pipeline(stages=[featureIndexer_Tot, rf])

# Train model.  This also runs the indexer.
modelrandom = pipeline.fit(treino)
modelrandom_Tot = pipeline_Tot.fit(treino_Tot)

# Make predictions.
predictions = modelrandom.transform(teste)
predictions_Tot = modelrandom_Tot.transform(teste_Tot)



# COMMAND ----------

# DBTITLE 1,Exibição das colunas "prediction", "freight_value" e "features"
display(predictions.select("prediction", "freight_value", "features"))

# COMMAND ----------

# DBTITLE 1,Resultado do modelo "RANDOM FOREST REGRESSION"
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R2 = %g" % r2)

maeRFR_teste  = mae
rmseRFR_teste = rmse
r2RFR_teste   = r2

rfModel = modelrandom.stages[1]
print(rfModel)  # summary only

#Base cheia
#----------------------------------------------------------------------------

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_Tot)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions_Tot)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions_Tot)
print("R2 = %g" % r2)

maeRFR_teste_Tot  = mae
rmseRFR_teste_Tot = rmse
r2RFR_teste_Tot   = r2


# COMMAND ----------

# DBTITLE 1,GeneralizedLinearRegression - Gaussian


# COMMAND ----------

# DBTITLE 1,Modelo GeneralizedLinearRegression - Gaussian
from pyspark.ml.regression import GeneralizedLinearRegression

glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3, labelCol="freight_value")

# Fit the model
modelgau = glr.fit(treino)
modelgau_Tot = glr.fit(treino_Tot)

# Print the coefficients and intercept for generalized linear regression model
print("Coefficients: " + str(modelgau.coefficients))
print("Intercept: " + str(modelgau.intercept))

# Summarize the model over the training set and print out some metrics
summary = modelgau.summary
print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
print("T Values: " + str(summary.tValues))
print("P Values: " + str(summary.pValues))
print("Dispersion: " + str(summary.dispersion))
print("Null Deviance: " + str(summary.nullDeviance))
print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
print("Deviance: " + str(summary.deviance))
print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
print("AIC: " + str(summary.aic))
print("Deviance Residuals: ")
summary.residuals().show()

# COMMAND ----------

# DBTITLE 1,Previsões
predictions = modelgau.transform(teste)
predictions_Tot = modelgau_Tot.transform(teste_Tot)

# COMMAND ----------

# DBTITLE 1,Exibição das colunas "prediction", "freight_value" e "features"
display(predictions.select("prediction", "freight_value", "features"))

# COMMAND ----------

# DBTITLE 1,Resultados do modelo "GeneralizedLinearRegression - Gaussian"
# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R2 = %g" % r2)

maeGLR_Gau_teste  = mae
rmseGLR_Gau_teste = rmse
r2GLR_Gau_teste   = r2

#Base cheia
#----------------------------------------------------------------------------

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_Tot)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions_Tot)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions_Tot)
print("R2 = %g" % r2)

maeGLR_Gau_teste_Tot  = mae
rmseGLR_Gau_teste_Tot = rmse
r2GLR_Gau_teste_Tot   = r2



# COMMAND ----------

# DBTITLE 1,COMPARATIVO DE RESULTADOS dos modelos quanto as métricas de testes


# COMMAND ----------

# DBTITLE 1,Qual é o melhor modelo?
columns = ("MODELO", "MAE", "MAE_FULL", "RMSE", "RMSE_FULL", "R2", "R2_FULL")
data    = ( ('LinearRegression'                   , maeLR_teste     , maeLR_teste_Tot     , rmseLR_teste      , rmseLR_teste_Tot      , r2LR_teste       * 100,   r2LR_teste_Tot       * 100),
           ('DecisionTreeRegressor'               , maeAR_teste     , maeAR_teste_Tot     , rmseAR_teste      , rmseAR_teste_Tot      , r2AR_teste       * 100,   r2AR_teste_Tot       * 100),
           ('GBTRegressor'                        , maeGBT_teste    , maeGBT_teste_Tot    , rmseGBT_teste     , rmseGBT_teste_Tot     , r2GBT_teste      * 100,   r2GBT_teste_Tot      * 100),
           ('RandomForestRegressor'               , maeRFR_teste    , maeRFR_teste_Tot    , rmseRFR_teste     , rmseRFR_teste_Tot     , r2RFR_teste      * 100,   r2RFR_teste_Tot      * 100),
           ('GeneralizedLinearRegression Gaussian', maeGLR_Gau_teste, maeGLR_Gau_teste_Tot, rmseGLR_Gau_teste , rmseGLR_Gau_teste_Tot , r2GLR_Gau_teste  * 100,   r2GLR_Gau_teste_Tot  * 100))

rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()
dfFromRDD1 = rdd.toDF(columns)

display(dfFromRDD1.orderBy('R2', ascending=False, ))

# COMMAND ----------

# DBTITLE 1,Resultado do modelo com nMin = 5
'''
MODELO	                                        MAE	             MAE_FULL	           RMSE	             RMSE_FULL	          R2	             R2_FULL
GBTRegressor	                        1.6829742177602103	4.473540942479851	2.434614975890428	9.219827125176893	71.62057516443838	66.80620295232022
RandomForestRegressor	                1.8997635232778034	4.865857056867012	2.5946979492735562	9.919719460971242	67.76581988618263	61.57532892387621
DecisionTreeRegressor	                1.90685251941861	5.050109013400841	2.652784875178676	10.186470369642779	66.30642631065373	59.48098951685474
LinearRegression	                    2.301002436385273	5.293480047119417	3.032605165346057	9.862082979772966	55.94360716156693	62.003370347318906
GeneralizedLinearRegression Gaussian	2.3068815598471657	5.267211130719013	3.0397542524028918	9.866085169380018	55.75950674222789	61.98971737533314

'''

# COMMAND ----------

# DBTITLE 1,Verificamos que com o GBT Regressor, obtivemos o melhor modelo no tocante a performance
#Sendo assim, incluo a feature product_category_name_Index para comparação dos resultados deste modelo em especifico, por se tratar do melhor modelo utilizado nesta analise
df = data_merge.select('product_category_name',
                       'distancia_sellers_customers',                     
                       'volume',               
                       'order_delivered_carrier_date',
                       'price', 
                       'payment_value',                
                       'product_weight_g', 
                       'codigo_UF_customers',
                       'codigo_UF_sellers',
                       'customer_zip_code_prefix',
                       'geolocation_lat_customer',
                       'geolocation_lng_customer',
                       'seller_zip_code_prefix',
                       'geolocation_lat_seller',
                       'geolocation_lng_seller',
                       'freight_value'
              )  

df = df.repartition(2).cache()
df.count()

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler
#para utilização do campo no modelo de regressao, faço uma tratativa nesta feature pois a mesma esta em formato string, sendo que o modelo exige variaveis numericas
indexer = StringIndexer().setInputCol("product_category_name").setOutputCol("product_category_name_Index").fit(df)
df = indexer.transform(df)

df = df.select('product_category_name_Index',
               'volume',               
               'order_delivered_carrier_date',
               'price', 
               'payment_value',                
               'product_weight_g', 
               'codigo_UF_customers',
               'codigo_UF_sellers',
               'customer_zip_code_prefix',
               'geolocation_lat_customer',
               'geolocation_lng_customer',
               'seller_zip_code_prefix',
               'geolocation_lat_seller',
               'geolocation_lng_seller',
               'freight_value'
              )  


df = df.repartition(2).cache()
df.count()


# COMMAND ----------

# DBTITLE 1,Tratamento dos outliers
data_merge_total = df

data_merge_total = data_merge_total.repartition(2).cache()
data_merge_total.count()

data_merge_Slice = data_merge_total.where((data_merge_total['freight_value'] > nMin) & (data_merge_total['freight_value'] < nMax))

data_merge_Slice = data_merge_Slice.repartition(2).cache()
data_merge_Slice.count()

#Antes verifico a quantidade registros que serão descartados
print(data_merge_total.count() - data_merge_Slice.count())

df = data_merge_Slice

# COMMAND ----------

# MAGIC %md Entendendo que mesmo utilizando apenas os dados até o 3-Quartil, ainda possuo o mesmo numero de categorias de produto na base

# COMMAND ----------

# DBTITLE 1,Tipos de categorias de produto na base inteira
df1 = (data_merge_total.select('*').groupBy('product_category_name_Index').count())
df1 = (df1.orderBy('count', ascending=False))
display(df1.select('*').limit(20))


# COMMAND ----------

dfC1  = (df1.select('*').groupBy('product_category_name_Index').count())
print(dfC1.select('count').count())

# COMMAND ----------

# DBTITLE 1,Tipos de categorias de produto na base sem os outiliers
df2 =(data_merge_Slice.select('*').where((data_merge_Slice['freight_value'] > 1) & (data_merge_Slice['freight_value'] < 34.13)).groupBy('product_category_name_Index').count())
df2 = (df2.orderBy('count', ascending=False))
display(df2.select('*').limit(20))

# COMMAND ----------

#Verificamos que fazem parte desta base, os mesmas categorias de produto da base total.
dfC2  = (df2.select('*').groupBy('product_category_name_Index').count())
print(dfC2.select('count').count())

# COMMAND ----------

assemblerGBT = VectorAssembler(inputCols = df.columns[:-1], outputCol = 'features')
assemblerGBT_tot = VectorAssembler(inputCols = data_merge_total.columns[:-1], outputCol = 'features')

assemblerGBT = assemblerGBT.transform(df)
assemblerGBT_Tot = assemblerGBT_tot.transform(data_merge_total)
#display(assemblerGBT)

# COMMAND ----------

treinoGBT, testeGBT = assemblerGBT .randomSplit([0.7, 0.3], seed=36)
treinoGBT_Tot, testeGBT_Tot = assemblerGBT_Tot .randomSplit([0.7, 0.3], seed=36)

treinoGBT = treinoGBT.repartition(2).cache()
treinoGBT.count()

testeGBT = testeGBT.repartition(2).cache()
testeGBT.count()

treinoGBT_Tot = treinoGBT_Tot.repartition(2).cache()
treinoGBT_Tot.count()

testeGBT_Tot = testeGBT_Tot.repartition(2).cache()
testeGBT_Tot.count()

# COMMAND ----------

# MAGIC %md Após a inclusão da feature Product_Category_Name, faremos outra analise utilizando o modelo que melhor performou. 

# COMMAND ----------

# DBTITLE 1,Novo teste com GRADIENTE BOOST TREE REGRESSION - Product_Category_Name
#GRADIENTE BOOST TREE REGRESSION

featureIndexerGBT =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(assemblerGBT)

featureIndexerGBT_Tot =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=100).fit(assemblerGBT_Tot)

# Train a GBT model.
#Setada a variavel maxBins para 74, pois possuimos 74 categorias de produto e o GBTR aceita por padrao 32
gbtPCN = GBTRegressor(featuresCol="indexedFeatures", labelCol="freight_value",  maxIter=10, maxBins=74)

# Chain indexer and GBT in a Pipeline
pipelineGBT = Pipeline(stages=[featureIndexerGBT, gbtPCN])
pipelineGBT_Tot = Pipeline(stages=[featureIndexerGBT_Tot, gbtPCN])

# Train model.  This also runs the indexer.
modelgbtPCA = pipelineGBT.fit(treinoGBT)
modelgbtPCA_Tot = pipelineGBT_Tot.fit(treinoGBT_Tot)

predictionsGBT = modelgbtPCA.transform(testeGBT)
predictionsGBT_Tot = modelgbtPCA.transform(testeGBT_Tot)

predictionsGBT.select("prediction", "freight_value", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictionsGBT)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictionsGBT)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictionsGBT)
print("R2 = %g" % r2)

maeGBT_PCA_teste  = mae
rmseGBT_PCA_teste = rmse
r2GBT_PCA_teste   = r2

gbtModel = modelgbtPCA.stages[1]
print(gbtModel)  # summary only

#Base cheia
#----------------------------------------------------------------------------

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictionsGBT_Tot)
print("MAE = %g" % mae)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictionsGBT_Tot)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator = RegressionEvaluator(
    labelCol="freight_value", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictionsGBT_Tot)
print("R2 = %g" % r2)

maeGBT_PCA_teste_Tot  = mae
rmseGBT_PCA_teste_Tot = rmse
r2GBT_PCA_teste_Tot   = r2

# COMMAND ----------

# DBTITLE 1,Comparação dos modelos GBT Regressor
columns = ("MODELO", "MAE", "MAE_FULL", "RMSE", "RMSE_FULL", "R2", "R2_FULL")
data    = (('GBTRegressor'                        , maeGBT_teste    , maeGBT_teste_Tot    , rmseGBT_teste     , rmseGBT_teste_Tot     , r2GBT_teste      * 100,   r2GBT_teste_Tot      * 100),
           ('GBTRegressor Product_Category_Name'  , maeGBT_PCA_teste, maeGBT_PCA_teste_Tot, rmseGBT_PCA_teste , rmseGBT_PCA_teste_Tot , r2GBT_PCA_teste  * 100,  r2GBT_PCA_teste_Tot  * 100))


rdd2 = spark.sparkContext.parallelize(data)
dfFromRDD2 = rdd2.toDF()
dfFromRDD2 = rdd2.toDF(columns)

display(dfFromRDD2.orderBy('R2', ascending=False ))

# COMMAND ----------

# DBTITLE 1,Resultado do modelo com nMin = 5
'''
MODELO	                                   MAE	             MAE_FULL	           RMSE	             RMSE_FULL	          R2	             R2_FULL						
GBTRegressor	                    1.6829742177602103	4.473540942479851	2.434614975890428	9.219827125176893	71.62057516443838	66.80620295232022
GBTRegressor Product_Category_Name	1.737223408500116	5.419682592124251	2.5606264366004683	14.23506543295993	68.65254958422193	16.805421774143237
'''


# COMMAND ----------

# MAGIC %md
# MAGIC Reparamos que o novo modelo GBTRegressor com a feature Product_Category_Name, nao performou melhor, levando tambem em consideração a metrica R2 FULL(Compoe a base com os outiliers).

# COMMAND ----------

# MAGIC %md FIM DO NOTEBOOK
