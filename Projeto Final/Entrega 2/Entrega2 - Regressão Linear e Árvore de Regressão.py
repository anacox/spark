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

# DBTITLE 1,Tratamento das variáveis nas variáveis data
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

# DBTITLE 1,Quantidade de missing na base "order_review"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_review.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_review.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento das variáveis categóricas da base "order_review"
order_review = order_review.fillna('NA', subset=['review_id'])
order_review = order_review.fillna('NA', subset=['order_id'])
order_review = order_review.fillna('NA', subset=['review_comment_title'])
order_review = order_review.fillna('NA', subset=['review_comment_message'])
order_review = order_review.fillna('NA', subset=['review_creation_date'])
order_review = order_review.fillna('NA', subset=['review_answer_timestamp'])

# COMMAND ----------

# DBTITLE 1,Remoção das linhas que tem NA na base "order_review"
order_review = order_review.dropna()
order_review.count()

# COMMAND ----------

# DBTITLE 1,Verificação da quantidade de missing na base "order_review"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_review.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_review.select(aux))

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

# DBTITLE 1,Criação da base "tabelaUF"
#Com estes dados eu crio o codigo do estado para melhorar a acuracia de nosso modelo, podendo utilizar assim o estado como feature na regressao linear
tabelaUF = spark.read.csv('/FileStore/tables/TabelaUF.csv', header = True, sep = ';', inferSchema=True)
tabelaUF = tabelaUF.withColumnRenamed("Código UF", "codigo_UF")
tabelaUF = tabelaUF.withColumnRenamed("Unidade da Federação", "desc_estado")
tabelaUF = tabelaUF.withColumnRenamed("UF", "customer_state")
display(tabelaUF)

# COMMAND ----------

# DBTITLE 1,União (left join) das bases no dataframe "data_merge"
#União das bases "orders" com "order_items" utilizando a coluna "order_id".
data_merge = order_items.join(orders, "order_id",how='left')

#União das bases "data_merge" com "order_items" utilizando a coluna "order_id".
data_merge = data_merge.join(order_payments, "order_id",how='left')

#União das bases "data_merge" com "products" utilizando a coluna "product_id".
data_merge = data_merge.join(products, "product_id",how='left')

#União das bases "data_merge" com "customers" utilizando a coluna "customer_id".
data_merge = data_merge.join(customers, "customer_id",how='left')

#União das bases "data_merge" com "Tabelas UF" utilizando a coluna "UF".
data_merge = data_merge.join(tabelaUF, "customer_state" ,how='left')


display(data_merge)

# COMMAND ----------

# DBTITLE 1,Quais os tipos de dados que o dataframe "data_merge" possui?
data_merge.dtypes

# COMMAND ----------

# DBTITLE 1,Quantas linhas possui o dataframe "data_merge"?
data_merge.count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico do dataframe "data_merge"
display(data_merge.select('price', 
                          'payment_sequential', 
                          'payment_installments', 
                          'payment_value', 
                          'product_name_lenght', 
                          'product_description_lenght',
                          'product_photos_qty', 
                          'product_weight_g', 
                          'product_length_cm', 
                          'product_height_cm', 
                          'product_width_cm', 
                          'customer_zip_code_prefix', 
                          'freight_value')\
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

# DBTITLE 1,Quantidade de missing do dataframe "data_merge"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in data_merge.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(data_merge.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missings nas variáveis quantitativas do dataframe "data_merge" 
installments = data_merge[['payment_installments']].agg({'payment_installments': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(lenght, subset=['payment_installments'])

valor_pagamentos = data_merge[['payment_value']].agg({'payment_value': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(lenght, subset=['payment_value'])

lenght = data_merge[['product_name_lenght']].agg({'product_name_lenght': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(lenght, subset=['product_name_lenght'])

descrip = data_merge[['product_description_lenght']].agg({'product_description_lenght': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(descrip, subset=['product_description_lenght'])

photo = data_merge[['product_photos_qty']].agg({'product_photos_qty': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(photo, subset=['product_photos_qty'])

weight = data_merge[['product_weight_g']].agg({'product_weight_g': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(weight, subset=['product_weight_g'])

lenght_cm = data_merge[['product_length_cm']].agg({'product_length_cm': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(lenght_cm, subset=['product_length_cm'])

height = data_merge[['product_height_cm']].agg({'product_height_cm': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(height, subset=['product_height_cm'])

width = data_merge[['product_width_cm']].agg({'product_width_cm': 'mean'}).collect()[0][0]
data_merge = data_merge.fillna(width, subset=['product_width_cm'])

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

# DBTITLE 1,Remoção das linhas que tem NA na base "data_merge" 
data_merge = data_merge.dropna()
data_merge.count()

# COMMAND ----------

# DBTITLE 1,Verificação da quantidade de missing do dataframe "data_merge"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in data_merge.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(data_merge.select(aux))

# COMMAND ----------

# DBTITLE 1,Quantidade de linhas após tratamento de missings
#Das 117604 linhas presentes após o merge, restaram 115018 linhas após tratamento de valores nulos. Desta forma, foram eliminadas 2586 linhas.
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

# DBTITLE 1,Criação do dataframe "df", que contém apenas variáveis numéricas
'''df = data_merge.select('price', 
                       'payment_sequential', 
                       'payment_installments', 
                       'payment_value', 
                       'product_name_lenght', 
                       'product_description_lenght',
                       'product_photos_qty', 
                       'product_weight_g', 
                       'product_length_cm', 
                       'product_height_cm', 
                       'product_width_cm', 
                       'customer_zip_code_prefix', 
                       'codigo_UF',
                       'freight_value')

display(df)
'''

df = data_merge.select('order_approved_at',
                       'order_delivered_carrier_date',
                       'product_id',
                       'product_category_name',
                       'price', 
                       'payment_value', 
                       'product_photos_qty', 
                       'product_weight_g', 
                       'product_length_cm', 
                       'product_height_cm', 
                       'product_width_cm', 
                       'customer_zip_code_prefix', 
                       'codigo_UF',
                       'freight_value')


from pyspark.sql.functions import unix_timestamp
df = df.withColumn('order_approved_at', unix_timestamp('order_approved_at'))
df = df.withColumn('order_delivered_carrier_date', unix_timestamp('order_delivered_carrier_date'))

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler

#para utilização do campo codigo do produto em no modelo de regressao, faço uma tratativa nesta feature pois a mesma esta em formato string, sendo que o modelo exige variaveis numericas
varIdxer = StringIndexer(inputCol='product_id',outputCol='product_id_Indx').fit(df)
df = varIdxer.transform(df)

#para utilização do campo product_category_name em no modelo de regressao, faço uma tratativa nesta feature pois a mesma esta em formato string, sendo que o modelo exige variaveis numericas
varIdxer = StringIndexer(inputCol='product_category_name',outputCol='product_category_name_Indx').fit(df)
df = varIdxer.transform(df)


  
df = df.select('order_approved_at',
                       'order_delivered_carrier_date',
                       'price', 
                       'payment_value', 
                       'product_photos_qty', 
                       'product_weight_g', 
                       'product_length_cm', 
                       'product_height_cm', 
                       'product_width_cm', 
                       'customer_zip_code_prefix', 
                       'codigo_UF',
                       'product_id_Indx',
                       'product_category_name_Indx',
                       'freight_value')  

display(df)

# COMMAND ----------

#excluo as linhas duplicadas
df = df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Importação da biblioteca para vetorização
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

# DBTITLE 1,Criação da coluna "features" no dataframe "df"
assembler = VectorAssembler(inputCols = df.columns[:-1], outputCol  = 'features')

# COMMAND ----------

# DBTITLE 1,Transformação do dataframe "df" e criação do dataframe "df_assembler"
df_assembler = assembler.transform(df)
display(df_assembler)

# COMMAND ----------

# DBTITLE 1,Qual a quantidade de linhas e colunas do dataframe "df_assembler"?
(df_assembler.count(), len(df_assembler.columns))

# COMMAND ----------

# DBTITLE 1,Divisão da base em treino e teste
treino, teste = df_assembler.randomSplit([0.8, 0.2], seed=36)

# COMMAND ----------

# DBTITLE 1,Importação da biblioteca para Regressão Linear
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

# DBTITLE 1,Regressão linear para o frete
lr = LinearRegression(labelCol= 'freight_value')

# COMMAND ----------

# DBTITLE 1,Treino do modelo de treino
modelo = lr.fit(treino)

# COMMAND ----------

# DBTITLE 1,Qual os valores para o Coeficiente, pValor e Intercepto do modelo?
print(f'Coeficientes: {modelo.coefficients}')
print(f'pValues: {modelo.summary.pValues}')
print(f'Intercepto: {modelo.intercept}')

# COMMAND ----------

# DBTITLE 1,Qual o erro absoluto, erro quadrado, raiz quadrada do erro, r quadrado ajustado do modelo?
print(f'MAE: {modelo.summary.meanAbsoluteError}') #Erro absoluto do modelo
print(f'MSE: {modelo.summary.meanSquaredError}')
print(f'RMSE: {modelo.summary.rootMeanSquaredError}') #R2 ao quadrado error em valor
print(f'r2 Ajustado: {modelo.summary.r2adj *100}') #As variaveis que nos temos no modelo explicam 53.36% do modelo

# COMMAND ----------

# DBTITLE 1,Testando o modelo
resultado_teste = modelo.evaluate(teste)

# COMMAND ----------

# DBTITLE 1,Avaliação do modelo
print(f'MAE: {resultado_teste.meanAbsoluteError}')
print(f'MSE: {resultado_teste.meanSquaredError}')
print(f'RMSE: {resultado_teste.rootMeanSquaredError}')
print(f'r2 Ajustado: {resultado_teste.r2adj}')

# COMMAND ----------

display(df_assembler)

# COMMAND ----------

# DBTITLE 1,ÁRVORE DE DECISÃO


# COMMAND ----------

# DBTITLE 1,Importação das bibliotecas para Árvore de Regressão
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# DBTITLE 1,Criação da coluna "indexedFeatures" no dataframe "featureIndexer"
indexer = VectorIndexer(inputCol="features", \
                              outputCol="featureIndexer",\
                              maxCategories=10)


# COMMAND ----------

featureIndexer = indexer.fit(df_assembler)

# COMMAND ----------

indexedData = featureIndexer.transform(df_assembler)
indexedData.show()

# COMMAND ----------

df_assembler.columns

# COMMAND ----------

# DBTITLE 1,Divisão da base em treino e teste
(trainingData, testData) = df_assembler.randomSplit([0.8, 0.2])

# COMMAND ----------

# DBTITLE 1,Chain indexador e a Árvore de Regressão no "pipeline"
# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])

# COMMAND ----------

# DBTITLE 1,Treinando o modelo da Árvore de Regressão
dt = DecisionTreeRegressor(featuresCol="featureIndexer")

# COMMAND ----------

# DBTITLE 1,Treinando o modelo e rodando o indexador
model = pipeline.fit(trainingData)

# COMMAND ----------

# DBTITLE 1,Predições
predictions = model.transform(testData)

# COMMAND ----------

# DBTITLE 1,Exibição das colunas "prediction", "label" e "features"
predictions.select("prediction", "label", "features").show()

# COMMAND ----------

# DBTITLE 1,Seleção das colunas "prediction" e "true label" e cálculo do erro
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

treeModel = model.stages[1]

# COMMAND ----------

# DBTITLE 1,Visualização da Árvore de Regressão
print(treeModel)

# COMMAND ----------

# DBTITLE 1,PRÓXIMOS PASSOS
# MAGIC %md
# MAGIC REGRESSÃO LINEAR --> Testaremos segmentar o frete por cidade para melhorar o resultado.
# MAGIC 
# MAGIC 
# MAGIC ÁRVORE DE REGRESSÃO --> Entender o erro "java.lang.IllegalArgumentException: Field &#34;label&#34; does not exist."
