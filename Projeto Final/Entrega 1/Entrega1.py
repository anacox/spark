# Databricks notebook source
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

# DBTITLE 1,Quais os tipos de variáveis da base "Closed Deals"?
closed_deals.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Closed Deals" antes do tratamento
display(closed_deals.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "Closed Deals"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in closed_deals.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(closed_deals.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missing nas variáveis categóricas
closed_deals = closed_deals.fillna('NA', subset=['business_segment'])
closed_deals = closed_deals.fillna('NA', subset=['lead_type'])
closed_deals = closed_deals.fillna('NA', subset=['lead_behaviour_profile'])
closed_deals = closed_deals.fillna('NA', subset=['has_company'])
closed_deals = closed_deals.fillna('NA', subset=['has_gtin'])
closed_deals = closed_deals.fillna('NA', subset=['average_stock'])
closed_deals = closed_deals.fillna('NA', subset=['business_type'])

# COMMAND ----------

# DBTITLE 1,Remover as linhas que contém NA
closed_deals = closed_deals.dropna()
closed_deals.count()

# COMMAND ----------

# DBTITLE 1,Tratamento de missing das variáveis quantitativas
tamanho_declarado = closed_deals[['declared_product_catalog_size']].agg({'declared_product_catalog_size': 'mean'}).collect()[0][0]
closed_deals = closed_deals.fillna(tamanho_declarado, subset=['declared_product_catalog_size'])

# COMMAND ----------

# DBTITLE 1,Verificação do missing após tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in closed_deals.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(closed_deals.select(aux))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Closed Deals" depois do tratamento
display(closed_deals.describe())

# COMMAND ----------

# DBTITLE 1,Nesta base ha 842 mql_ids diferentes...
closed_deals.select('mql_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e eles aparecem apenas 1 unica vez.
closed_deals.select('mql_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "mql_id"
closed_deals.groupby('mql_id').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 842 vendedores distintos na base...
closed_deals.select('seller_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e nenhum deles se repete na base.
closed_deals.select('seller_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência para a variável qualitativa "seller_id"
closed_deals.groupby('seller_id').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 32 sdr_ids distintos na base...
closed_deals.select('sdr_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...entretanto eles se repetem.
closed_deals.select('sdr_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "sdr_id"
closed_deals.groupby('sdr_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base existem 22 sr_ids...
closed_deals.select('sr_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...que aparecem mais de uma vez. 
closed_deals.select('sr_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "sr_id"
closed_deals.groupby('sr_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base há 824 registros na variável "won_date"...
closed_deals.select('won_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e percebe-se que mais de uma venda em um mesmo timestamp.
closed_deals.select('won_date').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "won_date"
closed_deals.groupby('won_date').count().show()

# COMMAND ----------

# DBTITLE 1,Na base há 34 segmentos de negócio distintos...
closed_deals.select('business_segment').distinct().count()

# COMMAND ----------

# DBTITLE 1,...mas que se repetem.
closed_deals.select('business_segment').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequencia da variável qualitativa "business_segment"
closed_deals.groupby('business_segment').count().show()

# COMMAND ----------

# DBTITLE 1,Há 9 tipos de lead...
closed_deals.select('lead_type').distinct().count()

# COMMAND ----------

# DBTITLE 1,...que aparecem diversas vezes na base.
closed_deals.select('lead_type').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "lead_type"
closed_deals.groupby('lead_type').count().show()

#É possivel perceber que o tipo de lead "online_medium" é o que mais possui registros, seguido do "online_big".

# COMMAND ----------

# DBTITLE 1,A base segmenta o comportamento dos leads em 10 tipos.
closed_deals.select('lead_behaviour_profile').distinct().count()

# COMMAND ----------

# DBTITLE 1,...que se repetem nas 842 linhas. 
closed_deals.select('lead_behaviour_profile').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "lead_behaviour_profile"
closed_deals.groupby('lead_behaviour_profile').count().show()

#O perfil "cat" aparece 407 vezes, o que representa 48,3% da base. Nessa base há 177 nulos. 

# COMMAND ----------

# DBTITLE 1,A variável "has_company" é dividida em true, false e null 
closed_deals.select('has_company').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "has_company"
closed_deals.groupby('has_company').count().show()

#Há muito nulo nessa coluna!

# COMMAND ----------

# DBTITLE 1,A variável "has_gtin" é dividida em true, false e null 
closed_deals.select('has_company').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela da frequência da variável qualitativa "has_gtin"
closed_deals.groupby('has_gtin').count().show()

# COMMAND ----------

# DBTITLE 1,A variável "average_stock" possui 7 possibilidades...
closed_deals.select('average_stock').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "average_stock"
closed_deals.groupby('average_stock').count().show()

# COMMAND ----------

# DBTITLE 1,A variável "business_type" apresenta 4 tipos de negócios.
closed_deals.select('business_type').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "business_type"
closed_deals.groupby('business_type').count().show()

#É possível observar que mais da metada dos tipos de negócios são de "reseller", seguido de "manufacturer".

# COMMAND ----------

# DBTITLE 1,Na base, existem 34 tamanhos de produtos catalogados.
closed_deals.select('declared_product_catalog_size').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável qualitativa "declared_product_catalog_size"
closed_deals.groupby('declared_product_catalog_size').count().show()

#Há muito nulo!

# COMMAND ----------

# DBTITLE 1,A variável "declared_monthly_revenue" possui 34 possibidades.
closed_deals.select('declared_monthly_revenue').distinct().count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "declared_monthly_revenue"
display(closed_deals.describe('declared_monthly_revenue'))

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "declared_monthly_revenue"
closed_deals.groupby('declared_monthly_revenue').count().show()

# COMMAND ----------

# DBTITLE 1,Olhando agora para a tabela de "Customers", quais os tipos de variáveis?
customers.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Customers"
display(customers.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "Customers"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in customers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(customers.select(aux))

# COMMAND ----------

# DBTITLE 1,A base possui 99441 clientes diferentes...
customers.select('customer_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e não há nenhum registro repetido.
customers.select('customer_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequencia da variável qualitativa "customer_id"
customers.groupby('customer_id').count().show()

# COMMAND ----------

# DBTITLE 1,Já a variável "customer_unique_id" tem 96096 registros distintos...
customers.select('customer_unique_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...em um total de 99441 registros.
customers.select('customer_unique_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "customer_unique_id"
customers.groupby('customer_unique_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base existem 14994 registros distintos na variável "customer_zip_code_prefix"
customers.select('customer_zip_code_prefix').distinct().count()

# COMMAND ----------

# DBTITLE 1,É possível perceber que há mais de um cliente por CEP.
customers.select('customer_zip_code_prefix').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "customer_zip_code_prefix"
customers.groupby('customer_zip_code_prefix').count().show()

# COMMAND ----------

# DBTITLE 1,Há 4119 cidades na base...
customers.select('customer_city').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "customer_city"
customers.groupby('customer_city').count().show()

# COMMAND ----------

# DBTITLE 1,Quais cidades possuem mais consumidores?
consumidor_cidade = customers.groupby('customer_city').count()
display(consumidor_cidade.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,A base possui registros de 27 estados distintos...
customers.select('customer_state').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "customer_state"
customers.groupby('customer_state').count().show()

# COMMAND ----------

# DBTITLE 1,Quais estados possuem mais consumidores?
consumidores_estados = customers.groupby('customer_state').count()
display(consumidores_estados.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Analisando agora a base "Marketing", quais os tipos de variáveis? 
marketing.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Marketing"
display(marketing.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "Marketing"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in marketing.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(marketing.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento do missing na variável "origin"
marketing = marketing.fillna('NA', subset=['origin'])

# COMMAND ----------

# DBTITLE 1,Remover as linhas que contém NA
marketing = marketing.dropna()
marketing.count()

# COMMAND ----------

# DBTITLE 1,Verificação do missing após tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in marketing.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(marketing.select(aux))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Marketing" após tratamento do missing
display(marketing.describe())

# COMMAND ----------

# DBTITLE 1,A variável "mql_id" possui 8000 registros...
marketing.select('mql_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e não possui repetição.
marketing.select('mql_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "mql_id"
marketing.groupby('mql_id').count().show()

# COMMAND ----------

# DBTITLE 1,A base apresenta registros distintos em 336 dias...
marketing.select('first_contact_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e nenhum timestamp se repete.
marketing.select('first_contact_date').count()

# COMMAND ----------

# DBTITLE 1,Tabela da frequência da variável "first_contact_date"
marketing.groupby('first_contact_date').count().show()

# COMMAND ----------

# DBTITLE 1,Qual dia houve mais primeiros contatos?
primeiro_contato = marketing.groupby('first_contact_date').count()
display(primeiro_contato.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Na base existe 495 IDs de landing page distintos...
marketing.select('landing_page_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "landing_page_id"
marketing.groupby('landing_page_id').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o ID da landing page mais acessada? 
landing = marketing.groupby('landing_page_id').count()
display(landing.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 11 origens de marketing possíveis...
marketing.select('origin').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "origin"
marketing.groupby('origin').count().show()

# COMMAND ----------

# DBTITLE 1,Qual a maior origem dos acessos? 
origem = marketing.groupby('origin').count()
display(origem.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Na base "order_items", quais os tipos das variáveis?
order_items.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "order_items"
display(order_items.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing da base "order_items"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_items.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_items.select(aux))

# COMMAND ----------

# DBTITLE 1,A variável "order_id" possui 98666 registros distintos...
order_items.select('order_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e é possível notar que há repetição na base.
order_items.select('order_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_id"
order_items.groupby('order_id').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o order_id que mais se repete?
order_id_rep = order_items.groupby('order_id').count()
display(order_id_rep.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 21 tipos na variável "order_item_id"
order_items.select('order_item_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_item_id"
order_items.groupby('order_item_id').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o order_item_id que mais se repete?
order_item_id_rep = order_items.groupby('order_item_id').count()
display(order_item_id_rep.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Existem 32951 product_id distintos.
order_items.select('product_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_id"
order_items.groupby('product_id').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o ID do produto que mais repete?
product_id_rep = order_items.groupby('product_id').count()
display(product_id_rep.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,A variável "seller_id" tem 3095 registros distintos na base...
order_items.select('seller_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e é possível perceber que há repetições. 
order_items.select('seller_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "seller_id"
order_items.groupby('seller_id').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o ID do vendedor que mais se repete?
seller_id_rep = order_items.groupby('seller_id').count()
display(seller_id_rep.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Existem 93318 timestamps diferentes na variável "shipping_limit_date"...
order_items.select('shipping_limit_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e há mais de uma linha com o mesmo timestamp.
order_items.select('shipping_limit_date').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "shipping_limit_date"
order_items.groupby('shipping_limit_date').count().show()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável quantitativa "price"
display(order_items.describe('price'))

# COMMAND ----------

# DBTITLE 1,Quartis da variável "price"
order_items.approxQuantile("price", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "price"
display(order_items.select('price'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "price"
display(order_items.select('price'))

# COMMAND ----------

# DBTITLE 1,Qual o preço mais comum?
price_rep = order_items.groupby('price').count()
display(price_rep.orderBy('count', ascending=False))

#R$59,90 é o preço que mais se repete

# COMMAND ----------

# DBTITLE 1,Existem 6999 registros diferentes na variável "freight_value"
order_items.select('freight_value').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "freight_value"
order_items.groupby('freight_value').count().show()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "freight_value"
display(order_items.describe('freight_value'))

# COMMAND ----------

# DBTITLE 1,Quartis da variável "freight_value"
order_items.approxQuantile("freight_value", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot "freight_value"
display(order_items.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "freight_value"
display(order_items.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Qual o valor de frete mais comum?
frete = order_items.groupby('freight_value').count()
display(frete.orderBy('count', ascending=False))

#R$15,10 é o frete mais comum.

# COMMAND ----------

# DBTITLE 1,Explorando a base "order_payments", quais os tipos de variáveis?
order_payments.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "order_payments"
display(order_payments.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing da base "order_payments"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_payments.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_payments.select(aux))

# COMMAND ----------

# DBTITLE 1,Nesta base há 99440 tipos de "order_id" distintos...
order_payments.select('order_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e um total de 103886 registros.
order_payments.select('order_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_id"
order_payments.groupby('order_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base há 29 "payment_sequential" distintos
order_payments.select('payment_sequential').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "payment_sequential"
order_payments.groupby('payment_sequential').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 5 tipos de pagamentos.
order_payments.select('payment_type').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "payment_type"
order_payments.groupby('payment_type').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o método de pagamento mais utilizado?
metodo_pagamento = order_payments.groupby('payment_type').count()
display(metodo_pagamento.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 24 tipos de installments na base
order_payments.select('payment_installments').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "payment_installments"
order_payments.groupby('payment_installments').count().show()

# COMMAND ----------

# DBTITLE 1,Qual é a quantidade de parcelas mais comum?
parcelas = order_payments.groupby('payment_installments').count()
display(parcelas.orderBy('count', ascending=False))

#O pagamento a vista é o mais escolhido

# COMMAND ----------

# DBTITLE 1,Há 29077 registros para a variável "payment_value"
order_payments.select('payment_value').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "payment_value"
order_payments.groupby('payment_value').count().show()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "payment_value"
display(order_payments.describe('payment_value'))

# COMMAND ----------

# DBTITLE 1,Quartis da variável "payment_value"
order_payments.approxQuantile("payment_value", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "payment_value"
display(order_items.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "payment_value"
display(order_items.select('freight_value'))

# COMMAND ----------

# DBTITLE 1,Qual o valor de pagamento mais frequente?
valor_pagamento = order_payments.groupby('payment_value').count()
display(valor_pagamento.orderBy('count', ascending=False))

#R$50,00 é o valor mais comum nesta base.

# COMMAND ----------

# DBTITLE 1,Explorando a base "order_review", quais tipos de variáveis?
order_review.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "order_review" antes do tratamento de missings
display(order_review.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missings na base "order_review"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_review.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_review.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missings nas variáveis categóricas
order_review = order_review.fillna('NA', subset=['review_id'])
order_review = order_review.fillna('NA', subset=['order_id'])
order_review = order_review.fillna('NA', subset=['review_comment_title'])
order_review = order_review.fillna('NA', subset=['review_comment_message'])
order_review = order_review.fillna('NA', subset=['review_creation_date'])
order_review = order_review.fillna('NA', subset=['review_answer_timestamp'])

# COMMAND ----------

# DBTITLE 1,Remover as linhas que tem NA
order_review = order_review.dropna()
order_review.count()

# COMMAND ----------

# DBTITLE 1,Verificação do missing após tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in order_review.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(order_review.select(aux))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "order_review" após tratamento de missings
display(order_review.describe())

# COMMAND ----------

# DBTITLE 1,A base "order_review" possui 101541 comentários...
order_review.select('review_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...de um total de 102692. Ou seja, existem comentários repetidos.
order_review.select('review_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_id"
order_review.groupby('review_id').count().show()

# COMMAND ----------

# DBTITLE 1,Quais são os reviews que mais apresentam repetição?
review = order_review.groupby('review_id').count()
display(review.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Visualizando de forma gráfica a variável "review_id"
review = order_review.groupby('review_id').count()
display(review.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Existem 100402 pedidos distintos registrados na base...
order_review.select('order_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_id"
order_review.groupby('order_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base existe 2567 registros diferentes de pontuação do comentário...
order_review.select('review_score').distinct().count()

# COMMAND ----------

# DBTITLE 1,...de um total de 102692 comentários.
order_review.select('review_score').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_score"
order_review.groupby('review_score').count().show()

# COMMAND ----------

# DBTITLE 1,Qual as maiores notas recebidas?
pontuacao = order_review.groupby('review_score').count()
display(pontuacao.orderBy('count', ascending=False))

# É necessário um tratamento dessa variável

# COMMAND ----------

# DBTITLE 1,Visualização gráfica da variável "review_score"
pontuacao = order_review.groupby('review_score').count()
display(pontuacao.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Existem 5044 títulos diferentes...
order_review.select('review_comment_title').distinct().count()

# COMMAND ----------

# DBTITLE 1,...de um total de 102692 registros. Ou seja, há títulos repetidos.
order_review.select('review_comment_title').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_comment_title"
order_review.groupby('review_comment_title').count().show()

# COMMAND ----------

# DBTITLE 1,Quais os títulos mais comuns?
titulo = order_review.groupby('review_comment_title').count()
display(titulo.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Na base há 36878 mensagens diferentes.
order_review.select('review_comment_message').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_comment_message"
order_review.groupby('review_comment_message').count().show()

# COMMAND ----------

# DBTITLE 1,Quais as mensgens mais comuns?
mensagens = order_review.groupby('review_comment_message').count()
display(mensagens.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,A base abrange 726 datas diferentes de criação de comentários.
order_review.select('review_creation_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_creation_date"
order_review.groupby('review_creation_date').count().show()

# COMMAND ----------

# DBTITLE 1,Qual a data que mais foram criados reviews?
data = order_review.groupby('review_creation_date').count()
display(data.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 95073 timestamps diferentes na base.
order_review.select('review_answer_timestamp').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "review_answer_timestamp"
order_review.groupby('review_answer_timestamp').count().show()

# COMMAND ----------

# DBTITLE 1,Qual dia que houveram mais respostas para os comentários?
data_resp = order_review.groupby('review_answer_timestamp').count()
display(data_resp.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Analisando a base "orders", quais os tipos de variáveis?
orders.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "orders" antes do tratamento de missings
display(orders.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "Orders"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in orders.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(orders.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missing nas variáveis categóricas
orders = orders.fillna('NA', subset=['order_approved_at'])
orders = orders.fillna('NA', subset=['order_delivered_carrier_date'])
orders = orders.fillna('NA', subset=['order_delivered_customer_date'])

# COMMAND ----------

# DBTITLE 1,Remover as linhas que contém NA
orders = orders.dropna()
orders.count()

# COMMAND ----------

# DBTITLE 1,Verificação do missing após tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in orders.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(orders.select(aux))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Orders" depois do tratamento
display(orders.describe())

# COMMAND ----------

# DBTITLE 1,A variável "order_id" possui 99441 registros distintos...
orders.select('order_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e os ID são únicos na base.
orders.select('order_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_id"
orders.groupby('order_id').count().show()

# COMMAND ----------

# DBTITLE 1,Na base existem xx ID de clientes...
orders.select('customer_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e os registros são únicos.
orders.select('customer_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "customer_id"
orders.groupby('customer_id').count().show()

# COMMAND ----------

# DBTITLE 1,Há 8 tipos de status de pedidos.
orders.select('order_status').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_status"
orders.groupby('order_status').count().show()

# COMMAND ----------

# DBTITLE 1,Há 98875 registros de compra em timestamps diferentes.
orders.select('order_purchase_timestamp').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_purchase_timestamp"
orders.groupby('order_purchase_timestamp').count().show()

# COMMAND ----------

# DBTITLE 1,Qual foi a data e hora que mais ocorreu compra?
timestamp_compra = orders.groupby('order_purchase_timestamp').count()
display(timestamp_compra.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 90734 aprovações de compra em timestamps diferentes.
orders.select('order_approved_at').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_approved_at"
orders.groupby('order_approved_at').count().show()

# COMMAND ----------

# DBTITLE 1,Em qual timestamp ocorreu mais aprovações de compras?
timestamp_aprovacao = orders.groupby('order_approved_at').count()
display(timestamp_aprovacao.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,A base possui 81019 data de entrega da transportadora diferentes
orders.select('order_delivered_carrier_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_delivered_carrier_date"
orders.groupby('order_delivered_carrier_date').count().show()

# COMMAND ----------

# DBTITLE 1,Qual a data de entrega da transportadora mais comum?
timestamp_transportadora = orders.groupby('order_delivered_carrier_date').count()
display(timestamp_transportadora.orderBy('count', ascending=False))

#TEM MUITO NULO!!!

# COMMAND ----------

# DBTITLE 1,A base possui 95665 data de entrega ao cliente distintas
orders.select('order_delivered_customer_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_delivered_customer_date"
orders.groupby('order_delivered_customer_date').count().show()

# COMMAND ----------

# DBTITLE 1,Qual a data mais comum de entrega para o cliente?
timestamp_cliente = orders.groupby('order_delivered_customer_date').count()
display(timestamp_cliente.orderBy('count', ascending=False))

#Tem muito nulo!

# COMMAND ----------

# DBTITLE 1,A base possui 459 data prevista de entrega do pedido diferentes
orders.select('order_estimated_delivery_date').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "order_estimated_delivery_date"
orders.groupby('order_estimated_delivery_date').count().show()

# COMMAND ----------

# DBTITLE 1,Qual a data de entrega estimada mais comum?
timestamp_estimado = orders.groupby('order_estimated_delivery_date').count()
display(timestamp_estimado.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Explorando a base "products", quais os tipos de variáveis?
products.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "products" antes do tratamento de missings
display(products.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing na base "products"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in products.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(products.select(aux))

# COMMAND ----------

# DBTITLE 1,Tratamento de missing nas variáveis categóricas
products = products.fillna('NA', subset=['product_category_name'])

# COMMAND ----------

# DBTITLE 1,Remover as linhas que contém NA
products = products.dropna()
products.count()

# COMMAND ----------

# DBTITLE 1,Tratamento de missing das variáveis quantitativas
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

# DBTITLE 1,Verificação do missing após tratamento
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in products.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(products.select(aux))

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "Products" depois do tratamento
display(products.describe())

# COMMAND ----------

# DBTITLE 1,A base possui 32951 produtos...
products.select('product_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e não há repetição. 
products.select('product_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_id"
products.groupby('product_id').count().show()

# COMMAND ----------

# DBTITLE 1,A base apresenta 74 categorias de produtos distintos
products.select('product_category_name').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_category_name"
products.groupby('product_category_name').count().show()

# COMMAND ----------

# DBTITLE 1,Quais são as categorias de produtos mais vendidas?
categoria_produtos = products.groupby('product_category_name').count()
display(categoria_produtos.orderBy('count', ascending=False))

#TEM MUITO NULO!!!

# COMMAND ----------

# DBTITLE 1,Quais são as 10 maiores categorias de produtos dentre as 74?
display(categoria_produtos.orderBy('count', ascending=False).head(10))

# COMMAND ----------

# DBTITLE 1,Existem 67 possibilidades de tamanho do produto diferentes.
products.select('product_name_lenght').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_name_lenght"
products.groupby('product_name_lenght').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 2961 possibilidades no tamanho da descrição do produto
products.select('product_description_lenght').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência na variável "product_description_lenght"
products.groupby('product_description_lenght').count().show()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_description_lenght"
display(products.describe('product_description_lenght'))

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_description_lenght"
products.approxQuantile("product_description_lenght", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_description_lenght"
display(products.select('product_description_lenght'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_description_lenght"
display(products.select('product_description_lenght'))

# COMMAND ----------

# DBTITLE 1,Há até 20 fotos por produto.
products.select('product_photos_qty').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_photos_qty"
products.groupby('product_photos_qty').count().show()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_photos_qty"
display(products.describe('product_photos_qty'))

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_photos_qty"
products.approxQuantile("product_photos_qty", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_photos_qty"
display(products.select('product_photos_qty'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_photos_qty"
display(products.select('product_photos_qty'))

# COMMAND ----------

# DBTITLE 1,Há 2205 possíveis pesos para os produtos
products.select('product_weight_g').distinct().count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_weight_g"
display(products.describe('product_weight_g'))

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_weight_g"
products.groupby('product_weight_g').count().show()

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_weight_g"
products.approxQuantile("product_weight_g", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_weight_g"
display(products.select('product_weight_g'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_weight_g"
display(products.select('product_weight_g'))

# COMMAND ----------

# DBTITLE 1,Há 100 possibilidades de comprimento de produtos diferentes
products.select('product_length_cm').distinct().count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_length_cm"
display(products.describe('product_length_cm'))

# COMMAND ----------

# DBTITLE 1,Tabela de frequência na variável "product_length_cm"
products.groupby('product_length_cm').count().show()

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_length_cm"
products.approxQuantile("product_length_cm", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_length_cm"
display(products.select('product_length_cm'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_length_cm"
display(products.select('product_length_cm'))

# COMMAND ----------

# DBTITLE 1,Há 103 possibilidades de altura de produtos distintos
products.select('product_height_cm').distinct().count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_height_cm"
display(products.describe('product_height_cm'))

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_height_cm"
products.groupby('product_height_cm').count().show()

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_height_cm"
products.approxQuantile("product_height_cm", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_height_cm"
display(products.select('product_height_cm'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_height_cm"
display(products.select('product_height_cm'))

# COMMAND ----------

# DBTITLE 1,Existem 96 possibilidades de largura dos produtos distintos
products.select('product_width_cm').distinct().count()

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da variável "product_width_cm"
display(products.describe('product_width_cm'))

#A média de largura dos produtos é 23,2 cm

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_width_cm"
products.groupby('product_width_cm').count().show()

# COMMAND ----------

# DBTITLE 1,Quartis da variável "product_width_cm"
products.approxQuantile("product_width_cm", [0.5], 0.25)

# COMMAND ----------

# DBTITLE 1,Box-plot da variável "product_width_cm"
display(products.select('product_width_cm'))

# COMMAND ----------

# DBTITLE 1,Histograma da variável "product_width_cm"
display(products.select('product_width_cm'))

# COMMAND ----------

# DBTITLE 1,Analisando a base "sellers", quais os tipos de variáveis?
sellers.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "sellers"
display(sellers.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing da base "sellers"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in sellers.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(sellers.select(aux))

# COMMAND ----------

# DBTITLE 1,Nesta base há 3095 vendedores diferentes... 
sellers.select('seller_id').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e todos os IDs são únicos.
sellers.select('seller_id').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "seller_id"
sellers.groupby('seller_id').count().show()

# COMMAND ----------

# DBTITLE 1,Há 2246 CEPs diferentes na base...
sellers.select('seller_zip_code_prefix').distinct().count()

# COMMAND ----------

# DBTITLE 1,...de um total de 3095. Ou seja, há mais de um CEP repetido.
sellers.select('seller_zip_code_prefix').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "seller_zip_code_prefix"
sellers.groupby('seller_zip_code_prefix').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o CEP que mais repete?
CEP = sellers.groupby('seller_zip_code_prefix').count()
display(CEP.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Na base existem vendedores em 611 cidades 
sellers.select('seller_city').distinct().count()

# COMMAND ----------

# DBTITLE 1,...de um total de 3095 cidades.
sellers.select('seller_city').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "seller_city"
sellers.groupby('seller_city').count().show()

# COMMAND ----------

# DBTITLE 1,Qual cidade possui mais vendedores?
cidade_vendedores = sellers.groupby('seller_city').count()
display(cidade_vendedores.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Há 23 estados diferentes de vendedores...
sellers.select('seller_state').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e que se repetem na base. 
sellers.select('seller_state').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "seller_state"
sellers.groupby('seller_state').count().show()

# COMMAND ----------

# DBTITLE 1,Qual o estado que mais possui vendedores?
estado_vendedores = sellers.groupby('seller_state').count()
display(estado_vendedores.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Explorando a base "product_category", quais os tipos de variáveis?
product_category.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "product_category"
display(product_category.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing da base "product_category"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in product_category.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(product_category.select(aux))

# COMMAND ----------

# DBTITLE 1,Há 71 categorias de produtos diferentes... 
product_category.select('product_category_name').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e eles são únicos.
product_category.select('product_category_name').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "product_category_name"
product_category.groupby('product_category_name').count().show()

# COMMAND ----------

# DBTITLE 1,Analogamente, o mesmo ocorre com a variável "product_category_name_english"
product_category.select('product_category_name_english').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência para a variável "product_category_name_english"
product_category.groupby('product_category_name_english').count().show()

# COMMAND ----------

# DBTITLE 1,Analisando agora a base "geolocation", quais os tipos de variáveis?
geolocation.dtypes

# COMMAND ----------

# DBTITLE 1,Resumo estatístico da base "geolocation"
display(geolocation.describe())

# COMMAND ----------

# DBTITLE 1,Quantidade de missing da base "geolocation"
from pyspark.sql.functions import isnull, when, count, col

aux = []
for c in geolocation.columns:
  aux.append(count(when(isnull(c), c)).alias(c))

display(geolocation.select(aux))

# COMMAND ----------

# DBTITLE 1,A variável "geolocation_zip_code_prefix" possui 19015 registros distintos...
geolocation.select('geolocation_zip_code_prefix').distinct().count()

# COMMAND ----------

# DBTITLE 1,...e um total de 1000163 registros.
geolocation.select('geolocation_zip_code_prefix').count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "geolocation_zip_code_prefix"
geolocation.groupby('geolocation_zip_code_prefix').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 717372 informações diferentes de latitude.
geolocation.select('geolocation_lat').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "geolocation_lat"
geolocation.groupby('geolocation_lat').count().show()

# COMMAND ----------

# DBTITLE 1,A variável "geolocation_lng" possui 717615 registros diferentes. 
geolocation.select('geolocation_lng').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "geolocation_lng"
geolocation.groupby('geolocation_lng').count().show()

# COMMAND ----------

# DBTITLE 1,Existem 8011 cidades diferentes registradas na base
geolocation.select('geolocation_city').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "geolocation_city"
geolocation.groupby('geolocation_city').count().show()

# COMMAND ----------

# DBTITLE 1,Quais as cidades que mais apresentam repetição?
cidade_geo = geolocation.groupby('geolocation_city').count()
display(cidade_geo.orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Existem 27 estados distintos na base.
geolocation.select('geolocation_state').distinct().count()

# COMMAND ----------

# DBTITLE 1,Tabela de frequência da variável "geolocation_state"
geolocation.groupby('geolocation_state').count().show()

# COMMAND ----------

# DBTITLE 1,Quais os estados que aparecem mais vezes?
estado_geo = geolocation.groupby('geolocation_state').count()
display(estado_geo.orderBy('count', ascending=False))
