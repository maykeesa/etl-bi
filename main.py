from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import year, quarter, month

spark = SparkSession.builder.appName("SimpleApp").config('spark.jars', 'postgresql-42.6.0.jar').getOrCreate()

"""
Tabelas:
    categories - categorias
    products - produtos
    sales - vendas
    sales_items - itens vendidos
    customers - clientes
    sellers - vendedores
    suppliers - fornecedores

Informações para o Datawarehouse:
    obs:
    * -> Dataframe criado, falta ennviar apenas os dados necessários para o DW
    
    Tabelas:
    • Produtos mais vendidos - V*
    • Faturamento total - V*
    • Faturamento por categoria e por produto - V*
    • Maiores comissões de vendedores - V*
    • Quantidade de Fornecedores por estado - V*
    • Quantidade de clientes por estado - V*
    
    Regras:
    • Todas as representações devem estar por ano, trimestre e mês. - 
    • Todas as datas devem estar no formato YYYYMMDD
    • Todos os textos precisam estar em maiúsculo
    • Embora não esteja no sistema OLTP, no DW será preciso criar um campo "region" para guardar a região
    do estado.

    Observações:
    • É preciso calcular e armazenar o subtotal por item de venda.
"""


def get_dataframe_by_datatable(spark, dt):
    return spark.read \
        .jdbc("jdbc:postgresql://localhost:5432/techpop", dt, \
            properties={"user": "postgres", "password": "123", "driver":"org.postgresql.Driver"})

def teste(df_sales_items, df_products):    
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.orderBy("total_quantity", ascending=False)
    df_products = df_products.withColumn("year", year(df_products["date"]))
    return df_products

def get_top_selling_products_year(df_sales_items, df_products):    
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.orderBy("total_quantity", ascending=False)
    return df_products

def get_top_selling_products_quarter(df_sales_items, df_products):    
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.orderBy("total_quantity", ascending=False)
    df_products = df_products.withColumn("quarter", quarter(df_products["date"]))
    return df_products

def get_top_selling_products_month(df_sales_items, df_products):    
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.orderBy("total_quantity", ascending=False)
    df_products = df_products.withColumn("month", month(df_products["date"]))
    return df_products

def get_total_revenue(df_sales_items, df_products):
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.withColumn("total_revenue", df_products["total_quantity"] * df_products["price"])
    total_revenue = df_products.select("total_revenue").agg({"total_revenue": "sum"})
    total_revenue = total_revenue.withColumnRenamed("sum(total_revenue)", "total_revenue")
    return total_revenue

def get_total_revenue_by_category_and_product(df_sales_items, df_products, df_categories):
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_sales_items = df_sales_items.withColumnRenamed("price", "price_sales")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.join(df_categories, df_products["category_id"] == df_categories["category_id"], "inner")
    df_products = df_products.withColumn("total_revenue", col("total_quantity") * col("price"))

    return df_products    

def get_top_selling_sellers(df_sales, df_sellers):
    df_sales = df_sales.groupBy("seller_id").sum("total_price").withColumnRenamed("sum(total_price)", "total_price")
    df_sellers = df_sellers.join(df_sales, df_sellers["seller_id"] == df_sales["seller_id"], "inner")
    df_sellers = df_sellers.withColumn("total_commision", df_sellers["total_price"] * (df_sellers["tx_commission"] / 100))
    df_sellers = df_sellers.orderBy("total_commision", ascending=False)
    return df_sellers

def get_suppliers_by_state(df_suppliers):
    df_suppliers = df_suppliers.groupBy("state").count()
    df_suppliers = df_suppliers.withColumnRenamed("count", "total_suppliers")
    return df_suppliers

def get_customers_by_state(df_customers):
    df_customers = df_customers.groupBy("state").count()
    df_customers = df_customers.withColumnRenamed("count", "total_customers")
    return df_customers

if __name__ == "__main__":
    df_sales_items = get_dataframe_by_datatable(spark, "sales_items")
    df_products = get_dataframe_by_datatable(spark, "products")
    df_sales = get_dataframe_by_datatable(spark, "sales")
    df_customers = get_dataframe_by_datatable(spark, "customers")
    df_suppliers = get_dataframe_by_datatable(spark, "suppliers")
    df_sellers = get_dataframe_by_datatable(spark, "sellers")
    df_categories = get_dataframe_by_datatable(spark, "categories")

    get_top_selling_products_year(df_sales_items, df_products).show()
