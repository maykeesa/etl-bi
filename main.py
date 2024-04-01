from pyspark.sql import SparkSession

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

    • Produtos mais vendidos - V*
    • Faturamento total - V*
    • Faturamento por categoria e por produto
    • Maiores comissões de vendedores
    • Quantidade de Fornecedores por estado
    • Quantidade de clientes por estado
    • Todas as representações devem estar por ano, trimestre e mês.
    • Todas as datas devem estar no formato YYYYMMDD
    • Todos os textos precisam estar em maiúsculo
    • Embora não esteja no sistema OLTP, no DW será preciso criar um campo "region" para guardar a região
    do estado.
    • É preciso calcular e armazenar o subtotal por item de venda.
"""


def get_dataframe_by_datatable(spark, dt):
    return spark.read \
        .jdbc("jdbc:postgresql://localhost:5432/techpop", dt, \
            properties={"user": "postgres", "password": "123", "driver":"org.postgresql.Driver"})

def get_top_selling_products(df_sales_items, df_products):    
    df_sales_items = df_sales_items.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.orderBy("total_quantity", ascending=False)
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
    df_products = df_products.join(df_sales_items, df_products["product_id"] == df_sales_items["product_id"], "inner")
    df_products = df_products.join(df_categories, df_products["product_id"] == df_categories["category_id"], "inner")
    df_products = df_products.withColumn("total_revenue", df_products["total_quantity"] * df_products["price"])
    return df_products

if __name__ == "__main__":
    df_products = get_dataframe_by_datatable(spark, "products")
    df_sales_items = get_dataframe_by_datatable(spark, "sales_items")
    df_categories = get_dataframe_by_datatable(spark, "products")
    
    """
    df_customers = get_dataframe_by_datatable(spark, "customers")
    df_sales = get_dataframe_by_datatable(spark, "sales")
    df_sellers = get_dataframe_by_datatable(spark, "sellers")
    df_suppliers = get_dataframe_by_datatable(spark, "suppliers")
    """

    get_total_revenue_by_category_and_product(df_sales_items, df_products, df_categories).show()
