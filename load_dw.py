from pyspark.sql import SparkSession
from time import sleep

from df_bd import get_total_revenue
from df_bd import get_suppliers_by_state
from df_bd import get_customers_by_state
from df_bd import get_top_selling_sellers
from df_bd import get_top_selling_products
from df_bd import get_total_revenue_by_product_and_category

spark = SparkSession.builder.appName("SimpleApp").config('spark.jars', 'postgresql-42.6.0.jar').getOrCreate()

def get_dataframe_by_datatable(spark, dt):
    return spark.read \
        .jdbc("jdbc:postgresql://localhost:5432/techpop", dt, \
            properties={"user": "postgres", "password": "123", "driver":"org.postgresql.Driver"})

def load_dw_table(df, table_name, mode="append"):
    df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/techpop_dw") \
    .option("dbtable", table_name) \
    .option("user", "postgres") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .mode(mode) \
    .save()

def load_dw():\
    # Dataframes 
    df_sales = get_dataframe_by_datatable(spark, "sales")
    df_sellers = get_dataframe_by_datatable(spark, "sellers")
    df_products = get_dataframe_by_datatable(spark, "products")
    df_customers = get_dataframe_by_datatable(spark, "customers")
    df_suppliers = get_dataframe_by_datatable(spark, "suppliers")
    df_categories = get_dataframe_by_datatable(spark, "categories")
    df_sales_items = get_dataframe_by_datatable(spark, "sales_items")
    
    # Load DW
    load_dw_table(get_suppliers_by_state(df_suppliers), "suppliers_by_state")
    load_dw_table(get_customers_by_state(df_customers), "customers_by_state")

    load_dw_table(get_total_revenue(df_sales_items, df_sales, df_products, "DATE"), "total_revenue", "overwrite")
    load_dw_table(get_total_revenue(df_sales_items, df_sales, df_products, "YEAR"), "total_revenue_year", "overwrite")
    load_dw_table(get_total_revenue(df_sales_items, df_sales, df_products, "MONTH"), "total_revenue_month", "overwrite")
    load_dw_table(get_total_revenue(df_sales_items, df_sales, df_products, "QUARTER"), "total_revenue_quarter", "overwrite")

    load_dw_table(get_top_selling_sellers(df_sales_items, df_sales, df_products, df_sellers, "DATE"), "top_selling_sellers_date", "overwrite")
    load_dw_table(get_top_selling_sellers(df_sales_items, df_sales, df_products, df_sellers, "YEAR"), "top_selling_sellers_year", "overwrite")
    load_dw_table(get_top_selling_sellers(df_sales_items, df_sales, df_products, df_sellers, "MONTH"), "top_selling_sellers_month", "overwrite")
    load_dw_table(get_top_selling_sellers(df_sales_items, df_sales, df_products, df_sellers, "QUARTER"), "top_selling_sellers_quarter", "overwrite")
    
    load_dw_table(get_top_selling_products(df_sales_items, df_sales, df_products, "DATE"), "top_selling_products", "overwrite")
    load_dw_table(get_top_selling_products(df_sales_items, df_sales, df_products, "YEAR"), "top_selling_products_year", "overwrite")
    load_dw_table(get_top_selling_products(df_sales_items, df_sales, df_products, "MONTH"), "top_selling_products_month", "overwrite")
    load_dw_table(get_top_selling_products(df_sales_items, df_sales, df_products, "QUARTER"), "top_selling_products_quarter", "overwrite")

    load_dw_table(get_total_revenue_by_product_and_category(df_sales_items, df_sales, df_products, df_categories, "DATE"), "total_revenue_by_product_and_category")
    load_dw_table(get_total_revenue_by_product_and_category(df_sales_items, df_sales, df_products, df_categories, "YEAR"), "total_revenue_by_product_and_category_year", "overwrite")
    load_dw_table(get_total_revenue_by_product_and_category(df_sales_items, df_sales, df_products, df_categories, "MONTH"), "total_revenue_by_product_and_category_month", "overwrite")
    load_dw_table(get_total_revenue_by_product_and_category(df_sales_items, df_sales, df_products, df_categories, "QUARTER"), "total_revenue_by_product_and_category_quarter", "overwrite")

if __name__ == "__main__":
    load_dw()
    print("\n\n\n\n\n\n\n")
    print("Carregando.")
    sleep(1)
    print("Carregando..")
    sleep(1)
    print("Carregando...")
    sleep(1)

    print("DW carregado com sucesso!")
