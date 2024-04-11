from pyspark.sql import functions as F

def get_top_selling_products(df_sales_items, df_sales, df_products, type_date="YEAR"):
    df_joined = df_sales_items.join(df_sales, "sales_id") \
        .join(df_products, "product_id")
    
    df_joined = df_joined.withColumn("product_name", F.upper(F.col("product_name")))
    
    if type_date == "YEAR":
        df_joined = df_joined.withColumn("period", F.year(F.col("date")))
    elif type_date == "QUARTER":
        df_joined = df_joined.withColumn("period", F.quarter(F.col("date")))
    elif type_date == "MONTH":
        df_joined = df_joined.withColumn("period", F.month(F.col("date")))
    elif type_date == "DATE":
        df_joined = df_joined.withColumn("period", F.date_format(F.col("date"), "yyyyMMdd"))

    df_result = df_joined.groupBy("period", "product_name") \
        .agg(F.sum("quantity").alias("qtd")) \
        .orderBy("period", F.col("qtd").desc())
    
    return df_result

def get_total_revenue(df_sales_items, df_sales, df_products, type_date="YEAR"):
    df_sales_items_alias = df_sales_items.alias('si')
    df_sales_alias = df_sales.alias('sl')
    df_products_alias = df_products.alias('p')

    df_joined = df_sales_items_alias.join(df_sales_alias, "sales_id") \
                                    .join(df_products_alias, "product_id")

    df_joined = df_joined.withColumn("total_revenue", F.col("p.price") * F.col("si.quantity"))
    
    if type_date == "YEAR":
        df_result = df_joined.withColumn("DATE", F.year(F.col("sl.date")).cast("string"))
    elif type_date == "QUARTER":
        df_result = df_joined.withColumn("DATE", F.expr("CONCAT(YEAR(sl.date), 'Q', QUARTER(sl.date))"))
    elif type_date == "MONTH":
        df_result = df_joined.withColumn("DATE", F.date_format(F.col("sl.date"), "yyyyMM"))
    elif type_date == "DATE":
        df_result = df_joined.withColumn("DATE", F.date_format(F.col("sl.date"), "yyyyMMdd"))

    df_result = df_result.withColumn("PRODUCT_NAME", F.upper(F.col("p.product_name")))
    
    df_grouped = df_result.groupBy("DATE").agg(F.sum("total_revenue").alias("TOTAL_REVENUE"))

    df_ordered = df_grouped.orderBy("DATE", F.col("TOTAL_REVENUE").desc())

    return df_ordered

def get_total_revenue_by_product_and_category(df_sales_items, df_sales, df_products, df_categories, type_date="YEAR"):
    df_sales_items_alias = df_sales_items.alias('si')
    df_sales_alias = df_sales.alias('sl')
    df_products_alias = df_products.alias('p')
    df_categories_alias = df_categories.alias('c')

    df_joined = df_sales_items_alias.join(df_sales_alias, "sales_id") \
                                    .join(df_products_alias, "product_id") \
                                    .join(df_categories_alias, "category_id")

    df_joined = df_joined.withColumn("total_revenue", F.col("p.price") * F.col("si.quantity"))

    if type_date == "YEAR":
        df_joined = df_joined.withColumn("sale_period", F.year(F.col("sl.date")))
    elif type_date == "QUARTER":
        df_joined = df_joined.withColumn("sale_period", F.quarter(F.col("sl.date")))
    elif type_date == "MONTH":
        df_joined = df_joined.withColumn("sale_period", F.month(F.col("sl.date")))
    elif type_date == "DATE":
        df_joined = df_joined.withColumn("sale_period", F.date_format(F.col("sl.date"), "yyyyMMdd"))

    df_joined = df_joined.withColumn("nome_produto", F.upper(F.col("p.product_name"))) \
                         .withColumn("categoria", F.upper(F.col("c.category_name")))

    df_grouped = df_joined.groupBy("sale_period", "nome_produto", "categoria") \
                          .agg(F.sum("total_revenue").alias("total_revenue"))

    df_ordered = df_grouped.orderBy(F.col("sale_period").asc(), F.col("total_revenue").desc())

    return df_ordered

def get_top_selling_sellers(df_sales_items, df_sales, df_products, df_sellers, type_date="YEAR"):
    df_sales_items_alias = df_sales_items.alias('si')
    df_sales_alias = df_sales.alias('s')
    df_products_alias = df_products.alias('p')
    df_sellers_alias = df_sellers.alias('s2')

    df_joined = df_sales_items_alias.join(df_sales_alias, "sales_id") \
                                    .join(df_products_alias, "product_id") \
                                    .join(df_sellers_alias, "seller_id")

    df_joined = df_joined.withColumn("commission", (F.col("si.quantity") * F.col("p.price") * F.col("s2.tx_commission")) / 100)

    if type_date == "YEAR":
        df_joined = df_joined.withColumn("period", F.year(F.col("s.date")))
    elif type_date == "QUARTER":
        df_joined = df_joined.withColumn("period", F.quarter(F.col("s.date")))
    elif type_date == "MONTH":
        df_joined = df_joined.withColumn("period", F.month(F.col("s.date")))
    elif type_date == "DATE":
        df_joined = df_joined.withColumn("period", F.date_format(F.col("s.date"), "yyyyMMdd"))

    df_joined = df_joined.withColumn("seller_name", F.upper(F.col("s2.seller_name")))

    df_grouped = df_joined.groupBy("period", "seller_name") \
                          .agg(F.sum("commission").alias("commission"))

    df_ordered = df_grouped.orderBy("period", F.col("commission").desc())

    return df_ordered

def get_suppliers_by_state(df_suppliers):
    df_suppliers = df_suppliers.groupBy("state").count()
    df_suppliers = df_suppliers.withColumnRenamed("count", "total_suppliers")
    return df_suppliers

def get_customers_by_state(df_customers):
    df_customers = df_customers.groupBy("state").count()
    df_customers = df_customers.withColumnRenamed("count", "total_customers")
    return df_customers
