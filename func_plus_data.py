from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ProductCategoryTask") \
    .getOrCreate()

products_data = [(1, "product1"), (2, "product2"), (3, "product3"), (4, "product4")]
categories_data = [(1, "category1"), (2, "category2"), (3, "category3")]
relations_data = [(1, 1), (2, 2), (1, 2)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

print("Products DataFrame:")
products_df.show()

print("Categories DataFrame:")
categories_df.show()

print("Relations DataFrame:")
relations_df.show()

products_df.createOrReplaceTempView("PDF")
categories_df.createOrReplaceTempView("CDF")
relations_df.createOrReplaceTempView("RDF")

print("ANTI JOIN")
spark.sql("""SELECT p.product_name, c.category_name  
              FROM PDF p 
                LEFT OUTER JOIN RDF r ON p.product_id == r.product_id 
                  LEFT OUTER JOIN CDF c ON c.category_id == r.category_id""").show()

products_df \
    .join(relations_df, products_df.product_id == relations_df.product_id, 'left_outer') \
    .join(categories_df, categories_df.category_id == relations_df.category_id, 'left_outer') \
    .select(products_df.product_name, categories_df.category_name).show()
