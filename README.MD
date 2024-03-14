# задача
В PySpark приложении в виде DataFrame заданы продукты, категории и связи между ними.  
Каждому продукту может соответствовать несколько категорий или ни одной.   
А каждой категории может соответствовать несколько продуктов или ни одного.  
Напишите метод на PySpark, который в одном DataFrame вернет все пары    
«Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий. 

## вопрос
Как я это понял:      
На **вход**е у нас есть типичный many-to-many.    
Таблица Продукты, таблица  Категории, и обычная таблица связывания:   
id продукта и id категории.   
На **выход**е:
таблица ***Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.***    
**Задача:**
Написать оптимальный запрос на PySpark.

# ответ
ответ по ссылке можно проверить в google colab 
там всё готово, сначала ставим паиспарк, потом запускаем скрипт    
https://colab.research.google.com/drive/1DX1TdoB9mKGA5vvFS4L1sw_g5Y5H62Mh?usp=sharing       

products_data = [(1, "product1"), (2, "product2"), (3, "product3"), (4, "product4")]     
categories_data = [(1, "category1"), (2, "category2"), (3, "category3")]     
relations_data = [(1, 1), (2, 2), (1, 2)]     

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])     
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])     
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])    

products_df.createOrReplaceTempView("PDF")    
categories_df.createOrReplaceTempView("CDF")    
relations_df.createOrReplaceTempView("RDF")    

spark.sql("""SELECT p.product_name, c.category_name       
              FROM PDF p       
                LEFT OUTER JOIN RDF r ON p.product_id == r.product_id      
                  LEFT OUTER JOIN CDF c ON c.category_id == r.category_id""").show()

products_df \
    .join(relations_df, products_df.product_id == relations_df.product_id, 'left_outer') \
    .join(categories_df, categories_df.category_id == relations_df.category_id, 'left_outer') \
    .select(products_df.product_name, categories_df.category_name).show()


