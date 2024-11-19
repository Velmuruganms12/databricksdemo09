# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Data from SQLServer and cache 

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
def cacheSqlServer(tableName):
  df = spark.read.format("jdbc").option("url", f"jdbc:sqlserver://104.237.2.219:1433;databaseName=retaildemo;encrypt=true;trustServerCertificate=true;") \
  .option("dbtable", "demoschema."+tableName) \
  .option("user", "sa") \
  .option("password", "AdminMSSQL2022") \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
  df.createOrReplaceTempView(tableName)
  #df.cache()  
  #df.persist(StorageLevel.DISK_ONLY)
  #df.persist(StorageLevel.MEMORY_ONLY)
  #df.display()


# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
cacheSqlServer("customers")
cacheSqlServer("departments")
cacheSqlServer("categories")
cacheSqlServer("products")
cacheSqlServer("orders")
cacheSqlServer("order_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'customers ' AS TABLE_NAME ,count(*) as Count FROM customers
# MAGIC UNION
# MAGIC SELECT 'departments' AS TABLE_NAME ,count(*) as Count FROM departments
# MAGIC UNION
# MAGIC SELECT 'categories' AS TABLE_NAME, count(*) as Count  FROM categories
# MAGIC UNION
# MAGIC SELECT 'products' AS TABLE_NAME,count(*) as Count FROM products
# MAGIC UNION
# MAGIC SELECT 'orders' AS TABLE_NAME,count(*) as Count FROM orders
# MAGIC UNION
# MAGIC SELECT 'order_items' AS TABLE_NAME,count(*) as Count FROM order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED select count(*) from orders

# COMMAND ----------

#Check cache in Memory or Disk
print("useMemory : ",spark.table("products").storageLevel.useMemory)
print("useDisk : ",spark.table("products").storageLevel.useDisk)

# COMMAND ----------

#Get How many Orders were placed:
#SQL
spark.sql("select count(*) from orders").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Average Revenue Per Day

# COMMAND ----------


spark.sql(
        """SELECT o.order_date, sum(oi.order_item_subtotal) / COUNT(DISTINCT oi.order_item_order_id) as avg_rev_per_day
          FROM orders o JOIN order_items oi 
          ON o.order_id = oi.order_item_order_id
          GROUP BY o.order_date 
          ORDER BY o.order_date
        """).display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get Total Revenue Per Month Per Year

# COMMAND ----------

total_rev_month_df=spark.sql(
        """SELECT YEAR(o.order_date) as order_year, MONTH(o.order_date) as order_month, SUM(oi.order_item_subtotal) tot_revenue 
          FROM orders o JOIN order_items oi 
              ON o.order_id = oi.order_item_order_id
          GROUP BY order_year, order_month 
          ORDER BY order_year, order_month
        """)
total_rev_month_df.display(truncate=False)
total_rev_month_df.cache()
#total_rev_month_df.persist(StorageLevel.MEMORY_ONLY)
total_rev_month_df.count()

# COMMAND ----------

import seaborn as sns
pdf = total_rev_month_df.toPandas()
g = sns.barplot(x='order_month', y='tot_revenue', hue='order_year', data=pdf)
g.set_title('Total Revenue Per Month Per Year');

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top Performing Departments:

# COMMAND ----------

top_perf_dep_df=spark.sql(
        """SELECT d.department_name, YEAR(o.order_date) as order_year, SUM(oi.order_item_subtotal) as tot_revenue
          FROM orders o 
              INNER JOIN order_items oi 
                  ON o.order_id = oi.order_item_order_id
              INNER JOIN products p
                  ON oi.order_item_product_id = p.product_id
              INNER JOIN categories c
                  ON c.category_id = p.product_category_id
              INNER JOIN departments d
                  ON c.category_department_id = d.department_id
          WHERE o.order_status <> 'CANCELED' AND o.order_status <> 'SUSPECTED_FRAUD'
          GROUP BY d.department_name, order_year
          ORDER BY d.department_name, order_year
        """)
top_perf_dep_df.display(truncate=False)

# COMMAND ----------

pdf = top_perf_dep_df.toPandas()
pdf = pdf.pivot(index='department_name', columns='order_year', values='tot_revenue')
print(pdf)
pdf.plot.bar(stacked=True, title='Top Performing Departments');

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 revenue generating products

# COMMAND ----------


spark.sql(
        """SELECT p.product_id, p.product_category_id, p.product_name, r.product_revenue
          FROM products p INNER JOIN
                              (SELECT oi.order_item_product_id, round(SUM(CAST(oi.order_item_subtotal as float)), 2) as product_revenue
                               FROM order_items oi INNER JOIN orders o 
                                   ON oi.order_item_order_id = o.order_id
                               WHERE o.order_status <> 'CANCELED'
                               AND o.order_status <> 'SUSPECTED_FRAUD'
                               GROUP BY oi.order_item_product_id) r
          ON p.product_id = r.order_item_product_id
          ORDER BY r.product_revenue DESC
          LIMIT 10
        """).display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 5 Highest Revenue Earning Products Per Month Per Year
# MAGIC

# COMMAND ----------

from pyspark.sql import  SQLContext
from pyspark.sql.types import *
# Map from month number to actual month string
monthmap = {1:"Jan", 2:"Feb", 3:"Mar",  4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}
# in order to use an udf with sql it needs to be registerd to sqlContext
sqlContext = SQLContext(spark.sparkContext)
sqlContext.udf.register("udfmonTomonth", lambda m: monthmap[m], StringType())

# COMMAND ----------

highest_rev_df = spark.sql(
        """SELECT q.* 
          FROM (
               SELECT r.*, DENSE_RANK() OVER (PARTITION by order_year, order_month ORDER BY product_revenue DESC) as dense_rank
               FROM (
                    SELECT YEAR(o.order_date) as order_year, udfmonTomonth(MONTH(o.order_date)) as order_month, p.product_name, ROUND(SUM(CAST(oi.order_item_subtotal as float)), 2) as product_revenue
                    FROM order_items oi 
                        INNER JOIN orders o 
                            ON oi.order_item_order_id = o.order_id
                        INNER JOIN products p
                            ON oi.order_item_product_id = p.product_id
                        WHERE o.order_status <> 'CANCELED' AND o.order_status <> 'SUSPECTED_FRAUD'
                        GROUP BY order_year, order_month, p.product_name ) r ) q
          WHERE q.dense_rank <= 5
          ORDER BY q.order_year, q.order_month, q.dense_rank
        """)

highest_rev_df.display(truncate=False)

# COMMAND ----------

pdf = highest_rev_df.toPandas()
g = sns.barplot(x="order_month", y="product_revenue", hue="product_name", data=pdf[pdf['order_year'] == 2014])
g.set_title('Top 5 Highest Revenue Earning Products Per Month in 2013');


# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the most popular Categories
# MAGIC

# COMMAND ----------

#Get the most popular Categories
pop_cat=spark.sql(
        """SELECT c.category_name, COUNT(order_item_quantity) as order_count 
          FROM order_items oi 
          INNER JOIN products p on oi.order_item_product_id = p.product_id 
          INNER JOIN categories c on c.category_id = p.product_category_id 
          GROUP BY c.category_name 
          ORDER BY order_count DESC 
          LIMIT 10 
        """)
pop_cat.display()

# COMMAND ----------

pdf = pop_cat.toPandas()
(pdf.plot(kind='pie', y = 'order_count', autopct='%1.1f%%', startangle=90, labels=pdf['category_name'], 
          legend=False, title='Most popular Categories', figsize=(9, 9)));

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the revenue for each Category Per Year Per Quarter
# MAGIC

# COMMAND ----------

(spark.sql(
"""SELECT *, ROUND(COALESCE(Q1, 0) + COALESCE(Q2, 0) + COALESCE(Q3, 0) + COALESCE(Q4, 0), 2) as total_sales FROM (
     SELECT * FROM (
        SELECT c.category_name, YEAR(o.order_date) as order_year, CONCAT('Q', QUARTER(o.order_date)) as order_quarter, order_item_subtotal
        FROM orders o 
        INNER JOIN order_items oi on order_item_order_id = o.order_id
        INNER JOIN products p on oi.order_item_product_id = p.product_id 
        INNER JOIN categories c on p.product_category_id = c.category_id
        WHERE o.order_status <> 'CANCELED' AND o.order_status <> 'SUSPECTED_FRAUD'
     )
     PIVOT (
        ROUND(SUM(order_item_subtotal), 2)
        FOR order_quarter in ('Q1', 'Q2', 'Q3', 'Q4')
     )
   )
   ORDER BY total_sales DESC
""")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UNCACHE table orders;
