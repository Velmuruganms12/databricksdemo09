# Databricks notebook source
# Sample data for principal, rate, and time
data = [
    (1000, 0.05, 5),  # $1000 at 5% interest for 5 years
    (1500, 0.04, 3),  # $1500 at 4% interest for 3 years
    (2000, 0.03, 10), # $2000 at 3% interest for 10 years
    (500, 0.06, 7),   # $500 at 6% interest for 7 years
    (1200, 0.07, 4)   # $1200 at 7% interest for 4 years
]




# Define schema for the DataFrame
schema = ["principal", "rate", "time"]
df = spark.createDataFrame(data, schema=schema)
df.display()
df.createOrReplaceTempView("investments")
print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Define the Compound Interest Function and Register it as a UDF
# MAGIC formula for compound interest is:
# MAGIC
# MAGIC Future Value=Principal×(1+Rate) 
# MAGIC Time

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
# Define the compound interest function
def compound_interest(principal, rate, time):
    return principal * ((1 + rate) ** time)

# Register the UDF
#compound_interest_udf = udf(compound_interest, DoubleType())
spark.udf.register("compound_interest", compound_interest, DoubleType())


# COMMAND ----------

# Apply the UDF to calculate the future value
df_with_future_value = df.withColumn("future_value", compound_interest_udf(df["principal"], df["rate"], df["time"]))

# Show the result
df_with_future_value.display()

# COMMAND ----------

# Run the SQL query
result = spark.sql("""
    SELECT principal, rate, time,compound_interest(principal, rate, time) AS future_value
    FROM investments
""")

# Show the result
result.display()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,StructType,StructField

# Extended data with name, address, city, country, and phone number
data = [
    ("1", "Alice", "123 Maple St", "New York", "USA", "123-456-7890"),
    ("2", "Bob", "456 Oak St", "Los Angeles", "USA", "234-567-8901"),
    ("3", "Cathy", "789 Pine St", "Chicago", "USA", "345-678-9012"),
    ("4", "David", "101 Cedar St", "Houston", "USA", "456-789-0123"),
    ("5", "Eve", "202 Birch St", "Phoenix", "USA", "567-890-1234")
]

# Update the schema for the DataFrame
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone_number", StringType(), True)
])
# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)
df.createOrReplaceTempView("customers")
# Show the DataFrame
df.display()


# COMMAND ----------

def mask_phone(phone):
    if phone and len(phone) >= 10:
        return phone[:-4] + "****"
    return phone

spark.udf.register("mask_phone", mask_phone, StringType())

# COMMAND ----------

# Run the SQL query
result = spark.sql("""SELECT *, mask_phone(phone_number) AS masked_phone FROM customers """)

# Show the result
result.show()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Define the compound interest function as a Pandas UDF
@pandas_udf(DoubleType())
def compound_interest_pandas(principal: pd.Series, rate: pd.Series, time: pd.Series) -> pd.Series:
    return principal * ((1 + rate) ** time)

# Register the Pandas UDF in Spark SQL
spark.udf.register("compound_interest_pandas", compound_interest_pandas)

# COMMAND ----------

# Run the SQL query
result = spark.sql("""
    SELECT principal, rate, time,compound_interest_pandas(principal, rate, time) AS future_value
    FROM investments
""")

# Show the result
result.display()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define the risk_score UDF based on conditions
def risk_score_udf(purchase_frequency, avg_purchase_value, days_since_last_purchase):
    if purchase_frequency < 2 and avg_purchase_value < 100 and days_since_last_purchase > 30:
        return 3  # High Risk
    elif 2 <= purchase_frequency <= 5 and 100 <= avg_purchase_value <= 200:
        return 2  # Medium Risk
    else:
        return 1  # Low Risk

# Register the UDF in Spark
risk_score = udf(risk_score_udf, IntegerType())
spark.udf.register("risk_score", risk_score_udf, IntegerType())

# COMMAND ----------

# Sample data
data = [
    (1, 50, 45),   # High Risk
    (3, 150, 20),  # Medium Risk
    (4, 250, 10),  # Low Risk
    (6, 120, 5)    # Low Risk
]

# Create DataFrame
columns = ["purchase_frequency", "avg_purchase_value", "days_since_last_purchase"]
df = spark.createDataFrame(data, columns)

# Create a temporary view for SQL querying
df.createOrReplaceTempView("customer_data")
df.display()

# COMMAND ----------

# Query using the UDF in Spark SQL
result_df = spark.sql("""
    SELECT *, 
           risk_score(purchase_frequency, avg_purchase_value, days_since_last_purchase) AS risk_score
    FROM customer_data
""")

# Show results
result_df.show()
