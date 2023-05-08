# Databricks notebook source
# MAGIC %fs ls /FileStore/shared_uploads/yashbuty07@gmail.com/30cdbiotaws00124_accessKeys__1_-2.csv

# COMMAND ----------

aws_keys=spark.read.format('csv').option('header','true').option('inferschema','true').load('/FileStore/shared_uploads/yashbuty07@gmail.com/30cdbiotaws00124_accessKeys__1_-1.csv')
aws_keys.columns

# COMMAND ----------

ak=aws_keys.select('Access key ID').take(1)[0]['Access key ID']
sk=aws_keys.select('Secret access key').take(1)[0]['Secret access key']


# COMMAND ----------

import urllib
encoded=urllib.parse.quote(string=sk,safe="")

# COMMAND ----------

#bucket1
aws_bucket1='aws-bucky7'
mount_name1='/mnt/bucket1'

source_url1="s3a://{0}:{1}@{2}".format(ak,encoded,aws_bucket1)


# COMMAND ----------

dbutils.fs.mount(source_url1,mount_name1)

# COMMAND ----------

#bucket2
aws_bucket2='aws-bucky-8'
mount_name2='/mnt/b-8new'

source_url2="s3a://{0}:{1}@{2}".format(ak,encoded,aws_bucket2)

# COMMAND ----------

dbutils.fs.mount(source_url2,mount_name2)

# COMMAND ----------

aws_s3_df1=spark.read.format('csv').option('header','true').option('inferschema','true').load('/mnt/bucket1/P1 data.csv')
aws_s3_df1.show()

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

df1=spark.read.format('csv').option('header','true').option('inferschema','true').load('/mnt/bucket1/P1 data.csv')


# COMMAND ----------

display(df1)

# COMMAND ----------

display(df1.head(7))

# COMMAND ----------

from pyspark.sql.functions import col
num_rows = df1.count()
num_cols = len(df1.columns)

print((num_rows, num_cols))

# COMMAND ----------

df1.describe()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date
df2 = df1.withColumn("Order Date", to_date("Order Date", "yyyy-MM-dd"))
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import first
from pyspark.sql.functions import col

# COMMAND ----------

a = df2.groupBy(['Order Date', 'Profit']).count()
display(a.first())


# COMMAND ----------

from pyspark.sql.functions import col

df2.select([col(c).isNull().alias(c) for c in df2.columns]).show()


# COMMAND ----------

from pyspark.sql.functions import col, sum

df2.select([sum(col(c).isNull().cast("int")).alias(c) for c in df2.columns]).show()


# COMMAND ----------

df2 = df2.drop('Postal Code')

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(df2.select([sum(col(c).isNull().cast("int")).alias(c) for c in df2.columns]))

# COMMAND ----------

from pyspark.sql.functions import col

df3 = df2.withColumn('Ship Mode', col('Ship Mode').cast('string'))


# COMMAND ----------

from pyspark.sql.functions import trim

def remove_leading_spaces(df3):
    for col_name, data_type in df2.dtypes:
        if data_type in ['string', 'char', 'varchar']:
            df = df2.withColumn(col_name, trim(df2[col_name]))
    return df


# COMMAND ----------

df4 = remove_leading_spaces(df3)


# COMMAND ----------

display(df4.head(3))

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count

display(df4.groupBy('Country').agg(count('Order ID').alias('Order ID count')))


# COMMAND ----------

display(df4.groupby('Product ID').agg(count('Order ID')))

# COMMAND ----------

# Top 5 countries with hghtest quantity
from pyspark.sql.functions import sum, desc, col, dense_rank
from pyspark.sql.window import Window

w = Window.orderBy(desc('Total Quantity'))

top5q = df4.groupBy('Country') \
         .agg(sum('Quantity').alias('Total Quantity')) \
         .withColumn('rank', dense_rank().over(w)) \
         .filter(col('rank') <= 5) \
         .drop('rank') \
         .orderBy(desc('Total Quantity'))


# COMMAND ----------

display(top5q)

# COMMAND ----------

# Top 5 Products with hightest Order count
from pyspark.sql.functions import count
from pyspark.sql.window import Window

w = Window.orderBy(desc('Order Count'))

top5p = df4.groupBy('Product ID') \
         .agg(count('Order ID').alias('Order Count')) \
         .withColumn('rank', dense_rank().over(w)) \
         .filter(col('rank') <= 5) \
         .drop('rank') \
         .orderBy(desc('Order Count'))


# COMMAND ----------

top5p.write.mode('overwrite').csv('/mnt/b-8new/top5profit',header=True)

# COMMAND ----------

display(top5p)


# COMMAND ----------

from pyspark.sql.functions import sum, desc

top5= df4.groupBy('Product Name') \
         .agg(sum('Profit').alias('Total Profit')) \
         .orderBy(desc('Total Profit')) \
         .limit(5)


display(top5)


# COMMAND ----------

type(top5)

# COMMAND ----------

top5.write.mode('overwrite').csv('/mnt/b-8new/top5profit(1)',header=True)

# COMMAND ----------

# TOP 5 PRODUCT BY TOTAL PROFIT



import pyspark.sql.functions as F
import matplotlib.pyplot as plt

top5b = df4.groupBy('Product Name').agg(F.sum('Profit').alias('Total Profit')).orderBy(F.desc('Total Profit')).limit(5).toPandas()

top5b.plot(kind='bar', x='Product Name', y='Total Profit', title='Top 5 Most Profitable Products')
plt.xlabel('Product Name')
plt.ylabel('Profit')
plt.show()


# COMMAND ----------

#TOP 5 COUNTRY BY TOTAL PROFIT

top5 = df4.groupBy('Country').agg(F.sum('Profit').alias('Total Profit')).orderBy(F.desc('Total Profit')).limit(5).toPandas()

top5.plot(kind='bar', x='Country', y='Total Profit', title='Top 5 Most Profitable Countries', color='green')
plt.xlabel('Country')
plt.ylabel('Profit')
plt.show()


# COMMAND ----------

#TOP 5 PRODUCT BY TOTAL ORDER


import pyspark.sql.functions as F
import matplotlib.pyplot as plt

top5 = df4.groupBy('Product Name').agg(F.countDistinct('Order ID').alias('Order Count')).orderBy(F.desc('Order Count')).limit(5).toPandas()

top5.plot(kind='bar', x='Product Name', y='Order Count', title='Top 5 Most Ordered Products', color='blue')
plt.xlabel('Product Name')
plt.ylabel('Order Count')
plt.show()


# COMMAND ----------

# TOP 10 CITY BY TOTAL ORDER

import pyspark.sql.functions as F
import matplotlib.pyplot as plt

top10 = df4.groupBy('City').count().orderBy(F.desc('count')).limit(10).toPandas()

top10.plot(kind='barh', x='City', y='count', title='Top 10 Cities with Highest Order Count', color='navy')
plt.xlabel('Order Count')
plt.ylabel('City')
plt.show()



# COMMAND ----------

# TOTAL ORDER BY CATEGORY

from pyspark.sql.functions import desc
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

pdf1 = df4.groupBy('Category') \
        .agg(F.count('Order ID').alias('Order ID Count')) \
        .sort(desc('Order ID Count')) \
        .limit(5) \
        .toPandas()

pdf1.plot(kind='pie', y='Order ID Count', labels=pdf1['Category'], autopct='%1.1f%%', subplots=True)
plt.show()


# COMMAND ----------

pdf1.plot(kind='pie', y='Order ID Count', labels=pdf1['Category'], autopct='%1.1f%%', subplots=True)
plt.savefig("pie.pdf")


# COMMAND ----------

# TOTAL PROFIT BY CATEGORY



from pyspark.sql.functions import desc
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

pdf2 = df4.groupBy('Category') \
        .agg(F.sum('Profit').alias('Profit')) \
        .sort(desc('Profit')) \
        .limit(5) \
        .toPandas()

pdf2.plot(kind='pie', y='Profit', labels=pdf2['Category'], autopct='%1.1f%%', subplots=True)
plt.show()

