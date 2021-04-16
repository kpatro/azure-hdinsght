
# coding: utf-8

# # Spark DataFrames Project Exercise 

# Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# 
# For now, just answer the questions and complete the tasks below.

# #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# #### Start a simple Spark Session

# In[ ]:

sc = SparkContext('yarn-client') # Azure automatically Provides SC


# #### Load the Walmart Stock CSV File, have Spark infer the data types.

# In[ ]:

df = sp.read.option("header",true,inferSchema='true').csv("walmart_stock.csv")


# #### What are the column names?

# In[67]:

df.columns


# #### What does the Schema look like?

# In[68]:

df.printSchema()


# #### Print out the first 5 columns.

# In[76]:

df.show(n=5)


# #### Use describe() to learn about the DataFrame.

# In[77]:

df.describe().show()


# ## Bonus Question!
# #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# 
# If you get stuck on this, don't worry, just view the solutions.

# In[80]:

from pyspark.sql.functions import round
df_desc = df.describe().show()

for c in df_desc.columns:
    df_desc = df_desc.withColumn(c, round(c, 2))
    
df2 = df_desc.withColumn("Volume", func.round(df["Volume"]).cast('integer'))
df2.show()


# #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# In[81]:

df_ratio = df_desc.withColumn('HV Ratio', col('High') / col('Volume'))
df_ratio.show()


# #### What day had the Peak High in Price?

# In[88]:

df_desc


# #### What is the mean of the Close column?

# In[89]:

df_desc.agg({'Close': 'avg'}).show()


# #### What is the max and min of the Volume column?

# In[90]:

from pyspark.sql.functions import max,min


# In[92]:

df_desc.agg(max("Volume").alias("Max")
            min("Volume").alias("Min")).show()


# #### How many days was the Close lower than 60 dollars?

# In[100]:




# #### What percentage of the time was the High greater than 80 dollars ?
# #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# In[107]:




# #### What is the Pearson correlation between High and Volume?
# #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# In[110]:

df_desc.stat.corr("High","Volume")


# #### What is the max High per year?

# In[117]:

df_desc.groupBy("Year").max("salary")
from pyspark.sql.functions import year
df_desc.groupBy(year("Date").alias("Year")).agg(max("Close").alias("Close")).show()


# #### What is the average Close for each Calendar Month?
# #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# In[121]:

df_desc.groupBy(month("Date").alias("Month")).agg(avg("Close").alias("Close")).show()


# # Great Job!
