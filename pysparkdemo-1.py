#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()
from pyspark.sql import SparkSession


# In[ ]:


spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com").getOrCreate()

data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)


# In[ ]:


rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)


# In[ ]:


data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()


# In[ ]:


rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()


# In[ ]:


# By Calling function
def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

# Apply the func1 function using lambda
rdd2 = df.rdd.map(lambda x: func1(x))

#or
# Apply the func1 function to each element of the RDD using map()
rdd2 = df.rdd.map(func1)


# In[ ]:


for element in rdd2.collect():
    print(element)


# In[ ]:


# Create DataFrames for Employees and Departments
data_employees = [(1, "John", 1), (2, "Emma", 2), (3, "Raj", None), (4, "Nina", 4)]
data_departments = [(1, "HR"), (2, "Tech"), (3, "Marketing"), (None, "Temp")]

columns_employees = ["emp_id", "emp_name", "dept_id"]
columns_departments = ["dept_id", "dept_name"]

df_employees = spark.createDataFrame(data_employees, columns_employees)
df_departments = spark.createDataFrame(data_departments, columns_departments)

# Perform INNER JOIN
# since `inner` is the default join type, we can omit it
df_joined = df_employees.join(df_departments, df_employees.dept_id == df_departments.dept_id)

# Show the result
df_joined.show()


# In[ ]:


df_cross_joined = df_employees.crossJoin(df_departments)
df_cross_joined.show()


# In[ ]:


df_leftjoined = df_employees.join(df_departments, df_employees.dept_id == df_departments.dept_id, "left")

# Show the result
df_leftjoined.show()


# In[ ]:


df_rightjoined = df_employees.join(df_departments, df_employees.dept_id == df_departments.dept_id, "right")

# Show the result
df_rightjoined.show()


# In[ ]:


# Create DataFrames for Users and Purchases
data_users = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
data_purchases = [(1, "Book"), (2, "Pen"), (5, "Notebook")]

columns_users = ["id", "name"]
columns_purchases = ["user_id", "item"]

df_users = spark.createDataFrame(data_users, columns_users)
df_purchases = spark.createDataFrame(data_purchases, columns_purchases)

# Perform Left Semi Join
df_purchasers = df_users.join(df_purchases, df_users.id == df_purchases.user_id, "left_semi")

# Show the result
df_purchasers.show()

# Perform Left anti Join
df_purchasers = df_users.join(df_purchases, df_users.id == df_purchases.user_id, "left_anti")

# Show the result
df_purchasers.show()


# In[ ]:


df = spark.read.option("header",True) \
          .csv("Zipcodes.csv").createOrReplaceTempView("Zipcodes")


# In[ ]:


spark.sql("SELECT country, city, zipcode, state FROM ZIPCODES") \
     .show(5)


# In[ ]:


spark.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
          WHERE state = 'AZ' """) \
     .show(5)


# In[ ]:


spark.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
          WHERE state in ('PR','AZ','FL') order by state """) \
     .show(10)


# In[ ]:


spark.sql(""" SELECT state, count(*) as count FROM ZIPCODES 
          GROUP BY state""") \
     .show()


# In[ ]:




