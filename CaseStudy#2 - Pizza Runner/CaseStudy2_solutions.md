# Case Study #2: Pizza Runner
___
The case study questions presented here are created by [**Data With Danny**](https://linktr.ee/datawithdanny). They are part of the [**8 WeekSQL Challenge**](https://8weeksqlchallenge.com/).

My SQL queries are written in the `MySQL` language, integrated into `Jupyter Notebook`, which allows us to instantly view the query results and document the queries.

For more details about the **Case Study #2**, click [**here**](https://8weeksqlchallenge.com/case-study-2/).

## Table of Contents
___
### [1. Importing Libraries](#Import)
### [2. Tables of the Database](#Tables)
### [3. Data Cleaning](#DataCleaning)
### [4. Case Study Questions](#CaseStudyQuestions)

- [A. Pizza Metrics](#A)
- [B. Runner and Customer Experience](#B)
- [C. Ingredient Optimisation](#C)
- [D. Pricing and Ratings](#D)
- [E. Bonus Questions](#E)

___
<a id="Import"></a>
## 1. Importing necessary libraries


```python
import os
import pymysql
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
```

___
<a id="Tables"></a>
## 2. Tables of the Database

### Connecting MySQL database from Jupyter Notebook


```python
# Get MySQL password from Environment Variables
password = os.getenv('MYSQL_PASSWORD')
```


```python
# Connect MySQL database from Jupyter Notebook
conn = pymysql.connect(host = 'localhost', user = 'root', passwd = password, db = 'pizza_runner')
mycursor = conn.cursor()


# Display tables within "pizza_runner" database
mycursor.execute("SHOW TABLES;")
print('--- Tables within "pizza_runner" database --- ')
for table in mycursor:
    print(table[0])
```

    --- Tables within "pizza_runner" database --- 
    customer_orders
    pizza_names
    pizza_recipes
    pizza_toppings
    runner_orders
    runners
    

The followings are the 5 tables within the `pizza_runner` database. Please click [here](https://8weeksqlchallenge.com/case-study-2/) to get more insights about the tables.


```python
mycursor.execute("SHOW TABLES;")
for table in mycursor:
    print("\nTable:", table[0])
    query = """SELECT * FROM pizza_runner.""" + table[0]
    df = pd.read_sql(query, conn)
    display(df)
```

    
    Table: customer_orders

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>order_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 18:05:02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 19:00:52</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>2</td>
      <td></td>
      <td>None</td>
      <td>2020-01-02 23:51:23</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>1</td>
      <td>null</td>
      <td>1</td>
      <td>2020-01-08 21:00:29</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>2</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-08 21:03:13</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td>null</td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>1</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-09 23:54:33</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>2020-01-10 11:22:59</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-11 18:34:49</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2020-01-11 18:34:49</td>
    </tr>
  </tbody>
</table>
</div>


    
    Table: pizza_names

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>pizza_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Vegetarian</td>
    </tr>
  </tbody>
</table>
</div>


    
    Table: pizza_recipes
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>toppings</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1, 2, 3, 4, 5, 6, 8, 10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>4, 6, 7, 9, 11, 12</td>
    </tr>
  </tbody>
</table>
</div>


    
    Table: pizza_toppings
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>topping_id</th>
      <th>topping_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>BBQ Sauce</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Beef</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Cheese</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Chicken</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Mushrooms</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Onions</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>Pepperoni</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>Peppers</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>Salami</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>Tomatoes</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>Tomato Sauce</td>
    </tr>
  </tbody>
</table>
</div>


    
    Table: runner_orders
    

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>runner_id</th>
      <th>pickup_time</th>
      <th>distance</th>
      <th>duration</th>
      <th>cancellation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2020-01-01 18:15:34</td>
      <td>20km</td>
      <td>32 minutes</td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2020-01-01 19:10:54</td>
      <td>20km</td>
      <td>27 minutes</td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>2020-01-03 00:12:37</td>
      <td>13.4km</td>
      <td>20 mins</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2</td>
      <td>2020-01-04 13:53:03</td>
      <td>23.4</td>
      <td>40</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>3</td>
      <td>2020-01-08 21:10:57</td>
      <td>10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>3</td>
      <td>null</td>
      <td>null</td>
      <td>null</td>
      <td>Restaurant Cancellation</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2</td>
      <td>2020-01-08 21:30:45</td>
      <td>25km</td>
      <td>25mins</td>
      <td>null</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>2</td>
      <td>2020-01-10 00:15:02</td>
      <td>23.4 km</td>
      <td>15 minute</td>
      <td>null</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>2</td>
      <td>null</td>
      <td>null</td>
      <td>null</td>
      <td>Customer Cancellation</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>1</td>
      <td>2020-01-11 18:50:20</td>
      <td>10km</td>
      <td>10minutes</td>
      <td>null</td>
    </tr>
  </tbody>
</table>
</div>


    
    Table: runners
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>registration_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2021-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2021-01-03</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2021-01-08</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2021-01-15</td>
    </tr>
  </tbody>
</table>
</div>


___
<a id="DataCleaning"></a>
## 3. Data Cleaning

Notice that `customer_orders` and `runner_orders` tables contain inconsistent data, with either incorrect datatypes, missing values (`""`,`null` or `None`) or both. Conducting a data cleaning on these two datasets are required before jumping into the Case Study Questions.

## Data Cleaning: `customer_orders` table


```python
# Check datatypes
pd.read_sql_query("""
DESCRIBE pizza_runner.customer_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Field</th>
      <th>Type</th>
      <th>Null</th>
      <th>Key</th>
      <th>Default</th>
      <th>Extra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>order_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>customer_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>pizza_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>exclusions</td>
      <td>varchar(4)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>extras</td>
      <td>varchar(4)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>order_time</td>
      <td>timestamp</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
  </tbody>
</table>
</div>




```python
# Find the unique values available in the columns "exclusions" and "extras"
pd.read_sql_query("""
SELECT DISTINCT exclusions, extras
FROM pizza_runner.customer_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>exclusions</th>
      <th>extras</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>null</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>null</td>
      <td>null</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>1, 5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2, 6</td>
      <td>1, 4</td>
    </tr>
  </tbody>
</table>
</div>



Let's attempt to tidy up the table by ensuring the data is consistent.


```python
pd.read_sql_query("""
SELECT *, 
    CASE WHEN exclusions = "null" THEN "" ELSE exclusions END AS exclusions_corrected,
    CASE WHEN extras IS NULL OR extras = "null" THEN "" ELSE extras END AS extras_corrected
FROM pizza_runner.customer_orders;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>order_time</th>
      <th>exclusions_corrected</th>
      <th>extras_corrected</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 18:05:02</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 19:00:52</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>2</td>
      <td></td>
      <td>None</td>
      <td>2020-01-02 23:51:23</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td></td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td></td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>1</td>
      <td>null</td>
      <td>1</td>
      <td>2020-01-08 21:00:29</td>
      <td></td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>2</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-08 21:03:13</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td>null</td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
      <td></td>
      <td>1</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>1</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-09 23:54:33</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>2020-01-10 11:22:59</td>
      <td>4</td>
      <td>1, 5</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>null</td>
      <td>null</td>
      <td>2020-01-11 18:34:49</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2020-01-11 18:34:49</td>
      <td>2, 6</td>
      <td>1, 4</td>
    </tr>
  </tbody>
</table>
</div>



Now, let's update the `customer_orders` table.


```python
mycursor.execute("""
UPDATE pizza_runner.customer_orders
SET 
exclusions = CASE WHEN exclusions = 'null' THEN '' ELSE exclusions END,
extras = CASE WHEN extras IS NULL OR extras = 'null' THEN '' ELSE extras END;
""")

# Saving the updates
conn.commit()
```

Here is the updated and processed `customer_orders` table.


```python
pd.read_sql_query("""SELECT * FROM pizza_runner.customer_orders;""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>order_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 18:05:02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 19:00:52</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:00:29</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-08 21:03:13</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-09 23:54:33</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>2020-01-10 11:22:59</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-11 18:34:49</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2020-01-11 18:34:49</td>
    </tr>
  </tbody>
</table>
</div>



## Data Cleaning: `runner_orders` table


```python
# Check datatypes
pd.read_sql_query("""
DESCRIBE pizza_runner.runner_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Field</th>
      <th>Type</th>
      <th>Null</th>
      <th>Key</th>
      <th>Default</th>
      <th>Extra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>order_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>runner_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>pickup_time</td>
      <td>varchar(19)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>distance</td>
      <td>varchar(7)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>duration</td>
      <td>varchar(10)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>cancellation</td>
      <td>varchar(23)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
  </tbody>
</table>
</div>



**UPDATES TO DO**

- The `runner_orders` table contains incorrect data types for the columns `pickup_time`, `distance`, and `duration`.
- The `pickup_time` column should be of type **TIMESTAMP** instead of VARCHAR.
- Both the `distance` and `duration` columns should be of type **FLOAT or INTEGER** instead of VARCHAR.
- The missing values in the `cancellation` column need to be standardized.

Let's attempt to tidy up the table by ensuring the data is consistent.


```python
pd.read_sql_query("""
SELECT
    *,
    CASE WHEN pickup_time = 'null' THEN NULL ELSE pickup_time END AS pickup_time_corrected,
    REGEXP_SUBSTR(distance, '[0-9]+(\.[0-9]+)?') AS dist_re, 
    REGEXP_SUBSTR(duration, '^[0-9]+') AS dur_re,
    CASE WHEN cancellation IN ('null', '') THEN NULL ELSE cancellation END AS cancellation_corrected
FROM pizza_runner.runner_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>runner_id</th>
      <th>pickup_time</th>
      <th>distance</th>
      <th>duration</th>
      <th>cancellation</th>
      <th>pickup_time_corrected</th>
      <th>dist_re</th>
      <th>dur_re</th>
      <th>cancellation_corrected</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2020-01-01 18:15:34</td>
      <td>20km</td>
      <td>32 minutes</td>
      <td></td>
      <td>2020-01-01 18:15:34</td>
      <td>20</td>
      <td>32</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2020-01-01 19:10:54</td>
      <td>20km</td>
      <td>27 minutes</td>
      <td></td>
      <td>2020-01-01 19:10:54</td>
      <td>20</td>
      <td>27</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>2020-01-03 00:12:37</td>
      <td>13.4km</td>
      <td>20 mins</td>
      <td>None</td>
      <td>2020-01-03 00:12:37</td>
      <td>13.4</td>
      <td>20</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2</td>
      <td>2020-01-04 13:53:03</td>
      <td>23.4</td>
      <td>40</td>
      <td>None</td>
      <td>2020-01-04 13:53:03</td>
      <td>23.4</td>
      <td>40</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>3</td>
      <td>2020-01-08 21:10:57</td>
      <td>10</td>
      <td>15</td>
      <td>None</td>
      <td>2020-01-08 21:10:57</td>
      <td>10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>3</td>
      <td>null</td>
      <td>null</td>
      <td>null</td>
      <td>Restaurant Cancellation</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>Restaurant Cancellation</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2</td>
      <td>2020-01-08 21:30:45</td>
      <td>25km</td>
      <td>25mins</td>
      <td>null</td>
      <td>2020-01-08 21:30:45</td>
      <td>25</td>
      <td>25</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>2</td>
      <td>2020-01-10 00:15:02</td>
      <td>23.4 km</td>
      <td>15 minute</td>
      <td>null</td>
      <td>2020-01-10 00:15:02</td>
      <td>23.4</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>2</td>
      <td>null</td>
      <td>null</td>
      <td>null</td>
      <td>Customer Cancellation</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>Customer Cancellation</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>1</td>
      <td>2020-01-11 18:50:20</td>
      <td>10km</td>
      <td>10minutes</td>
      <td>null</td>
      <td>2020-01-11 18:50:20</td>
      <td>10</td>
      <td>10</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Updating the table
mycursor.execute(
"""
UPDATE pizza_runner.runner_orders
SET
pickup_time = CASE WHEN pickup_time = 'null' THEN NULL ELSE pickup_time END,
distance = REGEXP_SUBSTR(distance, '[0-9]+(\.[0-9]+)?'),
duration = REGEXP_SUBSTR(duration, '^[0-9]+'),
cancellation = CASE WHEN cancellation IN ('null', '') THEN NULL ELSE cancellation END;
""")


# Correcting the data types of the columns
mycursor.execute(
"""
ALTER TABLE pizza_runner.runner_orders
MODIFY COLUMN pickup_time TIMESTAMP,
MODIFY COLUMN distance FLOAT,
MODIFY COLUMN duration INT;
"""
)


# Saving the updates
conn.commit()
```

The data types of the columns have been corrected.


```python
pd.read_sql_query("""
DESCRIBE pizza_runner.runner_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Field</th>
      <th>Type</th>
      <th>Null</th>
      <th>Key</th>
      <th>Default</th>
      <th>Extra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>order_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>runner_id</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>pickup_time</td>
      <td>timestamp</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>distance</td>
      <td>float</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>duration</td>
      <td>int</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>cancellation</td>
      <td>varchar(23)</td>
      <td>YES</td>
      <td></td>
      <td>None</td>
      <td></td>
    </tr>
  </tbody>
</table>
</div>



Now, the `runner_orders` table contains consistent data.


```python
pd.read_sql_query("""SELECT * FROM pizza_runner.runner_orders;""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>runner_id</th>
      <th>pickup_time</th>
      <th>distance</th>
      <th>duration</th>
      <th>cancellation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2020-01-01 18:15:34</td>
      <td>20.0</td>
      <td>32.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2020-01-01 19:10:54</td>
      <td>20.0</td>
      <td>27.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>2020-01-03 00:12:37</td>
      <td>13.4</td>
      <td>20.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2</td>
      <td>2020-01-04 13:53:03</td>
      <td>23.4</td>
      <td>40.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>3</td>
      <td>2020-01-08 21:10:57</td>
      <td>10.0</td>
      <td>15.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>3</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Restaurant Cancellation</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2</td>
      <td>2020-01-08 21:30:45</td>
      <td>25.0</td>
      <td>25.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>2</td>
      <td>2020-01-10 00:15:02</td>
      <td>23.4</td>
      <td>15.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>2</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Customer Cancellation</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>1</td>
      <td>2020-01-11 18:50:20</td>
      <td>10.0</td>
      <td>10.0</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```

___
<a id="CaseStudyQuestions"></a>
## 4. Case Study Questions


```python
# Connecting MySQL database 
conn = pymysql.connect(host = 'localhost', user = 'root', passwd = password, db = 'pizza_runner')
mycursor = conn.cursor()
```

<a id="A"></a> 
## A. Pizza Metrics

<a id="A1"></a>
#### 1. How many pizzas were ordered?


```python
pd.read_sql_query("""
SELECT COUNT(*) AS TotalOrders
FROM pizza_runner.customer_orders;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>TotalOrders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
There were a total of `14 pizzas ordered` at Pizza Runner in January 2020.

___
#### 2. How many unique customer orders were made?


```python
pd.read_sql_query("""
SELECT COUNT(DISTINCT order_id) AS TotalCustomerOrders 
FROM pizza_runner.customer_orders;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>TotalCustomerOrders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
In January 2020, there have been a total of `10 orders` served by Pizza Runner.

___
#### 3.How many successful orders were delivered by each runner?


```python
pd.read_sql_query("""
SELECT 
    runner_id, 
    COUNT(order_id) AS SuccessfulOrders
FROM pizza_runner.runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>SuccessfulOrders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**
- **Runner 1** holds the record for the highest number of successful orders, delivering a total of 4 pizzas.
- **Runner 2** delivered 3 successful orders.
- **Runner 3** has the least impressive performance, with only 1 successful delivery.

___
#### 4. How many of each type of pizza was delivered?


```python
pd.read_sql_query("""
SELECT 
    p.pizza_name, 
    COUNT(*) AS SuccessfulOrders
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
JOIN pizza_runner.pizza_names p ON c.pizza_id = p.pizza_id
WHERE r.cancellation IS NULL
GROUP BY p.pizza_name;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_name</th>
      <th>SuccessfulOrders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Meatlovers</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Vegetarian</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
Pizza Runner made a total of `9 sales` for **Meatlovers pizzas**, and only `3 sales` for **Vegetarian pizzas**.

___
#### 5. How many Vegetarian and Meatlovers were ordered by each customer?


```python
pd.read_sql_query("""
SELECT 
    c.customer_id,
    COUNT(CASE WHEN p.pizza_name = 'Meatlovers' THEN 1 END) AS meatlovers_count,
    COUNT(CASE WHEN p.pizza_name = 'Vegetarian' THEN 1 END) AS vegetarian_count
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
JOIN pizza_runner.pizza_names p ON c.pizza_id = p.pizza_id
GROUP BY c.customer_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>meatlovers_count</th>
      <th>vegetarian_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>102</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>103</td>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>104</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>105</td>
      <td>0</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**

- Both **customers 101** and **102** placed orders for 2 Meatlovers pizzas and 1 Vegetarian pizza each.
- **Customer 103** ordered 3 Meatlovers pizzas and 1 Vegetarian pizza.
- **Customer 104** ordered a total of 3 Meatlovers pizzas.
- **Customer 105** ordered a single Vegetarian pizza.

___
#### 6. What was the maximum number of pizzas delivered in a single order?


```python
pd.read_sql_query("""
SELECT 
    MAX(counting_pizza.nb_pizza_per_order) AS max_delivered_pizzas
FROM
(
    SELECT c.order_id, COUNT(c.pizza_id) AS nb_pizza_per_order
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    WHERE r.cancellation IS NULL
    GROUP BY c.order_id
) counting_pizza;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>max_delivered_pizzas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
The maximum number of pizzas delivered in a single order is **3**.

___
#### 7. For each customer, how many delivered pizzas had at least 1 change and how many had no changes?


```python
pd.read_sql_query("""
SELECT 
    c.customer_id, 
    CAST(SUM(CASE WHEN c.exclusions != '' OR c.extras != '' THEN 1 ELSE 0 END) AS UNSIGNED) AS has_changes,
    CAST(SUM(CASE WHEN c.exclusions = '' AND c.extras = '' THEN 1 ELSE 0 END) AS UNSIGNED) AS no_changes
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
WHERE r.cancellation IS NULL
GROUP BY customer_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>has_changes</th>
      <th>no_changes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>102</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>103</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>104</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>105</td>
      <td>1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**
- **Customer 103** had the highest number of pizza orders that included either exclusions, extras or both, with a total of 3 deliveries.
- **Customer 101** placed 2 pizza orders without any modifications.
- **Customer 102** placed 3 pizza orders without any modifications.
- **Customer 104** had 2 pizza orders with either exclusions, extras or both, as well as 1 standard pizza.
- **Customer 105** placed 1 pizza order with modifications.

___
#### 8. How many pizzas were delivered that had both exclusions and extras?


```python
pd.read_sql_query("""
SELECT 
    COUNT(*) AS nb_pizzas_with_special_orders
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
WHERE 
    exclusions != '' 
    AND extras != '' 
    AND cancellation IS NULL;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_pizzas_with_special_orders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
There was only **one delivered order** at Pizza Runner that included both exclusions and extras.

___
#### 9. What was the total volume of pizzas ordered for each hour of the day?


```python
pd.read_sql_query("""
SELECT 
    HOUR(order_time) AS hour_of_the_day, 
    COUNT(pizza_id) AS pizzas_count
FROM pizza_runner.customer_orders
GROUP BY hour_of_the_day
ORDER BY hour_of_the_day ASC;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>hour_of_the_day</th>
      <th>pizzas_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>18</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>19</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>21</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>23</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
**Pizza Runner** got the highest number of orders at around 13h, 18h, 21h and 23h, with a total of **3 orders** placed during each of those time periods.

___
#### 10. What was the volume of orders for each day of the week?


```python
pd.read_sql_query("""
SELECT 
    DAYNAME(order_time) AS day_of_the_week,
    COUNT(*) AS pizzas_count
FROM pizza_runner.customer_orders
GROUP BY day_of_the_week
ORDER BY pizzas_count DESC;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>day_of_the_week</th>
      <th>pizzas_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Wednesday</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Saturday</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Thursday</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Friday</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**
- `Wednesday` and `Saturday` were the busiest and most profitable days of the week at **Pizza Runner**, with a total of 5 pizza orders each day in January 2020.
- It was followed by `Thursday`, with a total of 3 orders placed.
- `Friday` was the least busy day, with only 1 pizza order.

<a id="B"></a> 
## B. Runner and Customer Experience

#### 1. How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)


```python
pd.read_sql_query("""
SELECT 
    WEEK(registration_date, 1) + 1 AS nth_week_of_january, 
    COUNT(*) AS signups_per_week
FROM pizza_runner.runners
GROUP BY nth_week_of_january;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nth_week_of_january</th>
      <th>signups_per_week</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- There were 2 registrations in the first week of January 2021.
- Only 1 person signed up as a runner in both week 2 and week 3.

___
#### 2. What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?


```python
pd.read_sql_query("""
SELECT 
    r.runner_id,
    CONCAT(CAST(AVG(TIMESTAMPDIFF(MINUTE, order_time, pickup_time)) AS UNSIGNED), " minutes") AS avg_arrival_time
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
GROUP BY r.runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>avg_arrival_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>15 minutes</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>23 minutes</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>10 minutes</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- **Runner 1** took an average of 15 minutes to arrive at Pizza Runner.
- **Runner 2** took the longest time to arrive at Pizza Runner, with an average of 23 minutes.
- **Runner 3** had the shortest pickup time, with an average of only 10 minutes.

___
#### 3. Is there any relationship between the number of pizzas and how long the order takes to prepare?


```python
pd.read_sql_query("""
WITH order_time_preparation AS 
(
    SELECT 
        c.order_id,
        TIMEDIFF(o.pickup_time, c.order_time) AS prep_time, 
        COUNT(*) OVER (PARTITION BY c.order_id ORDER BY c.order_time) AS nb_pizza
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders o  ON c.order_id = o.order_id
)
SELECT 
    nb_pizza, 
    TIME_FORMAT(AVG(prep_time), '%H:%i:%s') AS avg_prep_time
FROM order_time_preparation
GROUP BY nb_pizza;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_pizza</th>
      <th>avg_prep_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>00:12:21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>00:18:22</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>00:29:17</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
From the query results above, it seems that there is a correlation between the number of pizzas and the preparation time for an order. As the number of pizzas increases, the time taken for order preparation also increases. However, further analysis is needed to confirm this hypothesis, such as conducting a *Linear Regression model*.

___
#### 4. What was the average distance travelled for each customer?


```python
pd.read_sql_query("""
SELECT 
    c.customer_id, 
    CONCAT(FORMAT(AVG(r.distance), 1), " km") AS avg_distance
FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
GROUP BY c.customer_id
ORDER BY c.customer_id ASC;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>avg_distance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101</td>
      <td>20.0 km</td>
    </tr>
    <tr>
      <th>1</th>
      <td>102</td>
      <td>16.7 km</td>
    </tr>
    <tr>
      <th>2</th>
      <td>103</td>
      <td>23.4 km</td>
    </tr>
    <tr>
      <th>3</th>
      <td>104</td>
      <td>10.0 km</td>
    </tr>
    <tr>
      <th>4</th>
      <td>105</td>
      <td>25.0 km</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- The average distance for **customer 101** is 20.0 km.
- The average distance for **customer 102** is 16.7 km.
- The average distance for **customer 103** is 23.4 km.
- **Customer 104** lives closest to Pizza Runner, with an average distance of only 10.0 km.
- **Customer 105** appears to live the farthest from Pizza Runner, with an average distance of 25.0 km.

___
#### 5. What was the difference between the longest and shortest delivery times for all orders?


```python
pd.read_sql_query("""
SELECT 
    CONCAT(MAX(duration)-MIN(duration), " minutes") AS delivery_time_difference
FROM pizza_runner.runner_orders
WHERE cancellation IS NULL;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>delivery_time_difference</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>30 minutes</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
The difference between the longest and shortest delivery times for all orders is `30 minutes`.

___
#### 6. What was the average speed for each runner for each delivery and do you notice any trend for these values?


```python
pd.read_sql_query("""
SELECT 
    order_id,
    runner_id, 
    CONCAT(FORMAT(ROUND(distance/(duration/60), 1), 1), " km/h") AS speed
FROM pizza_runner.runner_orders
WHERE cancellation IS NULL
ORDER BY runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>runner_id</th>
      <th>speed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>37.5 km/h</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>44.4 km/h</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>40.2 km/h</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10</td>
      <td>1</td>
      <td>60.0 km/h</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>2</td>
      <td>35.1 km/h</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>2</td>
      <td>60.0 km/h</td>
    </tr>
    <tr>
      <th>6</th>
      <td>8</td>
      <td>2</td>
      <td>93.6 km/h</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>3</td>
      <td>40.0 km/h</td>
    </tr>
  </tbody>
</table>
</div>



The above table displays the speed of the runner for each order. Therefore, let's determine the average speed of each runner by grouping them by runner.


```python
pd.read_sql_query("""
SELECT 
    runner_speed.runner_id, 
    CONCAT(FORMAT(AVG(runner_speed.speed), 1), " km/h") AS avg_speed
FROM
(
    SELECT runner_id, ROUND(distance/(duration/60), 1) AS speed
    FROM pizza_runner.runner_orders
    WHERE cancellation IS NULL
) runner_speed
GROUP BY runner_speed.runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>avg_speed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>45.5 km/h</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>62.9 km/h</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>40.0 km/h</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can apply directly the `AVG()` on `distance/(duration/60)`.


```python
pd.read_sql_query("""
SELECT 
    runner_id, 
    CONCAT(FORMAT(AVG(distance/(duration/60)), 1), " km/h") AS avg_speed
FROM pizza_runner.runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>avg_speed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>45.5 km/h</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>62.9 km/h</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>40.0 km/h</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- **Runner 1** has an average speed of 45.5 km/h, or between [37.5 - 60.0] km/h.
- **Runner 3** has an average speed of 40.0 km/h.
- **Runner 2** has the highest speed, with an average of 62.9 km/h, or between [35.1 - 93.6] km/h.

___
#### 7. What is the successful delivery percentage for each runner?


```python
pd.read_sql_query("""
SELECT r.runner_id, 
    COUNT(ro.distance) AS n_delivered_orders, 
    COUNT(ro.order_id) AS total_orders,
    CONCAT(ROUND(COUNT(ro.distance)/COUNT(ro.order_id)*100, 1), "%") AS delivery_rate
FROM pizza_runner.runners r
LEFT JOIN pizza_runner.runner_orders ro ON r.runner_id = ro.runner_id
GROUP BY r.runner_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>runner_id</th>
      <th>n_delivered_orders</th>
      <th>total_orders</th>
      <th>delivery_rate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>4</td>
      <td>4</td>
      <td>100.0%</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>4</td>
      <td>75.0%</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>2</td>
      <td>50.0%</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- **Runner 1** has achieved the most impressive performance, with a 100% successful delivery rate for all of his orders.
- **Runner 2** has a 75% success rate for delivering the pizza orders.
- **Runner 3** has a success rate of only 50% for delivering the pizza orders.
- As **Runner 4** has not yet registered, his performance cannot be evaluated at this time.

___
<a id="C"></a> 
## C. Ingredient Optimisation

#### 1. What are the standard ingredients for each pizza?

To approach this problem, we need to join the three tables below in a way that allows us to list the ingredients as strings for each pizza.
- `pizza_names`
- `pizza_recipes`
- `pizza_toppings`


```python
pd.read_sql_query("""
SELECT 
    n.pizza_id, 
    n.pizza_name, 
    r.toppings, 
    LENGTH(r.toppings) - LENGTH(REPLACE(r.toppings, ',', '')) + 1 AS nb_toppings
FROM pizza_runner.pizza_names n
JOIN pizza_runner.pizza_recipes r ON n.pizza_id = r.pizza_id;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>pizza_name</th>
      <th>toppings</th>
      <th>nb_toppings</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Meatlovers</td>
      <td>1, 2, 3, 4, 5, 6, 8, 10</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Vegetarian</td>
      <td>4, 6, 7, 9, 11, 12</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query("""
SELECT *
FROM pizza_runner.pizza_toppings;
""", conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>topping_id</th>
      <th>topping_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>BBQ Sauce</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Beef</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Cheese</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Chicken</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Mushrooms</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Onions</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>Pepperoni</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>Peppers</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>Salami</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>Tomatoes</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>Tomato Sauce</td>
    </tr>
  </tbody>
</table>
</div>



With **PostgreSQL**, there is a handy function called `SPLIT_TO_TABLE` that allows us to split a string into multiple rows or values based on a specified delimiter (e.g., commas). However, there are some limitations when using **MySQL**. Therefore, we need to create manually a table named `complete_pizza_recipes` within the same `pizza_runner` database. This table will match the `pizza_id` to the corresponding `topping_name` based on the `topping_id`.


```python
# Creating the table
mycursor.execute("DROP TABLE IF EXISTS pizza_runner.complete_pizza_recipes; ")

mycursor.execute("""
CREATE TABLE pizza_runner.complete_pizza_recipes
(
    pizza_id INT,
    topping_id INT,
    topping_name VARCHAR(255)
);""")


# Populating the table
mycursor.execute("""
INSERT INTO pizza_runner.complete_pizza_recipes
(pizza_id, topping_id, topping_name)
VALUES
(1, 1, 'Bacon'),
(1,2,  'BBQ Sauce'),
(1,3 , 'Beef'),
(1,4, 'Cheese'),
(1,5, 'Chicken'),
(1, 6, 'Mushrooms'),
(1,8, 'Pepperoni'),
(1,10, 'Salami'),
(2,4, 'Cheese'),
(2,6, 'Mushrooms'),
(2,7, 'Onions'),
(2,9, 'Peppers'),
(2,11, 'Tomatoes'),
(2,12, 'Tomato Sauce');
""")

# Saving 
conn.commit()
```

Here is the new table.


```python
pd.read_sql_query("SELECT * FROM pizza_runner.complete_pizza_recipes;", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>topping_id</th>
      <th>topping_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bacon</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>BBQ Sauce</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>3</td>
      <td>Beef</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>4</td>
      <td>Cheese</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>5</td>
      <td>Chicken</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1</td>
      <td>6</td>
      <td>Mushrooms</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>8</td>
      <td>Pepperoni</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1</td>
      <td>10</td>
      <td>Salami</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2</td>
      <td>4</td>
      <td>Cheese</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>6</td>
      <td>Mushrooms</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2</td>
      <td>7</td>
      <td>Onions</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2</td>
      <td>9</td>
      <td>Peppers</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2</td>
      <td>11</td>
      <td>Tomatoes</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2</td>
      <td>12</td>
      <td>Tomato Sauce</td>
    </tr>
  </tbody>
</table>
</div>



By using `GROUP_CONCAT` and `GROUP BY`, the list of topping names above can be concatenated into a single comma-separated string for each pizza. 


```python
pd.read_sql_query("""
SELECT 
    n.pizza_name, 
    GROUP_CONCAT(cr.topping_name) AS standard_ingredients
FROM pizza_runner.complete_pizza_recipes cr
JOIN pizza_runner.pizza_names n ON cr.pizza_id = n.pizza_id
GROUP BY n.pizza_name;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_name</th>
      <th>standard_ingredients</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Meatlovers</td>
      <td>Bacon,BBQ Sauce,Beef,Cheese,Chicken,Mushrooms,...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Vegetarian</td>
      <td>Cheese,Mushrooms,Onions,Peppers,Tomatoes,Tomat...</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 2. What was the most commonly added extra?

In the following query, we are splitting up the added toppings from the `extras` column into separate columns: `extra1` and `extra2`.


```python
pd.read_sql_query("""
SELECT 
    *, 
    CAST(SUBSTRING_INDEX(extras, ',', 1) AS UNSIGNED) AS extra1,
    CASE WHEN LENGTH(REPLACE(REPLACE(extras, ',', ''), ' ', '')) > 1 
        THEN SUBSTRING_INDEX(extras, ',', -1) 
        ELSE 0 
    END AS extra2

FROM pizza_runner.customer_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>order_time</th>
      <th>extra1</th>
      <th>extra2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 18:05:02</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 19:00:52</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:00:29</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-08 21:03:13</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-09 23:54:33</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>2020-01-10 11:22:59</td>
      <td>1</td>
      <td>5</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-11 18:34:49</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2020-01-11 18:34:49</td>
      <td>1</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



Now, we can count the number of added toppings in the delivered pizza orders.


```python
query2 = """
WITH pizza_extras AS
(
    SELECT 
        extras,
        CAST(SUBSTRING_INDEX(extras, ',', 1) AS UNSIGNED) AS extra1,
        CASE WHEN LENGTH(REPLACE(REPLACE(extras, ',', ''), ' ', '')) > 1 
            THEN SUBSTRING_INDEX(extras, ',', -1) 
            ELSE 0 
        END AS extra2
    FROM pizza_runner.customer_orders
),

extras_occurence AS(
    SELECT extra, CAST(SUM(occurence) AS UNSIGNED) AS total_occurence
    FROM (
        SELECT extra1 AS extra, COUNT(extra1) AS occurence
        FROM pizza_extras
        WHERE extra1 != 0
        GROUP BY extra1 

        UNION ALL
    
        SELECT extra2 AS extra, COUNT(extra2) AS occurence
        FROM pizza_extras
        WHERE extra2 != 0
        GROUP BY extra2
    ) subquery
    GROUP BY extra
)
SELECT DISTINCT o.extra, cr.topping_name, o.total_occurence
FROM extras_occurence o
JOIN pizza_runner.complete_pizza_recipes cr ON o.extra = cr.topping_id
ORDER BY o.total_occurence DESC;
"""
df2 = pd.read_sql_query(query2, conn)
df2
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>extra</th>
      <th>topping_name</th>
      <th>total_occurence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>5</td>
      <td>Chicken</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>Cheese</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can extract the most commonly added topping by adding the statement `LIMIT 1` to the above query.


```python
pd.read_sql_query(query2.replace(";", "") + "LIMIT 1", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>extra</th>
      <th>topping_name</th>
      <th>total_occurence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**
- The most frequently added topping is `Bacon`, with a total quantity of 4. 
- It is followed by `Chicken` and `Cheese`.

___
#### 3. What was the most common exclusion?

In the following query, we are splitting up the excluded toppings from the `exclusions` column into separate columns: `exclusion1` and `exclusion2`.


```python
pd.read_sql_query("""
SELECT 
    *,
    CAST(SUBSTRING_INDEX(exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
    CASE WHEN LENGTH(REPLACE(REPLACE(exclusions, ',', ''), ' ', '')) > 1 
        THEN SUBSTRING_INDEX(exclusions, ',', -1) 
        ELSE 0 
    END AS exclusion2

FROM pizza_runner.customer_orders;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>order_time</th>
      <th>exclusion1</th>
      <th>exclusion2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 18:05:02</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-01 19:00:52</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-02 23:51:23</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>2020-01-04 13:23:46</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:00:29</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>2020-01-08 21:03:13</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td></td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-09 23:54:33</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>2020-01-10 11:22:59</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>2020-01-11 18:34:49</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2020-01-11 18:34:49</td>
      <td>2</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python
query3 = """
WITH pizza_exclusions AS
(
    SELECT 
        exclusions,
        CAST(SUBSTRING_INDEX(exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
        CASE WHEN LENGTH(REPLACE(REPLACE(exclusions, ',', ''), ' ', '')) > 1 
            THEN SUBSTRING_INDEX(exclusions, ',', -1) 
            ELSE 0 
        END AS exclusion2
    FROM pizza_runner.customer_orders
),
exclusion_occurrence AS
(
    SELECT exclusion, CAST(SUM(occurrence) AS UNSIGNED) AS total_occurrence
    FROM (
        SELECT exclusion1 AS exclusion, COUNT(exclusion1) AS occurrence
        FROM pizza_exclusions
        WHERE exclusion1 != 0
        GROUP BY exclusion1
        
        UNION ALL
        
        SELECT exclusion2 AS exclusion, COUNT(exclusion2) AS occurrence
        FROM pizza_exclusions
        WHERE exclusion2 != 0
        GROUP BY exclusion2
    ) subquery
    GROUP BY exclusion
)
SELECT DISTINCT exclusion, cr.topping_name, o.total_occurrence
FROM exclusion_occurrence o
JOIN pizza_runner.complete_pizza_recipes cr ON o.exclusion = cr.topping_id
ORDER BY o.total_occurrence DESC;
"""
df3 = pd.read_sql_query(query3,conn)
df3
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>exclusion</th>
      <th>topping_name</th>
      <th>total_occurrence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>Cheese</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>BBQ Sauce</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>6</td>
      <td>Mushrooms</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can extract the most commonly excluded topping by adding the statement `LIMIT 1` to the above query.


```python
pd.read_sql_query(query3.replace(";", "") + "LIMIT 1", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>exclusion</th>
      <th>topping_name</th>
      <th>total_occurrence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>Cheese</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
Hence, the most common exclusion is `Cheese`.

___
#### 4. Generate an order item for each record in the customers_orders table in the format of one of the following:
- Meat Lovers
- Meat Lovers - Exclude Beef
- Meat Lovers - Extra Bacon
- Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers

**How to approach this problem:**
1. Split the `exclusions` and `extras` IDs into individual columns
2. Map the `pizza_id` to the corresponding `pizza_name` for each order
3. Add a column that holds the keyword `- Exclude ` and `- Extra `
4. Map each topping ID extracted from exclusions and extras (from step 1) to the corresponding `topping_name`
5. Concatenate all string columns 

**In this following query, we are:** 
- splitting up `exclusions` and `extras` into individual columns
- mapping each `pizza_id` to its respective `pizza_name`
- creating columns to hold the keyword ` - Exclude ` and ` - Extra `


```python
pd.read_sql_query("""
SELECT 
    c.order_id,
    c.pizza_id, 
    c.exclusions, 
    c.extras, 
    
    -- extract toppings from exclusions
    CAST(SUBSTRING_INDEX(c.exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
    CASE WHEN LENGTH(REPLACE(REPLACE(c.exclusions, ',',''), ' ','')) > 1 THEN SUBSTRING_INDEX(c.exclusions, ',', -1) ELSE 0 END AS exclusion2,
    
    -- extract toppings from extras
    CAST(SUBSTRING_INDEX(c.extras, ',', 1) AS UNSIGNED) AS extra1,
    CASE WHEN LENGTH(REPLACE(REPLACE(c.extras, ',',''), ' ','')) > 1 THEN SUBSTRING_INDEX(c.extras, ',', -1) ELSE 0 END AS extra2,
    
    -- create string columns
    n.pizza_name,
    CASE WHEN c.exclusions != '' THEN '- Exclude ' ELSE '' END AS exclude_keyword,
    CASE WHEN c.extras != '' THEN '- Extras ' ELSE '' END AS extra_keyword

FROM pizza_runner.customer_orders c
JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>pizza_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>exclusion1</th>
      <th>exclusion2</th>
      <th>extra1</th>
      <th>extra2</th>
      <th>pizza_name</th>
      <th>exclude_keyword</th>
      <th>extra_keyword</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Vegetarian</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>1</td>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td></td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>2</td>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Vegetarian</td>
      <td>- Exclude</td>
      <td></td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>1</td>
      <td></td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td>- Extras</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>2</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Vegetarian</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>2</td>
      <td></td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>Vegetarian</td>
      <td></td>
      <td>- Extras</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>1</td>
      <td>4</td>
      <td>1, 5</td>
      <td>4</td>
      <td>0</td>
      <td>1</td>
      <td>5</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>- Extras</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>1</td>
      <td></td>
      <td></td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>1</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2</td>
      <td>6</td>
      <td>1</td>
      <td>4</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>- Extras</td>
    </tr>
  </tbody>
</table>
</div>



Now, we need to map the `exclusion1`, `exclusion2`, `extra1`, and `extra2` to their respective topping names.

Note that **user-defined functions** cannot be used directly inside the `pd.read_sql_query()`. To access the function through Jupyter Notebook, we need to use `mycursor.execute()`, which will return the value(s). Therefore, please bear with me if the query below (combined with CTEs) appears lengthy.


```python
pd.read_sql_query("""
WITH pizza_orders_str AS
(
    SELECT 
        c.order_id,
        c.pizza_id, 
        c.exclusions, 
        c.extras, 
        n.pizza_name,
    
        -- extract toppings from exclusions
        CASE WHEN c.exclusions != '' THEN '- Exclude ' ELSE '' END AS exclude_keyword,
        CAST(SUBSTRING_INDEX(c.exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.exclusions, ',',''), ' ','')) > 1 
            THEN SUBSTRING_INDEX(c.exclusions, ',', -1) 
            ELSE 0 
        END AS exclusion2,
        
        -- extract toppings from extras
        CASE WHEN c.extras != '' THEN '- Extras ' ELSE '' END AS extra_keyword,
        CAST(SUBSTRING_INDEX(c.extras, ',', 1) AS UNSIGNED) AS extra1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.extras, ',',''), ' ','')) > 1 
            THEN SUBSTRING_INDEX(c.extras, ',', -1) 
            ELSE 0 
        END AS extra2

    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
)

-- mapping the exclusion and extra to its respective topping name
SELECT 
    order_id, 
    exclusions, 
    extras, 
    pizza_name, 
    
    -- Exclusions
    exclude_keyword,
    CASE WHEN exclusion1 != 0 THEN (SELECT DISTINCT topping_name
                                    FROM pizza_runner.complete_pizza_recipes 
                                    WHERE topping_id = exclusion1) ELSE '' END AS exclusion1_name,
                                
    CASE WHEN exclusion2 != 0 THEN (SELECT DISTINCT topping_name
                                    FROM pizza_runner.complete_pizza_recipes 
                                    WHERE topping_id = exclusion2) ELSE '' END AS exclusion2_name, 

    -- Extras
    extra_keyword,
    CASE WHEN extra1 != 0 THEN (SELECT DISTINCT topping_name
                                FROM pizza_runner.complete_pizza_recipes 
                                WHERE topping_id = extra1) ELSE '' END AS extra1_name,
                                
    CASE WHEN extra2 != 0 THEN (SELECT DISTINCT topping_name
                                FROM pizza_runner.complete_pizza_recipes 
                                WHERE topping_id = extra2) ELSE '' END AS extra2_name
FROM pizza_orders_str;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>pizza_name</th>
      <th>exclude_keyword</th>
      <th>exclusion1_name</th>
      <th>exclusion2_name</th>
      <th>extra_keyword</th>
      <th>extra1_name</th>
      <th>extra2_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td></td>
      <td></td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td></td>
      <td></td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td></td>
      <td></td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td></td>
      <td></td>
      <td>Vegetarian</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>4</td>
      <td></td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>Cheese</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>4</td>
      <td></td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>Cheese</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>4</td>
      <td></td>
      <td>Vegetarian</td>
      <td>- Exclude</td>
      <td>Cheese</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td></td>
      <td>1</td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td>- Extras</td>
      <td>Bacon</td>
      <td></td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td></td>
      <td></td>
      <td>Vegetarian</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td></td>
      <td>1</td>
      <td>Vegetarian</td>
      <td></td>
      <td></td>
      <td></td>
      <td>- Extras</td>
      <td>Bacon</td>
      <td></td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td></td>
      <td></td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>4</td>
      <td>1, 5</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>Cheese</td>
      <td></td>
      <td>- Extras</td>
      <td>Bacon</td>
      <td>Chicken</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td></td>
      <td></td>
      <td>Meatlovers</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>Meatlovers</td>
      <td>- Exclude</td>
      <td>BBQ Sauce</td>
      <td>Mushrooms</td>
      <td>- Extras</td>
      <td>Bacon</td>
      <td>Cheese</td>
    </tr>
  </tbody>
</table>
</div>



Finally, we can **concatenate** all the string columns into a single string for each pizza order by using 2 CTEs and `CONCAT()` function. The CTEs are:
- `extracting_toppings`
- `orders_in_string`


```python
pd.read_sql_query("""
WITH extracting_toppings AS
(
    SELECT 
        c.order_id,
        c.customer_id,
        c.pizza_id, 
        c.exclusions, 
        c.extras, 
        n.pizza_name,
    
        -- extract toppings from exclusions
        CASE WHEN exclusions != '' THEN ' - Exclude ' ELSE '' END AS exclude_keyword,
        CAST(SUBSTRING_INDEX(c.exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.exclusions, ',',''), ' ','')) > 1 
                THEN SUBSTRING_INDEX(c.exclusions, ',', -1) 
                ELSE 0 
        END AS exclusion2,
    
        -- extract toppings from extras
        CASE WHEN extras != '' THEN ' - Extras ' ELSE '' END AS extra_keyword,
        CAST(SUBSTRING_INDEX(c.extras, ',', 1) AS UNSIGNED) AS extra1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.extras, ',',''), ' ','')) > 1 
                THEN SUBSTRING_INDEX(c.extras, ',', -1) 
                ELSE 0 
        END AS extra2

    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
),
orders_in_string AS
(
    SELECT 
        order_id, 
        customer_id,
        exclusions, 
        extras, 
        pizza_id,
        pizza_name, 

        exclude_keyword, 
        CASE WHEN exclusion1 != 0 THEN (SELECT DISTINCT topping_name
                                        FROM pizza_runner.complete_pizza_recipes 
                                        WHERE topping_id = exclusion1) ELSE '' END AS exclusion1_name,
                                
        CASE WHEN exclusion2 != 0 THEN CONCAT(', ', (SELECT DISTINCT topping_name
                                                    FROM pizza_runner.complete_pizza_recipes 
                                                    WHERE topping_id = exclusion2)) ELSE '' END AS exclusion2_name, 
        extra_keyword,                            
        CASE WHEN extra1 != 0 THEN (SELECT DISTINCT topping_name
                                    FROM pizza_runner.complete_pizza_recipes 
                                    WHERE topping_id = extra1) ELSE '' END AS extra1_name,
                                
        CASE WHEN extra2 != 0 THEN CONCAT(', ', (SELECT DISTINCT topping_name
                                                FROM pizza_runner.complete_pizza_recipes 
                                                WHERE topping_id = extra2)) ELSE '' END AS extra2_name
    FROM extracting_toppings
)
SELECT 
    order_id, customer_id, 
    CONCAT(pizza_name, exclude_keyword, exclusion1_name, exclusion2_name, extra_keyword, extra1_name, extra2_name) AS orders
FROM orders_in_string;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>orders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>Vegetarian</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>Meatlovers - Exclude Cheese</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>Meatlovers - Exclude Cheese</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>Vegetarian - Exclude Cheese</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>Meatlovers - Extras Bacon</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>Vegetarian</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>Vegetarian - Extras Bacon</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>Meatlovers - Exclude Cheese - Extras Bacon, Ch...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>Meatlovers</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>Meatlovers - Exclude BBQ Sauce, Mushrooms - Ex...</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. Generate an alphabetically ordered comma separated ingredient list for each pizza order from the `customer_orders` table and add a 2x in front of any relevant ingredients
- For example: "Meat Lovers: 2xBacon, Beef, ... , Salami"

The query below is the `alphabetically_ordered_recipes` CTE that is used later.


```python
pd.read_sql_query("""
SELECT 
    pizza_id, 
    GROUP_CONCAT(topping_name ORDER BY topping_name) AS toppings
FROM pizza_runner.complete_pizza_recipes
GROUP BY pizza_id;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>toppings</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon,BBQ Sauce,Beef,Cheese,Chicken,Mushrooms,...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Cheese,Mushrooms,Onions,Peppers,Tomato Sauce,T...</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query("""
WITH alphabetically_ordered_recipes AS
(
    SELECT 
        pizza_id, 
        GROUP_CONCAT(topping_name ORDER BY topping_name) AS toppings
    FROM pizza_runner.complete_pizza_recipes
    GROUP BY pizza_id
)
SELECT 
    c.order_id,
    c.customer_id,
    CASE 
        WHEN c.exclusions = '2, 6' AND c.extras = '1, 4' AND c.pizza_id = '1'
        THEN CONCAT(n.pizza_name, ': ', 'x2Bacon,Beef,x2Cheese,Chicken,Pepperoni,Salami')
        
        WHEN c.exclusions = '4' AND c.extras = '1, 5' AND c.pizza_id = '1'
        THEN CONCAT(n.pizza_name, ': ', 'x2Bacon,BBQ Sauce,Beef,x2Chicken,Mushrooms,Pepperoni,Salami')
        
        WHEN c.exclusions = '4' AND c.pizza_id = 1 
        THEN CONCAT(n.pizza_name, ': ', 'Bacon,BBQ Sauce,Beef,Chicken,Mushrooms,Pepperoni,Salami')
        
        WHEN c.exclusions = '4' AND c.pizza_id = 2 
        THEN CONCAT(n.pizza_name, ': ', 'Mushrooms,Onions,Peppers,Tomato Sauce,Tomatoes')
        
        WHEN c.extras = '1' AND c.pizza_id = 1 
        THEN CONCAT(n.pizza_name, ': ', 'x2Bacon,BBQ Sauce,Beef,Cheese,Chicken,Mushrooms,Pepperoni,Salami')
 
        WHEN c.extras = '1' AND c.pizza_id = 2 
        THEN CONCAT(n.pizza_name, ': ', 'Bacon,Cheese,Mushrooms,Onions,Peppers,Tomato Sauce,Tomatoes')
        
        ELSE CONCAT(n.pizza_name, ': ', a.toppings)
    END AS list_of_ingredients
FROM pizza_runner.customer_orders c
JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
JOIN alphabetically_ordered_recipes a ON a.pizza_id = c.pizza_id;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>list_of_ingredients</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Cheese,Chicke...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Cheese,Chicke...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Cheese,Chicke...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>102</td>
      <td>Vegetarian: Cheese,Mushrooms,Onions,Peppers,To...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>103</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Chicken,Mushr...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>103</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Chicken,Mushr...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>103</td>
      <td>Vegetarian: Mushrooms,Onions,Peppers,Tomato Sa...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>104</td>
      <td>Meatlovers: x2Bacon,BBQ Sauce,Beef,Cheese,Chic...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>101</td>
      <td>Vegetarian: Cheese,Mushrooms,Onions,Peppers,To...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>105</td>
      <td>Vegetarian: Bacon,Cheese,Mushrooms,Onions,Pepp...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>102</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Cheese,Chicke...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>9</td>
      <td>103</td>
      <td>Meatlovers: x2Bacon,BBQ Sauce,Beef,x2Chicken,M...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>10</td>
      <td>104</td>
      <td>Meatlovers: Bacon,BBQ Sauce,Beef,Cheese,Chicke...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>104</td>
      <td>Meatlovers: x2Bacon,Beef,x2Cheese,Chicken,Pepp...</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 6. What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?

I created a `Temporary Table` to store the quantity of each topping used in all delivered pizzas, without considering the `excluded` and `extras` toppings. This approach of using a Temporary Table helps to simplify the query later, which is already combined with 3 **Common Table Expressions (CTEs)**.


```python
# Define query for total quantity of each ingredient used in all delivered pizzas (with no changes)
query_QtyIngredientsUsed = """
WITH nb_deliveries_per_pizza AS
(
    SELECT 
        p.pizza_id, 
        COUNT(p.pizza_id) AS nb_orders
    FROM (
        -- select data with successful delivered pizzas
        SELECT c.order_id, c.pizza_id
        FROM pizza_runner.customer_orders c
        JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
        WHERE r.cancellation IS NULL
        ) p
    GROUP BY p.pizza_id
)
SELECT 
    topping_id, 
    topping_name, 
    CAST(SUM(nb_orders) AS UNSIGNED) AS quantity
FROM pizza_runner.complete_pizza_recipes cr
JOIN nb_deliveries_per_pizza do ON cr.pizza_id = do.pizza_id
GROUP BY topping_id, topping_name
ORDER BY quantity DESC, topping_name ASC;
"""

# Create Temporary Table 
mycursor.execute("DROP TEMPORARY TABLE IF EXISTS qty_ingredients_used")
mycursor.execute("CREATE TEMPORARY TABLE qty_ingredients_used AS " + query_QtyIngredientsUsed)

# Display the total quantity of each ingredient used
pd.read_sql_query(query_QtyIngredientsUsed, conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>topping_id</th>
      <th>topping_name</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>Cheese</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>6</td>
      <td>Mushrooms</td>
      <td>12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>Bacon</td>
      <td>9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>BBQ Sauce</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>Beef</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>Chicken</td>
      <td>9</td>
    </tr>
    <tr>
      <th>6</th>
      <td>8</td>
      <td>Pepperoni</td>
      <td>9</td>
    </tr>
    <tr>
      <th>7</th>
      <td>10</td>
      <td>Salami</td>
      <td>9</td>
    </tr>
    <tr>
      <th>8</th>
      <td>7</td>
      <td>Onions</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
      <td>Peppers</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>12</td>
      <td>Tomato Sauce</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>11</td>
      <td>Tomatoes</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



Now, we need to determine the **precise quantity** of each topping used in each delivered pizza, taking into account the `excluded` and `extra` toppings.

To accomplish this, we need to revisit the exact same query used in the previous problem to split the `exclusions` and `extras` into separate columns. <br>
With this information, we can 
- **_subtract_** from the **standard quantity** if it is an `exclusion` or 
- **_add_** to the **standard quantity** for `extras`.


```python
# Recall the query for extracting the topping_id from "exclusions" and "extras"
pd.read_sql_query("""
SELECT 
    c.exclusions, 
    c.extras,
    
    -- extract toppings from exclusions
    CAST(SUBSTRING_INDEX(c.exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
    CASE WHEN LENGTH(REPLACE(REPLACE(c.exclusions, ',',''), ' ','')) > 1 
        THEN SUBSTRING_INDEX(c.exclusions, ',', -1) 
        ELSE 0 
        END AS exclusion2,
        
    -- extract toppings from extras
    CAST(SUBSTRING_INDEX(c.extras, ',', 1) AS UNSIGNED) AS extra1,
    CASE WHEN LENGTH(REPLACE(REPLACE(c.extras, ',',''), ' ','')) > 1 
        THEN SUBSTRING_INDEX(c.extras, ',', -1) 
        ELSE 0 
        END AS extra2

FROM pizza_runner.customer_orders c
JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
WHERE r.cancellation IS NULL AND (c.exclusions != 0 OR c.extras != 0);
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>exclusions</th>
      <th>extras</th>
      <th>exclusion1</th>
      <th>exclusion2</th>
      <th>extra1</th>
      <th>extra2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td></td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td></td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td></td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2, 6</td>
      <td>1, 4</td>
      <td>2</td>
      <td>6</td>
      <td>1</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



Finally, I combined the Temporary Table `qty_ingredients_used` with three CTEs to calculate the total quantity of each topping used in all delivered pizzas.
- `extracting_toppings_cte`: This CTE consists of the exact code used in the previous problem to split the `exclusions` and `extras` into separate topping ID columns.

- `exclusions_count_cte`: This CTE determines the number of times each corresponding topping is excluded from the delivered pizza.

- `extras_count_cte`: This CTE identifies the number of times each corresponding topping is added to the delivered pizza.


```python
pd.read_sql_query("""
WITH extracting_toppings_cte AS
(
    SELECT 
        c.exclusions,
        c.extras,
    
        -- extract toppings from "exclusions" string
        CAST(SUBSTRING_INDEX(c.exclusions, ',', 1) AS UNSIGNED) AS exclusion1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.exclusions, ',',''), ' ','')) > 1 
            THEN SUBSTRING_INDEX(c.exclusions, ',', -1) 
            ELSE 0 
        END AS exclusion2,
    
        -- extract toppings from "extras" string
        CAST(SUBSTRING_INDEX(c.extras, ',', 1) AS UNSIGNED) AS extra1,
        CASE WHEN LENGTH(REPLACE(REPLACE(c.extras, ',',''), ' ','')) > 1 
            THEN SUBSTRING_INDEX(c.extras, ',', -1) 
            ELSE 0 
        END AS extra2
    
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    WHERE r.cancellation IS NULL 
), 

exclusions_count_cte AS
(
    -- This CTE counts the number of excluded toppings along with its ID
    
    SELECT 
        exclusion1 AS exclusion_id, 
        COUNT(exclusion1) AS exclusions
    FROM extracting_toppings_cte
    GROUP BY exclusion1

    UNION ALL 

    SELECT 
        exclusion2 AS exclusion_id,
        COUNT(exclusion2) AS exclusions
    FROM extracting_toppings_cte
    GROUP BY exclusion2
),

extras_count_cte AS
(
    -- This CTE counts the number of added toppings along with its ID
    
    SELECT 
        extra1 AS extra_id, 
        COUNT(extra1) AS extras
    FROM extracting_toppings_cte
    GROUP BY extra1

    UNION ALL

    SELECT 
        extra2 AS extra_id, 
        COUNT(extra2) AS extras
    FROM extracting_toppings_cte
    GROUP BY extra2
)
SELECT
    Qty.topping_id, 
    Qty.topping_name, 
    Qty.quantity, 
    FORMAT(e1.exclusions, 0) AS exclusions, 
    FORMAT(e2.extras, 0) AS extras,
    CASE 
        WHEN e1.exclusions IS NULL AND e2.extras IS NOT NULL THEN Qty.quantity + e2.extras
        WHEN e1.exclusions IS NOT NULL AND e2.extras IS NULL THEN Qty.quantity - e1.exclusions
        WHEN e1.exclusions IS NOT NULL AND e2.extras IS NOT NULL THEN Qty.quantity - e1.exclusions + e2.extras        
        ELSE Qty.quantity
    END AS total_quantity
    
FROM qty_ingredients_used Qty
LEFT JOIN exclusions_count_cte e1 ON Qty.topping_id = e1.exclusion_id
LEFT JOIN extras_count_cte e2 ON Qty.topping_id = e2.extra_id
ORDER BY total_quantity DESC
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>topping_id</th>
      <th>topping_name</th>
      <th>quantity</th>
      <th>exclusions</th>
      <th>extras</th>
      <th>total_quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bacon</td>
      <td>9</td>
      <td>None</td>
      <td>3</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>6</td>
      <td>Mushrooms</td>
      <td>12</td>
      <td>1</td>
      <td>None</td>
      <td>11</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>Cheese</td>
      <td>12</td>
      <td>3</td>
      <td>1</td>
      <td>10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Beef</td>
      <td>9</td>
      <td>None</td>
      <td>None</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Chicken</td>
      <td>9</td>
      <td>None</td>
      <td>None</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>8</td>
      <td>Pepperoni</td>
      <td>9</td>
      <td>None</td>
      <td>None</td>
      <td>9</td>
    </tr>
    <tr>
      <th>6</th>
      <td>10</td>
      <td>Salami</td>
      <td>9</td>
      <td>None</td>
      <td>None</td>
      <td>9</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2</td>
      <td>BBQ Sauce</td>
      <td>9</td>
      <td>1</td>
      <td>None</td>
      <td>8</td>
    </tr>
    <tr>
      <th>8</th>
      <td>7</td>
      <td>Onions</td>
      <td>3</td>
      <td>None</td>
      <td>None</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
      <td>Peppers</td>
      <td>3</td>
      <td>None</td>
      <td>None</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>12</td>
      <td>Tomato Sauce</td>
      <td>3</td>
      <td>None</td>
      <td>None</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>11</td>
      <td>Tomatoes</td>
      <td>3</td>
      <td>None</td>
      <td>None</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**Result**:
- `Bacon` is the most commonly used ingredient at Pizza Runner, with a total quantity of 12.
- It is followed by `Mushrooms` and `Cheese` as the second and third most used ingredients, with a total quantity of 11 and 10 respectively.
- `Beef`, `Chicken`, `Pepperoni`, and `Salami` were used 9 times each, while `BBQ sauce` were used 8 times.
- `Onions`, `Peppers`, `Tomato Sauce`, and `Tomatoes` are the least used toppings, with a quantity of only 3 each.


```python
# Drop Temporary Table
mycursor.execute("DROP TEMPORARY TABLE IF EXISTS qty_ingredients_used")
conn.commit()
```

___
<a id="D"></a> 
## D. Pricing and Ratings

#### 1. If a Meat Lovers pizza costs \\$12 and Vegetarian costs \\$10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?


```python
pd.read_sql_query("""
WITH PizzaRunner_Sales AS
(
    SELECT 
        c.order_id, 
        c.pizza_id, 
        n.pizza_name, 
        CASE WHEN n.pizza_name = 'Meatlovers' THEN 12 ELSE 10 END AS price
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
    WHERE r.cancellation IS NULL
)
SELECT CONCAT("$", FORMAT(SUM(price), 2)) AS total_sales
FROM PizzaRunner_Sales;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>$138.00</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
**Pizza Runner** made a total of `$138.00`.

___
#### 2. What if there was an additional \$1 charge for any pizza extras?
- **Add cheese is \$1 extra**


```python
pd.read_sql_query("""
WITH PizzaRunner_Sales AS
(
    SELECT 
        c.pizza_id,
        c.extras, 
        n.pizza_name,
        CASE WHEN n.pizza_name = 'Meatlovers' THEN 12 ELSE 10 END AS price,
        CASE 
            WHEN LENGTH(REPLACE(REPLACE(extras, ',', ''), ' ', '')) > 0 
            THEN LENGTH(REPLACE(REPLACE(extras, ',', ''), ' ', '')) 
            ELSE 0 
        END AS extras_price,
        CASE WHEN pt.topping_id IS NOT NULL THEN 1 END AS has_cheese
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
    LEFT JOIN pizza_runner.pizza_toppings pt ON c.extras LIKE CONCAT('%', pt.topping_id, '%') AND pt.topping_name = 'Cheese'
    WHERE r.cancellation IS NULL
)
SELECT CONCAT("$ ", FORMAT(SUM(price) + SUM(extras_price) + SUM(has_cheese), 2)) AS total_sales
FROM PizzaRunner_Sales;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>$ 143.00</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
**Pizza Runner** made `$143.00` for charging extra $1 on extras

___
#### 3. The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, how would you design an additional table for this new dataset - generate a schema for this new table and insert your own data for ratings for each successful customer order between 1 to 5.


```python
# Create the table
mycursor.execute("DROP TABLE IF EXISTS pizza_runner.rating_system;")

mycursor.execute("""
CREATE TABLE pizza_runner.rating_system(
order_id INT DEFAULT NULL,
customer_id INT DEFAULT NULL,
runner_id INT DEFAULT NULL,
rating INT DEFAULT NULL
)""")

# Populate the table
mycursor.execute("""
INSERT INTO pizza_runner.rating_system
(order_id, customer_id, runner_id, rating)
VALUES
(1, 101, 1, 4),
(2, 101, 1, 5),
(3, 102, 1, 4),
(4, 103, 2, 2),
(5, 104, 3, 3),
(7, 105, 2, 1),
(8, 102, 2, 4),
(10, 104, 1, 5)
""")

# Saving the updated database
conn.commit()

# Display the table
pd.read_sql_query("""
SELECT *
FROM pizza_runner.rating_system
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>runner_id</th>
      <th>rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>101</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>101</td>
      <td>1</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>102</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>103</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>104</td>
      <td>3</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>105</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>8</td>
      <td>102</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>10</td>
      <td>104</td>
      <td>1</td>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 4. Using your newly generated table - can you join all of the information together to form a table which has the following information for successful deliveries?
- customer_id
- order_id
- runner_id
- rating
- order_time
- pickup_time
- Time between order and pickup
- Delivery duration
- Average speed
- Total number of pizzas

To find the total number of pizzas per order, I will use the **Window Functions** `COUNT()` and `ROW_NUMBER()`.


```python
pd.read_sql_query("""
SELECT 
    sq1.customer_id, 
    sq1.order_id, 
    sq1.runner_id, 
    s.rating,
    sq1.order_time, 
    sq1.pickup_time,
    sq1.order_pickup_duration,
    sq1.duration,
    sq1.avg_speed,
    sq1.counting AS total_orders
FROM pizza_runner.rating_system s
JOIN 
(
    -- Select delivered orders along with their total number of pizzas per order
    -- Window functions are required to determine the total number of pizzas
    
    SELECT 
        c.customer_id, 
        c.order_id, 
        r.runner_id, 
        c.order_time, 
        r.pickup_time,
        TIME_FORMAT(TIMEDIFF(r.pickup_time, c.order_time), '%H:%i:%s') AS order_pickup_duration,
        r.duration,
        ROUND(r.distance/(r.duration/60), 1) AS avg_speed,
        COUNT(c.pizza_id) OVER (PARTITION BY c.order_id) AS counting,
        ROW_NUMBER() OVER (PARTITION BY c.order_id) AS rk
    FROM pizza_runner.customer_orders c 
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    WHERE r.cancellation IS NULL
) sq1
ON s.order_id = sq1.order_id
WHERE sq1.rk = 1
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>runner_id</th>
      <th>rating</th>
      <th>order_time</th>
      <th>pickup_time</th>
      <th>order_pickup_duration</th>
      <th>duration</th>
      <th>avg_speed</th>
      <th>total_orders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>2020-01-01 18:05:02</td>
      <td>2020-01-01 18:15:34</td>
      <td>00:10:32</td>
      <td>32</td>
      <td>37.5</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>101</td>
      <td>2</td>
      <td>1</td>
      <td>5</td>
      <td>2020-01-01 19:00:52</td>
      <td>2020-01-01 19:10:54</td>
      <td>00:10:02</td>
      <td>27</td>
      <td>44.4</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>102</td>
      <td>3</td>
      <td>1</td>
      <td>4</td>
      <td>2020-01-02 23:51:23</td>
      <td>2020-01-03 00:12:37</td>
      <td>00:21:14</td>
      <td>20</td>
      <td>40.2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>103</td>
      <td>4</td>
      <td>2</td>
      <td>2</td>
      <td>2020-01-04 13:23:46</td>
      <td>2020-01-04 13:53:03</td>
      <td>00:29:17</td>
      <td>40</td>
      <td>35.1</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>104</td>
      <td>5</td>
      <td>3</td>
      <td>3</td>
      <td>2020-01-08 21:00:29</td>
      <td>2020-01-08 21:10:57</td>
      <td>00:10:28</td>
      <td>15</td>
      <td>40.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>105</td>
      <td>7</td>
      <td>2</td>
      <td>1</td>
      <td>2020-01-08 21:20:29</td>
      <td>2020-01-08 21:30:45</td>
      <td>00:10:16</td>
      <td>25</td>
      <td>60.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>102</td>
      <td>8</td>
      <td>2</td>
      <td>4</td>
      <td>2020-01-09 23:54:33</td>
      <td>2020-01-10 00:15:02</td>
      <td>00:20:29</td>
      <td>15</td>
      <td>93.6</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>104</td>
      <td>10</td>
      <td>1</td>
      <td>5</td>
      <td>2020-01-11 18:34:49</td>
      <td>2020-01-11 18:50:20</td>
      <td>00:15:31</td>
      <td>10</td>
      <td>60.0</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. If a Meat Lovers pizza was \\$12 and Vegetarian \\$10 fixed prices with no cost for extras and each runner is paid \\$0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?


```python
pd.read_sql_query("""
WITH pizza_income_cte AS 
(
    SELECT 
        c.order_id, 
        r.runner_id, 
        r.distance, 
        n.pizza_name, 
        CASE WHEN n.pizza_name = 'Meatlovers' THEN 12 ELSE 10 END AS pizza_price
    FROM pizza_runner.customer_orders c
    JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
    JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
    WHERE r.cancellation IS NULL
),
pizza_income_per_order_cte AS 
(
    SELECT 
        SUM(pizza_price) AS sales_per_order, 
        AVG(distance)*0.30 AS runner_payment
    FROM pizza_income_cte
    GROUP BY order_id
)
SELECT CONCAT("$ ", FORMAT(SUM(sales_per_order) - SUM(runner_payment),2)) AS Profit
FROM pizza_income_per_order_cte;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>$ 94.44</td>
    </tr>
  </tbody>
</table>
</div>



**Result:**\
**Pizza Runner** achieved a profit of `$94.44` after accounting for delivery expenses.

___
<a id="E"></a> 
## E. Bonus Questions

**If Danny wants to expand his range of pizzas - how would this impact the existing data design?\
Write an `INSERT` statement to demonstrate what would happen if a new Supreme pizza with all the toppings was added to the Pizza Runner menu?** 

Here is the original menu of Pizza Runner restaurant.


```python
pd.read_sql_query("""
SELECT r.pizza_id, n.pizza_name, r.toppings
FROM pizza_runner.pizza_recipes r
JOIN pizza_runner.pizza_names n ON r.pizza_id = n.pizza_id;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>pizza_name</th>
      <th>toppings</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Meatlovers</td>
      <td>1, 2, 3, 4, 5, 6, 8, 10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Vegetarian</td>
      <td>4, 6, 7, 9, 11, 12</td>
    </tr>
  </tbody>
</table>
</div>



Now, I need to insert the new item `Supreme Pizza` along with its topping IDs into the `pizza_names` and `pizza_recipes` tables.


```python
# Delete row and any data for `Supreme Pizza` from the tables affected
mycursor.execute("DELETE FROM pizza_runner.pizza_names where pizza_id = 3;")
mycursor.execute("DELETE FROM pizza_runner.pizza_recipes where pizza_id = 3;")


# Inserting "Supreme Pizza" into "pizza_names" table
mycursor.execute("""
INSERT INTO pizza_runner.pizza_names
(pizza_id, pizza_name)
VALUES
(3, 'Supreme Pizza');
""")


# Inserting "Supreme Pizza" toppings into "pizza_recipes" table
mycursor.execute("""
INSERT INTO pizza_runner.pizza_recipes
(pizza_id, toppings)
VALUES
(3, '1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12');
""")


# Saving the updated database
conn.commit()


# Display the updated menu of Pizza Runner 
pd.read_sql_query("""
SELECT r.pizza_id, n.pizza_name, r.toppings
FROM pizza_runner.pizza_recipes r
JOIN pizza_runner.pizza_names n ON r.pizza_id = n.pizza_id;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pizza_id</th>
      <th>pizza_name</th>
      <th>toppings</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Meatlovers</td>
      <td>1, 2, 3, 4, 5, 6, 8, 10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Vegetarian</td>
      <td>4, 6, 7, 9, 11, 12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Supreme Pizza</td>
      <td>1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```
