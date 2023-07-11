# Case Study #4: Data Bank
The case study questions presented here are created by [**Data With Danny**](https://linktr.ee/datawithdanny). They are part of the [**8 Week SQL Challenge**](https://8weeksqlchallenge.com/).

My SQL queries are written in the `PostgreSQL 15` dialect, integrated into `Jupyter Notebook`, which allows us to instantly view the query results and document the queries.

For more details about the **Case Study #4**, click [**here**](https://8weeksqlchallenge.com/case-study-4/).

## Table of Contents

### [1. Importing Libraries](#Import)

### [2. Tables of the Database](#Tables)

### [3. Case Study Questions](#CaseStudyQuestions)

- [A. Customer Nodes Exploration](#A)
- [B. Customer Transactions](#B)
- [C. Data Allocation Challenge](#C)


<a id = 'Import'></a>
## 1. Importing required Libraries


```python
import psycopg2 as pg2
import pandas as pd
import os
import warnings

warnings.filterwarnings('ignore')
```


<a id = 'Tables'></a>
## 2. Tables of the Database

### Connecting PostgreSQL database through Jupyter Notebook


```python
# Get the PostgreSQL password 
mypassword = os.getenv("POSTGRESQL_PASSWORD")

# Connect SQL database
conn = pg2.connect(user = 'postgres', password = mypassword, database = 'data_bank')
cursor = conn.cursor()
```

Now, let's list the table names of the `data_bank` database.


```python
query_ShowTables = """
SELECT 
    table_schema, 
    table_name
FROM information_schema.tables
WHERE table_schema = 'data_bank';
"""
cursor.execute(query_ShowTables)

print('--- Tables within "data_bank" database --- ')
for table in cursor:
    print(table[1])
```

    --- Tables within "data_bank" database --- 
    regions
    customer_nodes
    customer_transactions
    

The followings are the 3 tables within the `data_bank` database. Please click [**here**](https://8weeksqlchallenge.com/case-study-4/) to get more insights about the tables.


```python
cursor.execute(query_ShowTables)
for table in cursor:
    print("Table: ", table[1])
    query = "SELECT * FROM " + table[0] + '.' + table[1]
    df = pd.read_sql(query, conn)
    display(df)
```

    Table:  regions
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region_id</th>
      <th>region_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Australia</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>America</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Africa</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Asia</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Europe</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  customer_nodes
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>region_id</th>
      <th>node_id</th>
      <th>start_date</th>
      <th>end_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>3</td>
      <td>4</td>
      <td>2020-01-02</td>
      <td>2020-01-03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>5</td>
      <td>2020-01-03</td>
      <td>2020-01-17</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>5</td>
      <td>4</td>
      <td>2020-01-27</td>
      <td>2020-02-18</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>5</td>
      <td>4</td>
      <td>2020-01-07</td>
      <td>2020-01-19</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>3</td>
      <td>3</td>
      <td>2020-01-15</td>
      <td>2020-01-23</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3495</th>
      <td>496</td>
      <td>3</td>
      <td>4</td>
      <td>2020-02-25</td>
      <td>9999-12-31</td>
    </tr>
    <tr>
      <th>3496</th>
      <td>497</td>
      <td>5</td>
      <td>4</td>
      <td>2020-05-27</td>
      <td>9999-12-31</td>
    </tr>
    <tr>
      <th>3497</th>
      <td>498</td>
      <td>1</td>
      <td>2</td>
      <td>2020-04-05</td>
      <td>9999-12-31</td>
    </tr>
    <tr>
      <th>3498</th>
      <td>499</td>
      <td>5</td>
      <td>1</td>
      <td>2020-02-03</td>
      <td>9999-12-31</td>
    </tr>
    <tr>
      <th>3499</th>
      <td>500</td>
      <td>2</td>
      <td>2</td>
      <td>2020-04-15</td>
      <td>9999-12-31</td>
    </tr>
  </tbody>
</table>
<p>3500 rows × 5 columns</p>
</div>


    Table:  customer_transactions
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>txn_date</th>
      <th>txn_type</th>
      <th>txn_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>429</td>
      <td>2020-01-21</td>
      <td>deposit</td>
      <td>82</td>
    </tr>
    <tr>
      <th>1</th>
      <td>155</td>
      <td>2020-01-10</td>
      <td>deposit</td>
      <td>712</td>
    </tr>
    <tr>
      <th>2</th>
      <td>398</td>
      <td>2020-01-01</td>
      <td>deposit</td>
      <td>196</td>
    </tr>
    <tr>
      <th>3</th>
      <td>255</td>
      <td>2020-01-14</td>
      <td>deposit</td>
      <td>563</td>
    </tr>
    <tr>
      <th>4</th>
      <td>185</td>
      <td>2020-01-29</td>
      <td>deposit</td>
      <td>626</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>5863</th>
      <td>189</td>
      <td>2020-02-03</td>
      <td>withdrawal</td>
      <td>870</td>
    </tr>
    <tr>
      <th>5864</th>
      <td>189</td>
      <td>2020-03-22</td>
      <td>purchase</td>
      <td>718</td>
    </tr>
    <tr>
      <th>5865</th>
      <td>189</td>
      <td>2020-02-06</td>
      <td>purchase</td>
      <td>393</td>
    </tr>
    <tr>
      <th>5866</th>
      <td>189</td>
      <td>2020-01-22</td>
      <td>deposit</td>
      <td>302</td>
    </tr>
    <tr>
      <th>5867</th>
      <td>189</td>
      <td>2020-01-27</td>
      <td>withdrawal</td>
      <td>861</td>
    </tr>
  </tbody>
</table>
<p>5868 rows × 4 columns</p>
</div>



<a id = 'CaseStudyQuestions'></a>
## Case Study Questions

<a id = 'A'></a>
## A. Customer Nodes Exploration

#### 1. How many unique nodes are there on the Data Bank system?


```python
pd.read_sql("""
SELECT COUNT(DISTINCT node_id) AS nodes_count
FROM data_bank.customer_nodes
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nodes_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



**Result (Attempt #1)**\
In the Data Bank system, there are 5 unique nodes to which customers will be randomly reallocated.

However, if we interpret the question differently, we may understand that nodes are unique for each region, and customers are randomly distributed across the nodes *according to their region*. The following query shows the possible uniques nodes in each region.

**Here is the attempt #2**


```python
pd.read_sql("""
SELECT 
    cn.region_id, 
    r.region_name, 
    STRING_AGG(DISTINCT cn.node_id::VARCHAR(1), ', ') AS nodes
FROM data_bank.customer_nodes cn
JOIN data_bank.regions r ON cn.region_id = r.region_id
GROUP BY cn.region_id, r.region_name
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region_id</th>
      <th>region_name</th>
      <th>nodes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Australia</td>
      <td>1, 2, 3, 4, 5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>America</td>
      <td>1, 2, 3, 4, 5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Africa</td>
      <td>1, 2, 3, 4, 5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Asia</td>
      <td>1, 2, 3, 4, 5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Europe</td>
      <td>1, 2, 3, 4, 5</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql("""
SELECT SUM(nb_nodes)::INTEGER AS total_nodes
FROM
(
    -- Find the number of unique nodes per region
    
    SELECT region_id, COUNT(DISTINCT node_id) AS nb_nodes
    FROM data_bank.customer_nodes
    GROUP BY region_id
) n
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_nodes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>25</td>
    </tr>
  </tbody>
</table>
</div>



**Result (Attempt #2)**\
Hence, there are 25 unique nodes in the Data Bank system accross the world.

___
#### 2. What is the number of nodes per region?


```python
pd.read_sql("""
SELECT 
    r.region_name, 
    COUNT(DISTINCT node_id) AS nb_nodes
FROM data_bank.customer_nodes cn
JOIN data_bank.regions r ON cn.region_id = r.region_id
GROUP BY r.region_name; 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region_name</th>
      <th>nb_nodes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>America</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Asia</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Australia</td>
      <td>5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Europe</td>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
There are 5 nodes per region.

___
#### 3. How many customers are allocated to each region?


```python
pd.read_sql("""
SELECT 
    r.region_name AS region, 
    COUNT(DISTINCT customer_id) AS nb_customers
FROM data_bank.customer_nodes cn
JOIN data_bank.regions r ON cn.region_id = r.region_id
GROUP BY r.region_name
ORDER BY nb_customers DESC; 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region</th>
      <th>nb_customers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Australia</td>
      <td>110</td>
    </tr>
    <tr>
      <th>1</th>
      <td>America</td>
      <td>105</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Africa</td>
      <td>102</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Asia</td>
      <td>95</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Europe</td>
      <td>88</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- The majority of Data Bank customers are from Australia, totaling 110 customers.
- Following Australia, America and Africa are the second and third regions with the highest number of Data Bank customers.
- Asia has a total of 95 Data Bank customers.
- Data Bank does not seem to attract many customers from Europe, with only 88 clients.

___
#### 4. How many days on average are customers reallocated to a different node?


```python
pd.read_sql("""
SELECT ROUND(AVG(end_date::date - start_date::date), 1) AS avg_reallocation_days
FROM data_bank.customer_nodes
WHERE end_date != '9999-12-31'
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg_reallocation_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14.6</td>
    </tr>
  </tbody>
</table>
</div>



**Result (Attempt #1)**\
Customers are reallocated to a different node on average after 14.6 days .

However, if we examine the `customer_id 7`, we can observe two instances where the customer has been reallocated to the same node (nodes 2 and 4). For customer ID 7, it took him/her 
- 21 days to be reallocated from node 4 to node 2
- 34 days to be reallocated from node 2 to node 4


```python
pd.read_sql("""
SELECT * 
FROM data_bank.customer_nodes
WHERE customer_id = 7
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>region_id</th>
      <th>node_id</th>
      <th>start_date</th>
      <th>end_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>2</td>
      <td>5</td>
      <td>2020-01-20</td>
      <td>2020-02-04</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-02-05</td>
      <td>2020-02-20</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-02-21</td>
      <td>2020-02-26</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7</td>
      <td>2</td>
      <td>2</td>
      <td>2020-02-27</td>
      <td>2020-03-05</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>2</td>
      <td>2</td>
      <td>2020-03-06</td>
      <td>2020-04-01</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-04-02</td>
      <td>2020-04-07</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2</td>
      <td>5</td>
      <td>2020-04-08</td>
      <td>9999-12-31</td>
    </tr>
  </tbody>
</table>
</div>



**Table 1**: Customer 7 reallocated to node no 4 twice

node_id | start_date | end_date | nb_reallocation_days
--- | --- | --- | ---
4 | 2020-02-05 | 2020-02-20	| --
4 |	2020-02-21 | 2020-02-26	| 21

**Table 2**: Customer 7 reallocated to node no 2 twice 

node_id | start_date | end_date | nb_reallocation_days
--- | --- | --- | ---
2 | 2020-02-27 | 2020-03-05 | --
2 | 2020-03-06 | 2020-04-01	| 34

Hence, here is a query for the `reallocation_cte` that will be used later to answer the question. The query takes into consideration counting the total number of days for which customers will be reallocated to a different node, and includes all instances of both same and different nodes.


```python
# Let's see for customer ID 7

pd.read_sql("""
SELECT *,
    CASE 
        WHEN LEAD(node_id) OVER w = node_id THEN NULL 
        WHEN LAG(node_id) OVER w = node_id THEN end_date::date - LAG(start_date) OVER w::date
        ELSE end_date::date - start_date::date
    END AS nb_days
FROM data_bank.customer_nodes
WHERE end_date != '9999-12-31' AND customer_id = 7
WINDOW w AS (PARTITION BY customer_id ORDER BY start_date)
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>region_id</th>
      <th>node_id</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>nb_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>2</td>
      <td>5</td>
      <td>2020-01-20</td>
      <td>2020-02-04</td>
      <td>15.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-02-05</td>
      <td>2020-02-20</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-02-21</td>
      <td>2020-02-26</td>
      <td>21.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7</td>
      <td>2</td>
      <td>2</td>
      <td>2020-02-27</td>
      <td>2020-03-05</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>2</td>
      <td>2</td>
      <td>2020-03-06</td>
      <td>2020-04-01</td>
      <td>34.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>2</td>
      <td>4</td>
      <td>2020-04-02</td>
      <td>2020-04-07</td>
      <td>5.0</td>
    </tr>
  </tbody>
</table>
</div>



Now, let's utilize the aforementioned query as the `reallocation_cte`, to provide a comprehensive answer.


```python
pd.read_sql("""
WITH reallocation_cte AS
(
    -- Find the number of reallocation days
    SELECT *, 
        CASE 
            WHEN LEAD(node_id) OVER w = node_id THEN NULL 
            WHEN LAG(node_id) OVER w = node_id THEN end_date::date - LAG(start_date) OVER w::date
            ELSE end_date::date - start_date::date
        END AS nb_reallocation_days
    FROM data_bank.customer_nodes
    WHERE end_date != '9999-12-31' 
    WINDOW w AS (PARTITION BY customer_id ORDER BY start_date)
)
SELECT ROUND(AVG(nb_reallocation_days),1) AS avg_reallocation_days
FROM reallocation_cte;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg_reallocation_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>17.3</td>
    </tr>
  </tbody>
</table>
</div>



**Result (Attempt #2)**\
Customers are reallocated to a different node on average after 17.3 days .

___
#### 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

Since I have previously addressed the reallocation days problem (see question #4 above) using two different approaches, I will also provide my answer to this question in two attempts. Please note that the questions 4 and 5 are rather vague.

**Result (Attempt #1)**


```python
pd.read_sql("""
SELECT
    region_name, 
    PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY reallocation_days) as median,
    PERCENTILE_DISC(0.8) WITHIN GROUP(ORDER BY reallocation_days) as percentile_80th,
    PERCENTILE_DISC(0.95) WITHIN GROUP(ORDER BY reallocation_days) as percentile_95th
FROM
(
    SELECT 
        n.node_id, 
        n.start_date, 
        n.end_date, 
        r.region_name, 
        n.end_date::date - n.start_date::date AS reallocation_days
    FROM data_bank.customer_nodes n
    JOIN data_bank.regions r ON n.region_id = r.region_id
    WHERE n.end_date != '9999-12-31' 
) re
GROUP BY region_name;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region_name</th>
      <th>median</th>
      <th>percentile_80th</th>
      <th>percentile_95th</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>15</td>
      <td>24</td>
      <td>28</td>
    </tr>
    <tr>
      <th>1</th>
      <td>America</td>
      <td>15</td>
      <td>23</td>
      <td>28</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Asia</td>
      <td>15</td>
      <td>23</td>
      <td>28</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Australia</td>
      <td>15</td>
      <td>23</td>
      <td>28</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Europe</td>
      <td>15</td>
      <td>24</td>
      <td>28</td>
    </tr>
  </tbody>
</table>
</div>



**Result (Attempt #2)**


```python
pd.read_sql("""
WITH reallocation_cte AS
(
    SELECT *, 
        CASE 
            WHEN LEAD(node_id) OVER w = node_id THEN NULL 
            WHEN LAG(node_id) OVER w = node_id THEN end_date::date - LAG(start_date) OVER w::date
            ELSE end_date::date - start_date::date
        END AS nb_reallocation_days
    FROM data_bank.customer_nodes
    WHERE end_date != '9999-12-31' 
    WINDOW w AS (PARTITION BY customer_id ORDER BY start_date)
)
SELECT 
    r.region_name, 
    PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY cte.nb_reallocation_days) as median,
    PERCENTILE_DISC(0.8) WITHIN GROUP(ORDER BY cte.nb_reallocation_days) as percentile_80th,
    PERCENTILE_DISC(0.95) WITHIN GROUP(ORDER BY cte.nb_reallocation_days) as percentile_95th
FROM reallocation_cte cte
JOIN data_bank.regions r ON cte.region_id = r.region_id
GROUP BY r.region_name;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region_name</th>
      <th>median</th>
      <th>percentile_80th</th>
      <th>percentile_95th</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>17</td>
      <td>27</td>
      <td>36</td>
    </tr>
    <tr>
      <th>1</th>
      <td>America</td>
      <td>17</td>
      <td>26</td>
      <td>36</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Asia</td>
      <td>17</td>
      <td>25</td>
      <td>34</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Australia</td>
      <td>17</td>
      <td>26</td>
      <td>36</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Europe</td>
      <td>18</td>
      <td>27</td>
      <td>37</td>
    </tr>
  </tbody>
</table>
</div>



___
<a id = 'B'></a>
## B. Customer Transactions
#### 1. What is the unique count and total amount for each transaction type?


```python
pd.read_sql("""
SELECT 
    txn_type AS transaction_type, 
    to_char(COUNT(txn_type), 'FM 999,999') AS count, 
    to_char(SUM(txn_amount), 'FM$ 999,999,999.99') AS total_amount
FROM data_bank.customer_transactions
GROUP BY txn_type
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>transaction_type</th>
      <th>count</th>
      <th>total_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>purchase</td>
      <td>1,617</td>
      <td>$ 806,537.</td>
    </tr>
    <tr>
      <th>1</th>
      <td>withdrawal</td>
      <td>1,580</td>
      <td>$ 793,003.</td>
    </tr>
    <tr>
      <th>2</th>
      <td>deposit</td>
      <td>2,671</td>
      <td>$ 1,359,168.</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- The most fequently used transaction type at Data Bank is **deposit**, with a total numbers of 2,671 deposits, amounting to $1,359,168.
- There is a total of 1,671 **purchases** made at Data Bank, totalling $ 806,537.
- There is a total of 1,580 **withdrawals** made at Data Bank, totalling $ 793,003

___
#### 2. What is the average total historical deposit counts and amounts for all customers?


```python
pd.read_sql("""
SELECT 
    AVG(deposit_count)::INTEGER As nb_deposit, 
    to_char(AVG(avg_deposit_amount), 'FM$ 999,999.99') AS avg_deposit_amount
FROM
(
    -- Find the avegrage count and amount of deposits made by each customer
    SELECT 
        customer_id, 
        COUNT(txn_type) as deposit_count, 
        AVG(txn_amount) AS avg_deposit_amount
    FROM data_bank.customer_transactions
    WHERE txn_type = 'deposit'
    GROUP BY customer_id
    ORDER BY customer_id
) d
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_deposit</th>
      <th>avg_deposit_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>5</td>
      <td>$ 508.61</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
The Data Bank customers made an average of 5 deposits, with an average amount of $ 508.61.

___
#### 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?


```python
pd.read_sql("""
WITH counting_transactions_cte AS
(
    SELECT 
        customer_id,
        EXTRACT(MONTH FROM txn_date)::INTEGER AS month,
        TO_CHAR(txn_date, 'MONTH') AS month_name,
        SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) AS deposit,
        SUM(CASE WHEN txn_type = 'purchase' THEN 1 ELSE 0 END) AS purchase,
        SUM(CASE WHEN txn_type = 'withdrawal' THEN 1 ELSE 0 END) AS withdrawal
    FROM data_bank.customer_transactions
    GROUP BY customer_id, month, month_name
    ORDER BY customer_id
)
SELECT 
    month, 
    month_name, 
    COUNT(DISTINCT customer_id) AS nb_customers
FROM counting_transactions_cte
WHERE deposit > 1 AND (purchase > 0 OR withdrawal > 0)
GROUP BY month, month_name
ORDER BY month;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>month_name</th>
      <th>nb_customers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>JANUARY</td>
      <td>168</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>FEBRUARY</td>
      <td>181</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>MARCH</td>
      <td>192</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>APRIL</td>
      <td>70</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- **March** is the month when Data Bank customers make the most transactions (more than 1 deposit and either 1 purchase or 1 withdrawal in a single month), with a total of 192 customers.
- Following March, **February** is the second month with the highest number of customers engaging in the given transactions.
- In **January**, a total of 168 customers made more than 1 deposit and either 1 purchase or 1 withdrawal.
- The month of **April** has the fewest number of customers conducting these types of transactions, with only 70 clients.

___
#### 4. What is the closing balance for each customer at the end of the month?

Here are the results showing the first 5 customers.


```python
pd.read_sql("""
SELECT 
    customer_id,
    EXTRACT(MONTH FROM txn_date)::INTEGER AS month,
    TO_CHAR(txn_date, 'MONTH') AS month_name,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE - txn_amount END) AS closing_balance
FROM data_bank.customer_transactions
WHERE customer_id <= 5
GROUP BY customer_id, month, month_name
ORDER BY customer_id, month;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>month</th>
      <th>month_name</th>
      <th>closing_balance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>JANUARY</td>
      <td>312</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>3</td>
      <td>MARCH</td>
      <td>-952</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>JANUARY</td>
      <td>549</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>3</td>
      <td>MARCH</td>
      <td>61</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>1</td>
      <td>JANUARY</td>
      <td>144</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>2</td>
      <td>FEBRUARY</td>
      <td>-965</td>
    </tr>
    <tr>
      <th>6</th>
      <td>3</td>
      <td>3</td>
      <td>MARCH</td>
      <td>-401</td>
    </tr>
    <tr>
      <th>7</th>
      <td>3</td>
      <td>4</td>
      <td>APRIL</td>
      <td>493</td>
    </tr>
    <tr>
      <th>8</th>
      <td>4</td>
      <td>1</td>
      <td>JANUARY</td>
      <td>848</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4</td>
      <td>3</td>
      <td>MARCH</td>
      <td>-193</td>
    </tr>
    <tr>
      <th>10</th>
      <td>5</td>
      <td>1</td>
      <td>JANUARY</td>
      <td>954</td>
    </tr>
    <tr>
      <th>11</th>
      <td>5</td>
      <td>3</td>
      <td>MARCH</td>
      <td>-2877</td>
    </tr>
    <tr>
      <th>12</th>
      <td>5</td>
      <td>4</td>
      <td>APRIL</td>
      <td>-490</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. What is the percentage of customers who increase their closing balance by more than 5%?


```python
pd.read_sql("""
WITH monthly_balance_cte AS
(
    SELECT 
        customer_id,
        EXTRACT(MONTH FROM txn_date)::INTEGER AS month,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE - txn_amount END) AS closing_balance
    FROM data_bank.customer_transactions
    GROUP BY customer_id, month
    ORDER BY customer_id, month
),
balance_greaterthan5_cte AS
(
    SELECT COUNT(DISTINCT customer_id) AS nb_customers
    FROM
    (
        SELECT 
            customer_id, 
            (LEAD(closing_balance) OVER (PARTITION BY customer_id ORDER BY month) - closing_balance)/ closing_balance::numeric*100 AS percent_change
        FROM monthly_balance_cte
    ) pc
    WHERE percent_change > 5
)
SELECT 
    MAX(nb_customers) AS nb_customers, 
    COUNT(DISTINCT ct.customer_id) AS total_customers,
    CONCAT(ROUND(MAX(nb_customers)/COUNT(DISTINCT ct.customer_id)::numeric * 100,1), ' %') AS percentage_customers
FROM balance_greaterthan5_cte b, data_bank.customer_transactions ct
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_customers</th>
      <th>total_customers</th>
      <th>percentage_customers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>269</td>
      <td>500</td>
      <td>53.8 %</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
There are 53.8 % of the Data Bank customers who increase their closing balance by more than 5%.

___
<a id = 'C'></a>
## C. Data Allocation Challenge

To test out a few different hypotheses - the Data Bank team wants to run an experiment where different groups of customers would be allocated data using 3 different options:

- `Option 1`: data is allocated based off the amount of money at the end of the previous month
- `Option 2`: data is allocated on the average amount of money kept in the account in the previous 30 days
- `Option 3`: data is updated real-time

For this multi-part challenge question - you have been requested to generate the following data elements to help the Data Bank team estimate how much data will need to be provisioned for each option:
- running customer balance column that includes the impact each transaction
- customer balance at the end of each month
- minimum, average and maximum values of the running balance for each customer

Using all of the data available - how much data would have been required for each option on a monthly basis?

### Running Balance


```python
pd.read_sql("""
SELECT *, 
    SUM(
        CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END
    ) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM data_bank.customer_transactions
WHERE customer_id <= 5;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>txn_date</th>
      <th>txn_type</th>
      <th>txn_amount</th>
      <th>running_balance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2020-01-02</td>
      <td>deposit</td>
      <td>312</td>
      <td>312</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2020-03-05</td>
      <td>purchase</td>
      <td>612</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2020-03-17</td>
      <td>deposit</td>
      <td>324</td>
      <td>24</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>2020-03-19</td>
      <td>purchase</td>
      <td>664</td>
      <td>-640</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>2020-01-03</td>
      <td>deposit</td>
      <td>549</td>
      <td>549</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>2020-03-24</td>
      <td>deposit</td>
      <td>61</td>
      <td>610</td>
    </tr>
    <tr>
      <th>6</th>
      <td>3</td>
      <td>2020-01-27</td>
      <td>deposit</td>
      <td>144</td>
      <td>144</td>
    </tr>
    <tr>
      <th>7</th>
      <td>3</td>
      <td>2020-02-22</td>
      <td>purchase</td>
      <td>965</td>
      <td>-821</td>
    </tr>
    <tr>
      <th>8</th>
      <td>3</td>
      <td>2020-03-05</td>
      <td>withdrawal</td>
      <td>213</td>
      <td>-1034</td>
    </tr>
    <tr>
      <th>9</th>
      <td>3</td>
      <td>2020-03-19</td>
      <td>withdrawal</td>
      <td>188</td>
      <td>-1222</td>
    </tr>
    <tr>
      <th>10</th>
      <td>3</td>
      <td>2020-04-12</td>
      <td>deposit</td>
      <td>493</td>
      <td>-729</td>
    </tr>
    <tr>
      <th>11</th>
      <td>4</td>
      <td>2020-01-07</td>
      <td>deposit</td>
      <td>458</td>
      <td>458</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4</td>
      <td>2020-01-21</td>
      <td>deposit</td>
      <td>390</td>
      <td>848</td>
    </tr>
    <tr>
      <th>13</th>
      <td>4</td>
      <td>2020-03-25</td>
      <td>purchase</td>
      <td>193</td>
      <td>655</td>
    </tr>
    <tr>
      <th>14</th>
      <td>5</td>
      <td>2020-01-15</td>
      <td>deposit</td>
      <td>974</td>
      <td>974</td>
    </tr>
    <tr>
      <th>15</th>
      <td>5</td>
      <td>2020-01-25</td>
      <td>deposit</td>
      <td>806</td>
      <td>1780</td>
    </tr>
    <tr>
      <th>16</th>
      <td>5</td>
      <td>2020-01-31</td>
      <td>withdrawal</td>
      <td>826</td>
      <td>954</td>
    </tr>
    <tr>
      <th>17</th>
      <td>5</td>
      <td>2020-03-02</td>
      <td>purchase</td>
      <td>886</td>
      <td>68</td>
    </tr>
    <tr>
      <th>18</th>
      <td>5</td>
      <td>2020-03-19</td>
      <td>deposit</td>
      <td>718</td>
      <td>786</td>
    </tr>
    <tr>
      <th>19</th>
      <td>5</td>
      <td>2020-03-26</td>
      <td>withdrawal</td>
      <td>786</td>
      <td>0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>5</td>
      <td>2020-03-27</td>
      <td>deposit</td>
      <td>412</td>
      <td>-288</td>
    </tr>
    <tr>
      <th>21</th>
      <td>5</td>
      <td>2020-03-27</td>
      <td>withdrawal</td>
      <td>700</td>
      <td>-288</td>
    </tr>
    <tr>
      <th>22</th>
      <td>5</td>
      <td>2020-03-29</td>
      <td>purchase</td>
      <td>852</td>
      <td>-1140</td>
    </tr>
    <tr>
      <th>23</th>
      <td>5</td>
      <td>2020-03-31</td>
      <td>purchase</td>
      <td>783</td>
      <td>-1923</td>
    </tr>
    <tr>
      <th>24</th>
      <td>5</td>
      <td>2020-04-02</td>
      <td>withdrawal</td>
      <td>490</td>
      <td>-2413</td>
    </tr>
  </tbody>
</table>
</div>



### Monthly Balance


```python
pd.read_sql("""
SELECT 
    customer_id,
    EXTRACT(MONTH FROM txn_date)::INTEGER AS month,
    TO_CHAR(txn_date, 'Month') AS month_name,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS closing_balance
FROM data_bank.customer_transactions
WHERE customer_id <= 5
GROUP BY customer_id, month, month_name
ORDER BY customer_id, month;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>month</th>
      <th>month_name</th>
      <th>closing_balance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>January</td>
      <td>312</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>3</td>
      <td>March</td>
      <td>-952</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>January</td>
      <td>549</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>3</td>
      <td>March</td>
      <td>61</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>1</td>
      <td>January</td>
      <td>144</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>2</td>
      <td>February</td>
      <td>-965</td>
    </tr>
    <tr>
      <th>6</th>
      <td>3</td>
      <td>3</td>
      <td>March</td>
      <td>-401</td>
    </tr>
    <tr>
      <th>7</th>
      <td>3</td>
      <td>4</td>
      <td>April</td>
      <td>493</td>
    </tr>
    <tr>
      <th>8</th>
      <td>4</td>
      <td>1</td>
      <td>January</td>
      <td>848</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4</td>
      <td>3</td>
      <td>March</td>
      <td>-193</td>
    </tr>
    <tr>
      <th>10</th>
      <td>5</td>
      <td>1</td>
      <td>January</td>
      <td>954</td>
    </tr>
    <tr>
      <th>11</th>
      <td>5</td>
      <td>3</td>
      <td>March</td>
      <td>-2877</td>
    </tr>
    <tr>
      <th>12</th>
      <td>5</td>
      <td>4</td>
      <td>April</td>
      <td>-490</td>
    </tr>
  </tbody>
</table>
</div>



### Min, Average, Max Transaction


```python
pd.read_sql("""
SELECT 
    customer_id, 
    MIN(running_balance) AS min_transaction, 
    MAX(running_balance) AS max_transaction,
    ROUND(AVG(running_balance),2) AS avg_transaction
FROM 
(
    SELECT *,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
    FROM data_bank.customer_transactions
) running_balance
WHERE customer_id <= 10
GROUP BY customer_id
ORDER BY customer_id
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>min_transaction</th>
      <th>max_transaction</th>
      <th>avg_transaction</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>-640</td>
      <td>312</td>
      <td>-151.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>549</td>
      <td>610</td>
      <td>579.50</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>-1222</td>
      <td>144</td>
      <td>-732.40</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>458</td>
      <td>848</td>
      <td>653.67</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>-2413</td>
      <td>1780</td>
      <td>-135.45</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>-552</td>
      <td>2197</td>
      <td>624.00</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>887</td>
      <td>3539</td>
      <td>2268.69</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>-1029</td>
      <td>1363</td>
      <td>173.70</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>-91</td>
      <td>2030</td>
      <td>1021.70</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>-5090</td>
      <td>556</td>
      <td>-2229.83</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```
