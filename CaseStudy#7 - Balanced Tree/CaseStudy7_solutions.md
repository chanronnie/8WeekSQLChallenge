# Case Study #7: Balanced Tree Clothing Co.
The case study questions presented here are created by [**Data With Danny**](https://linktr.ee/datawithdanny). They are part of the [**8 Week SQL Challenge**](https://8weeksqlchallenge.com/).

My SQL queries are written in the `PostgreSQL 15` dialect, integrated into `Jupyter Notebook`, which allows us to instantly view the query results and document the queries.

For more details about the **Case Study #7**, click [**here**](https://8weeksqlchallenge.com/case-study-7/).

## Table of Contents
### [1. Importing Libraries](#Import)
### [2. Tables of the Database](#Tables)
### [3. Case Study Questions](#CaseStudyQuestions)
- [A. High Level Sales Analysis](#A)
- [B. Transaction Analysis](#B)
- [C. Product Analysis](#C)
- [D. Bonus Challenge](#D)


<a id = 'Import'></a>
## 1. Import Libraries


```python
import pandas as pd
import psycopg2 as pg2
import os
import warnings

warnings.filterwarnings("ignore")
```

### Connecting the database from Jupyter Notebook


```python
# Get PostgreSQL password
mypassword = os.getenv("POSTGRESQL_PASSWORD")

# Connect the database
try:
    conn = pg2.connect(user = 'postgres', password = mypassword, database = 'balanced_tree')
    cursor = conn.cursor()
    print("Database connection successful")
except mysql.connector.Error as err:
   print(f"Error: '{err}'")
```

    Database connection successful
    


<a id = 'Tables'></a>
## 2. Tables of the database

First, let's verify if the connected database contains the 4 dataset names.


```python
cursor.execute("""
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'balanced_tree'
""")

table_names = []
print('--- Tables within "balanced_tree" database --- ')
for table in cursor:
    print(table[1])
    table_names.append(table[1])
```

    --- Tables within "balanced_tree" database --- 
    product_hierarchy
    product_prices
    product_details
    sales
    

Here are the 4 datasets of the `balanced_tree` database. For more details about each dataset, please click [**here**](https://8weeksqlchallenge.com/case-study-7/).


```python
for table in table_names:
    print("Table: ", table)
    display(pd.read_sql("SELECT * FROM balanced_tree." + table, conn))
```

    Table:  product_hierarchy
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>parent_id</th>
      <th>level_text</th>
      <th>level_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>NaN</td>
      <td>Womens</td>
      <td>Category</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NaN</td>
      <td>Mens</td>
      <td>Category</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1.0</td>
      <td>Jeans</td>
      <td>Segment</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1.0</td>
      <td>Jacket</td>
      <td>Segment</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2.0</td>
      <td>Shirt</td>
      <td>Segment</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>2.0</td>
      <td>Socks</td>
      <td>Segment</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>3.0</td>
      <td>Navy Oversized</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>3.0</td>
      <td>Black Straight</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>3.0</td>
      <td>Cream Relaxed</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>4.0</td>
      <td>Khaki Suit</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>4.0</td>
      <td>Indigo Rain</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>4.0</td>
      <td>Grey Fashion</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13</td>
      <td>5.0</td>
      <td>White Tee</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>13</th>
      <td>14</td>
      <td>5.0</td>
      <td>Teal Button Up</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>14</th>
      <td>15</td>
      <td>5.0</td>
      <td>Blue Polo</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>15</th>
      <td>16</td>
      <td>6.0</td>
      <td>Navy Solid</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>16</th>
      <td>17</td>
      <td>6.0</td>
      <td>White Striped</td>
      <td>Style</td>
    </tr>
    <tr>
      <th>17</th>
      <td>18</td>
      <td>6.0</td>
      <td>Pink Fluro Polkadot</td>
      <td>Style</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  product_prices
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>product_id</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>c4a632</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>8</td>
      <td>e83aa3</td>
      <td>32</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9</td>
      <td>e31d39</td>
      <td>10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10</td>
      <td>d5e9a6</td>
      <td>23</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11</td>
      <td>72f5d4</td>
      <td>19</td>
    </tr>
    <tr>
      <th>5</th>
      <td>12</td>
      <td>9ec847</td>
      <td>54</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13</td>
      <td>5d267b</td>
      <td>40</td>
    </tr>
    <tr>
      <th>7</th>
      <td>14</td>
      <td>c8d436</td>
      <td>10</td>
    </tr>
    <tr>
      <th>8</th>
      <td>15</td>
      <td>2a2353</td>
      <td>57</td>
    </tr>
    <tr>
      <th>9</th>
      <td>16</td>
      <td>f084eb</td>
      <td>36</td>
    </tr>
    <tr>
      <th>10</th>
      <td>17</td>
      <td>b9a74d</td>
      <td>17</td>
    </tr>
    <tr>
      <th>11</th>
      <td>18</td>
      <td>2feb6b</td>
      <td>29</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  product_details
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>price</th>
      <th>product_name</th>
      <th>category_id</th>
      <th>segment_id</th>
      <th>style_id</th>
      <th>category_name</th>
      <th>segment_name</th>
      <th>style_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c4a632</td>
      <td>13</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>7</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Navy Oversized</td>
    </tr>
    <tr>
      <th>1</th>
      <td>e83aa3</td>
      <td>32</td>
      <td>Black Straight Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>8</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Black Straight</td>
    </tr>
    <tr>
      <th>2</th>
      <td>e31d39</td>
      <td>10</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>9</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Cream Relaxed</td>
    </tr>
    <tr>
      <th>3</th>
      <td>d5e9a6</td>
      <td>23</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>10</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Khaki Suit</td>
    </tr>
    <tr>
      <th>4</th>
      <td>72f5d4</td>
      <td>19</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>11</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Indigo Rain</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9ec847</td>
      <td>54</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>12</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Grey Fashion</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5d267b</td>
      <td>40</td>
      <td>White Tee Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>13</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>White Tee</td>
    </tr>
    <tr>
      <th>7</th>
      <td>c8d436</td>
      <td>10</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>14</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Teal Button Up</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2a2353</td>
      <td>57</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>15</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Blue Polo</td>
    </tr>
    <tr>
      <th>9</th>
      <td>f084eb</td>
      <td>36</td>
      <td>Navy Solid Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>16</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Navy Solid</td>
    </tr>
    <tr>
      <th>10</th>
      <td>b9a74d</td>
      <td>17</td>
      <td>White Striped Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>17</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>White Striped</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2feb6b</td>
      <td>29</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>18</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Pink Fluro Polkadot</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  sales
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>prod_id</th>
      <th>qty</th>
      <th>price</th>
      <th>discount</th>
      <th>member</th>
      <th>txn_id</th>
      <th>start_txn_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c4a632</td>
      <td>4</td>
      <td>13</td>
      <td>17</td>
      <td>True</td>
      <td>54f307</td>
      <td>2021-02-13 01:59:43.296000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>5d267b</td>
      <td>4</td>
      <td>40</td>
      <td>17</td>
      <td>True</td>
      <td>54f307</td>
      <td>2021-02-13 01:59:43.296000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>b9a74d</td>
      <td>4</td>
      <td>17</td>
      <td>17</td>
      <td>True</td>
      <td>54f307</td>
      <td>2021-02-13 01:59:43.296000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2feb6b</td>
      <td>2</td>
      <td>29</td>
      <td>17</td>
      <td>True</td>
      <td>54f307</td>
      <td>2021-02-13 01:59:43.296000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>c4a632</td>
      <td>5</td>
      <td>13</td>
      <td>21</td>
      <td>True</td>
      <td>26cc98</td>
      <td>2021-01-19 01:39:00.345600</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>15090</th>
      <td>9ec847</td>
      <td>1</td>
      <td>54</td>
      <td>13</td>
      <td>True</td>
      <td>f15ab3</td>
      <td>2021-03-20 12:01:22.944000</td>
    </tr>
    <tr>
      <th>15091</th>
      <td>2a2353</td>
      <td>3</td>
      <td>57</td>
      <td>13</td>
      <td>True</td>
      <td>f15ab3</td>
      <td>2021-03-20 12:01:22.944000</td>
    </tr>
    <tr>
      <th>15092</th>
      <td>e83aa3</td>
      <td>5</td>
      <td>32</td>
      <td>1</td>
      <td>True</td>
      <td>93620b</td>
      <td>2021-03-01 07:11:24.662400</td>
    </tr>
    <tr>
      <th>15093</th>
      <td>d5e9a6</td>
      <td>2</td>
      <td>23</td>
      <td>1</td>
      <td>True</td>
      <td>93620b</td>
      <td>2021-03-01 07:11:24.662400</td>
    </tr>
    <tr>
      <th>15094</th>
      <td>5d267b</td>
      <td>2</td>
      <td>40</td>
      <td>1</td>
      <td>True</td>
      <td>93620b</td>
      <td>2021-03-01 07:11:24.662400</td>
    </tr>
  </tbody>
</table>
<p>15095 rows × 7 columns</p>
</div>


<a id = 'CaseStudyQuestions'></a>
## 3. Case Study Questions

<a id = 'A'></a>
## A. High Level Sales Analysis

#### 1. What was the total quantity sold for all products?


```python
pd.read_sql("""
SELECT TO_CHAR(SUM(qty), 'FM 999,999') AS total_quantity
FROM balanced_tree.sales
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>45,216</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can break down the total quantity by product.


```python
pd.read_sql("""
SELECT 
    pd.product_name, 
    SUM(s.qty) AS total_quantity
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.product_name
ORDER BY total_quantity DESC
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>total_quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>3876</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Navy Oversized Jeans - Womens</td>
      <td>3856</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Blue Polo Shirt - Mens</td>
      <td>3819</td>
    </tr>
    <tr>
      <th>3</th>
      <td>White Tee Shirt - Mens</td>
      <td>3800</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Navy Solid Socks - Mens</td>
      <td>3792</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Black Straight Jeans - Womens</td>
      <td>3786</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>3770</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Indigo Rain Jacket - Womens</td>
      <td>3757</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Khaki Suit Jacket - Womens</td>
      <td>3752</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>3707</td>
    </tr>
    <tr>
      <th>10</th>
      <td>White Striped Socks - Mens</td>
      <td>3655</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Teal Button Up Shirt - Mens</td>
      <td>3646</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 2. What is the total generated revenue for all products before discounts?


```python
pd.read_sql("""
SELECT TO_CHAR(SUM(qty*price), 'FM$ 999,999,999.99') AS total_sales
FROM balanced_tree.sales
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
      <td>$ 1,289,453.</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can break down the total sales by product.


```python
pd.read_sql("""
SELECT 
    product_name, 
    TO_CHAR(total_sales, 'FM$ 999,999')  AS total_sales
FROM
(
    SELECT pd.product_name, SUM(s.qty * s.price) AS total_sales
    FROM balanced_tree.sales s
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    GROUP BY pd.product_name
    ORDER BY total_sales DESC
) ts
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>total_sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Blue Polo Shirt - Mens</td>
      <td>$ 217,683</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>$ 209,304</td>
    </tr>
    <tr>
      <th>2</th>
      <td>White Tee Shirt - Mens</td>
      <td>$ 152,000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Navy Solid Socks - Mens</td>
      <td>$ 136,512</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Black Straight Jeans - Womens</td>
      <td>$ 121,152</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>$ 109,330</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Khaki Suit Jacket - Womens</td>
      <td>$ 86,296</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Indigo Rain Jacket - Womens</td>
      <td>$ 71,383</td>
    </tr>
    <tr>
      <th>8</th>
      <td>White Striped Socks - Mens</td>
      <td>$ 62,135</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Navy Oversized Jeans - Womens</td>
      <td>$ 50,128</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>$ 37,070</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Teal Button Up Shirt - Mens</td>
      <td>$ 36,460</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 3. What was the total discount amount for all products?


```python
pd.read_sql("""
SELECT TO_CHAR(SUM(qty * price * (discount/100::NUMERIC)), 'FM$ 999,999.99') AS total_discounts
FROM balanced_tree.sales
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_discounts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>$ 156,229.14</td>
    </tr>
  </tbody>
</table>
</div>


<a id = 'B'></a>
## B. Transaction Analysis

#### 1. How many unique transactions were there?


```python
pd.read_sql("""
SELECT COUNT(DISTINCT txn_id) AS nb_transactions
FROM balanced_tree.sales
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_transactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2500</td>
    </tr>
  </tbody>
</table>
</div>



**Result**</br>
There are 2,500 unique transactions made at Balanced Tree Clothing Co.

___
#### 2. What is the average unique products purchased in each transaction?


```python
pd.read_sql("""
WITH count_unique_product_ids_cte AS
(
    SELECT txn_id, COUNT(DISTINCT prod_id) AS product_id
    FROM balanced_tree.sales
    GROUP BY txn_id
)
SELECT ROUND(AVG(product_id), 1) AS avg_unique_products
FROM count_unique_product_ids_cte
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg_unique_products</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6.0</td>
    </tr>
  </tbody>
</table>
</div>



**Result**</br>
The average number of distinct products purchased per transaction is 6.

___
#### 3. What are the 25th, 50th and 75th percentile values for the revenue per transaction?


```python
pd.read_sql("""
WITH revenue_per_transaction_cte AS
(
    SELECT 
        txn_id, 
        SUM((qty * price) * (1 - discount/100::NUMERIC)) AS revenue
    FROM balanced_tree.sales
    GROUP BY txn_id
)
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY revenue) AS percentile_25th,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY revenue) AS percentile_50th,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY revenue) AS percentile_75th
FROM revenue_per_transaction_cte
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>percentile_25th</th>
      <th>percentile_50th</th>
      <th>percentile_75th</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>326.405</td>
      <td>441.225</td>
      <td>572.7625</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 4. What is the average discount value per transaction?


```python
pd.read_sql("""
SELECT 
    CONCAT('$ ', ROUND(AVG(discount_value),2)) AS average_discount
FROM 
(
    SELECT txn_id, SUM(qty * price * (discount/100::NUMERIC)) AS discount_value
    FROM balanced_tree.sales
    GROUP BY txn_id
) dv
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>average_discount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>$ 62.49</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. What is the percentage split of all transactions for members vs non-members?

We can utilize the `total revenue` as a metric to determine the percentage distribution of all transactions between members and non-members. The analysis reveals that members bought more at Balanced Tree Clothing Co. compared to non-members, with a split of 60.31% for members and 36.69% for non-members.


```python
pd.read_sql("""
WITH txn_revenue_cte AS
(
    SELECT txn_id, member, SUM((qty * price) * (1 - discount/100::NUMERIC)) AS revenue
    FROM balanced_tree.sales
    GROUP BY txn_id, member
)
SELECT 
    member, 
    CONCAT(ROUND(SUM(revenue)/(SELECT SUM(revenue) FROM txn_revenue_cte)::NUMERIC * 100,2), ' %') AS percentage_split
FROM txn_revenue_cte
GROUP BY member
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>member</th>
      <th>percentage_split</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>39.69 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>True</td>
      <td>60.31 %</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can use the `number of transactions` made by members and non-members as a metric to address the question. Employing this approach, the percentage split remains relatively consistent with the previous findings.


```python
pd.read_sql("""
SELECT 
    CONCAT(ROUND(SUM(CASE WHEN member = True THEN 1 ELSE 0 END)/COUNT(*)::NUMERIC * 100,2), ' %') AS member_percentage,
    CONCAT(ROUND(SUM(CASE WHEN member = False THEN 1 ELSE 0 END)/COUNT(*)::NUMERIC * 100,2), ' %') AS nonmember_percentage
FROM balanced_tree.sales
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>member_percentage</th>
      <th>nonmember_percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>60.03 %</td>
      <td>39.97 %</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 6. What is the average revenue for member transactions and non-member transactions?


```python
pd.read_sql("""
WITH txn_revenue_cte AS
(
    SELECT txn_id, member, SUM((qty * price) * (1 - discount/100::NUMERIC)) AS revenue 
    FROM balanced_tree.sales
    GROUP BY txn_id, member
)
SELECT member, CONCAT('$ ', ROUND(AVG(revenue),2)) AS avg_revenue
FROM txn_revenue_cte
GROUP BY member
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>member</th>
      <th>avg_revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>$ 452.01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>True</td>
      <td>$ 454.14</td>
    </tr>
  </tbody>
</table>
</div>


<a id = 'C'></a>
## C. Product Analysis

#### 1. What are the top 3 products by total revenue before discount?


```python
pd.read_sql("""
SELECT product_name, TO_CHAR(total_revenue, 'FM$ 999,999') AS total_revenue
FROM
(
    SELECT 
        pd.product_name, 
        SUM(s.qty * s.price) AS total_revenue
    FROM balanced_tree.sales s
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    GROUP BY pd.product_name
    ORDER BY total_revenue DESC
) tr
LIMIT 3
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>total_revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Blue Polo Shirt - Mens</td>
      <td>$ 217,683</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>$ 209,304</td>
    </tr>
    <tr>
      <th>2</th>
      <td>White Tee Shirt - Mens</td>
      <td>$ 152,000</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 2. What is the total quantity, revenue and discount for each segment?


```python
pd.read_sql("""
SELECT 
    pd.segment_name AS segment, 
    TO_CHAR(SUM(s.qty), 'FM 999,999') AS total_quantity, 
    TO_CHAR(SUM(s.qty * s.price), 'FM$ 999,999') AS total_revenue, 
    TO_CHAR(SUM(s.qty * s.price * s.discount/100::NUMERIC), 'FM$ 999,999') AS total_discounts
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.segment_name
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>segment</th>
      <th>total_quantity</th>
      <th>total_revenue</th>
      <th>total_discounts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Shirt</td>
      <td>11,265</td>
      <td>$ 406,143</td>
      <td>$ 49,594</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jeans</td>
      <td>11,349</td>
      <td>$ 208,350</td>
      <td>$ 25,344</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Jacket</td>
      <td>11,385</td>
      <td>$ 366,983</td>
      <td>$ 44,277</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Socks</td>
      <td>11,217</td>
      <td>$ 307,977</td>
      <td>$ 37,013</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 3. What is the top selling product for each segment?


```python
pd.read_sql("""
WITH sales_cte AS
(
    SELECT 
        pd.segment_name AS segment, 
        pd.product_name AS product, 
        SUM(s.qty) AS total_sold_items,
        DENSE_RANK() OVER (PARTITION BY pd.segment_name ORDER BY SUM(s.qty) DESC) AS rank
    FROM balanced_tree.sales s
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    GROUP BY pd.segment_name, pd.product_name
    ORDER BY pd.segment_name
)
SELECT 
    segment, 
    product, 
    total_sold_items 
FROM sales_cte
WHERE rank = 1
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>segment</th>
      <th>product</th>
      <th>total_sold_items</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Jacket</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>3876</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jeans</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>3856</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Shirt</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>3819</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Socks</td>
      <td>Navy Solid Socks - Mens</td>
      <td>3792</td>
    </tr>
  </tbody>
</table>
</div>

**Result**
- The best-selling product in the **Jacket** segment is **Grey Fashion Jacket - Womens**.
- The best-selling product in the **Jeans** segment is **Navy Oversized Jeans - Womens**.
- The best-selling product in the **Shirt** segment is **Blue Polo Shirt - Mens**.
- The best-selling product in the **Socks** segment is **Navy Solid Socks - Mens**.


___
#### 4. What is the total quantity, revenue and discount for each category?


```python
pd.read_sql("""
SELECT 
    category,
    TO_CHAR(total_quantity, 'FM 999,999') AS total_quantity,
    TO_CHAR(total_revenue, 'FM$ 999,999') AS total_revenue,
    TO_CHAR(total_discount, 'FM$ 999,999.99') AS total_discount
FROM
(
    SELECT 
        pd.category_name AS category, 
        SUM(s.qty) AS total_quantity, 
        SUM(s.qty * s.price) AS total_revenue, 
        SUM(s.qty * s.price * s.discount/100::NUMERIC) AS total_discount
    FROM balanced_tree.sales s
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    GROUP BY pd.category_name
) t
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>total_quantity</th>
      <th>total_revenue</th>
      <th>total_discount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mens</td>
      <td>22,482</td>
      <td>$ 714,120</td>
      <td>$ 86,607.71</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Womens</td>
      <td>22,734</td>
      <td>$ 575,333</td>
      <td>$ 69,621.43</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. What is the top selling product for each category?


```python
pd.read_sql("""
WITH category_sales_cte AS
(
    SELECT 
        pd.category_name AS category, 
        pd.product_name AS product, 
        SUM(s.qty) AS total_sold_items,
        RANK() OVER (PARTITION BY pd.category_name ORDER BY SUM(s.qty) DESC) AS rank
    FROM balanced_tree.sales s
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    GROUP BY 1,2
)
SELECT 
    category, 
    product, 
    TO_CHAR(total_sold_items, 'FM 999,999') AS total_sold_items
FROM category_sales_cte 
WHERE rank = 1 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>product</th>
      <th>total_sold_items</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mens</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>3,819</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Womens</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>3,876</td>
    </tr>
  </tbody>
</table>
</div>

**Result**:
- The best-selling product in the **Men** category is **Blue Polo Shirt - Men**.
- The best-selling product in the **Women** category is **Grey Fashion Jacket - Women**.


___
#### 6. What is the percentage split of revenue by product for each segment?


```python
pd.read_sql("""
SELECT 
    pd.segment_name AS segment, 
    pd.product_name AS product,
    TO_CHAR(SUM(s.qty * s.price), 'FM$ 999,999') AS total_revenue,
    CONCAT(ROUND(SUM(s.qty * s.price)/SUM(SUM(s.qty * s.price)) OVER (PARTITION BY pd.segment_name)::NUMERIC * 100,1), ' %') AS percent_split
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.segment_name, pd.product_name
ORDER BY segment_name, percent_split
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>segment</th>
      <th>product</th>
      <th>total_revenue</th>
      <th>percent_split</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Jacket</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>$ 71,383</td>
      <td>19.5 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jacket</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>$ 86,296</td>
      <td>23.5 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Jacket</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>$ 209,304</td>
      <td>57.0 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Jeans</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>$ 37,070</td>
      <td>17.8 %</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Jeans</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>$ 50,128</td>
      <td>24.1 %</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Jeans</td>
      <td>Black Straight Jeans - Womens</td>
      <td>$ 121,152</td>
      <td>58.1 %</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Shirt</td>
      <td>White Tee Shirt - Mens</td>
      <td>$ 152,000</td>
      <td>37.4 %</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Shirt</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>$ 217,683</td>
      <td>53.6 %</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Shirt</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>$ 36,460</td>
      <td>9.0 %</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Socks</td>
      <td>White Striped Socks - Mens</td>
      <td>$ 62,135</td>
      <td>20.2 %</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Socks</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>$ 109,330</td>
      <td>35.5 %</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Socks</td>
      <td>Navy Solid Socks - Mens</td>
      <td>$ 136,512</td>
      <td>44.3 %</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 7. What is the percentage split of revenue by segment for each category?


```python
pd.read_sql("""
SELECT 
    pd.category_name AS category, 
    pd.segment_name AS segment, 
    TO_CHAR(SUM(s.qty * s.price), 'FM$ 999,999') AS total_revenue,
    CONCAT(ROUND(SUM(s.qty * s.price)/SUM(SUM(s.qty * s.price)) OVER (PARTITION BY pd.category_name)::NUMERIC * 100, 1), ' %') AS percent_split
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.category_name, pd.segment_name
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>segment</th>
      <th>total_revenue</th>
      <th>percent_split</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mens</td>
      <td>Socks</td>
      <td>$ 307,977</td>
      <td>43.1 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Mens</td>
      <td>Shirt</td>
      <td>$ 406,143</td>
      <td>56.9 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Womens</td>
      <td>Jeans</td>
      <td>$ 208,350</td>
      <td>36.2 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Womens</td>
      <td>Jacket</td>
      <td>$ 366,983</td>
      <td>63.8 %</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 8. What is the percentage split of total revenue by category?


```python
pd.read_sql("""
SELECT 
    pd.category_name AS category, 
    TO_CHAR(SUM(s.qty * s.price), 'FM$ 999,999') AS total_revenue,
    CONCAT(ROUND(SUM(s.qty * s.price)/(SELECT SUM(qty * price) FROM balanced_tree.sales)::NUMERIC * 100, 1), ' %') AS percent_split
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.category_name
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>total_revenue</th>
      <th>percent_split</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mens</td>
      <td>$ 714,120</td>
      <td>55.4 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Womens</td>
      <td>$ 575,333</td>
      <td>44.6 %</td>
    </tr>
  </tbody>
</table>
</div>

**Result**</br>
Out of the total sales, 55.4% comes from the **Men's category**, while 44.6% comes from the **Women's category**.

___
#### 9. What is the total transaction `“penetration”` for *each product*? (hint: penetration = number of transactions where at least 1 quantity of a product was purchased divided by total number of transactions)


```python
pd.read_sql("""
SELECT 
    pd.product_name, 
    COUNT(txn_id) AS nb_transactions,
    CONCAT(ROUND(COUNT(txn_id)/(SELECT COUNT(DISTINCT txn_id) FROM balanced_tree.sales)::NUMERIC*100,2), ' %') AS penetration_percent
FROM balanced_tree.sales s
JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
GROUP BY pd.product_name
ORDER BY penetration_percent DESC
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>nb_transactions</th>
      <th>penetration_percent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Navy Solid Socks - Mens</td>
      <td>1281</td>
      <td>51.24 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>1275</td>
      <td>51.00 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Navy Oversized Jeans - Womens</td>
      <td>1274</td>
      <td>50.96 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>White Tee Shirt - Mens</td>
      <td>1268</td>
      <td>50.72 %</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Blue Polo Shirt - Mens</td>
      <td>1268</td>
      <td>50.72 %</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>1258</td>
      <td>50.32 %</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Indigo Rain Jacket - Womens</td>
      <td>1250</td>
      <td>50.00 %</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Khaki Suit Jacket - Womens</td>
      <td>1247</td>
      <td>49.88 %</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Black Straight Jeans - Womens</td>
      <td>1246</td>
      <td>49.84 %</td>
    </tr>
    <tr>
      <th>9</th>
      <td>White Striped Socks - Mens</td>
      <td>1243</td>
      <td>49.72 %</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>1243</td>
      <td>49.72 %</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Teal Button Up Shirt - Mens</td>
      <td>1242</td>
      <td>49.68 %</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 10. What is the most common combination of at least 1 quantity of any 3 products in a 1 single transaction?

This question involves combinations. Let's recall the combination equation: </br> 	

$$\frac{n!}{k!(n-k)!}$$

where </br>
- $n$ represents the n objects in a set (in this case, there is a total of 12 products at the Balanced Tree company: $n=12$)
- $k$ represents the number of distinct objects in each combination (here, we need 3 distinct products in each combination: $k=3$)

Hence, there are `220 possible combinations of 3 different items`:

$$\frac{n!}{k!(n-k)!} = \frac{12!}{3!(12-3)!} = \frac{12!}{3! * 9!} = \frac{12 * 11 * 10}{3 * 2 * 1} = 4 * 5 * 11 = 220$$


To answer this question, we will employ `SELF JOINS` to generate the possible combinations. This approach yields `220 rows`, indicating a total of 220 combinations of 3 distinct items.


```python
query10 = """
WITH products_cte AS
(
    -- Filter the data to include only those txn_ids that have at least 3 products in a single transaction
    
    SELECT 
        s.txn_id,
        pd.product_name
    FROM
    (
        SELECT 
            txn_id, 
            COUNT(prod_id) AS nb_products
        FROM balanced_tree.sales 
        GROUP BY txn_id
    
    ) count_products
    JOIN balanced_tree.sales s ON count_products.txn_id = s.txn_id
    JOIN balanced_tree.product_details pd ON s.prod_id = pd.product_id
    WHERE nb_products >= 3
)
SELECT 
    p.product_name AS product1, 
    p1.product_name AS product2, 
    p2.product_name AS product3, 
    COUNT(p.txn_id) AS nb_occurences     -- Count the occurrences of each combination
FROM products_cte p
JOIN products_cte p1                     -- SELF JOIN Table 1 to Table 2
ON p.txn_id = p1.txn_id 
AND p.product_name != p1.product_name    -- Ensure that items are not duplicated
AND p.product_name < p1.product_name
JOIN products_cte p2                     -- SELF JOIN Table 1 to Table 3
ON p.txn_id = p2.txn_id 
AND p.product_name != p2.product_name    -- Ensure that items are not duplicated in Table 1
AND p1.product_name != p2.product_name   -- Ensure that items are not duplicated in Table 2
AND p.product_name < p2.product_name
AND p1.product_name < p2.product_name
GROUP BY p.product_name, p1.product_name, p2.product_name
ORDER BY nb_occurences DESC
"""

pd.read_sql(query10, conn)
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product1</th>
      <th>product2</th>
      <th>product3</th>
      <th>nb_occurences</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>White Tee Shirt - Mens</td>
      <td>352</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Black Straight Jeans - Womens</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>Navy Solid Socks - Mens</td>
      <td>349</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Black Straight Jeans - Womens</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>347</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Blue Polo Shirt - Mens</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>White Striped Socks - Mens</td>
      <td>347</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Navy Oversized Jeans - Womens</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>White Tee Shirt - Mens</td>
      <td>347</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>215</th>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>White Tee Shirt - Mens</td>
      <td>290</td>
    </tr>
    <tr>
      <th>216</th>
      <td>Indigo Rain Jacket - Womens</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>289</td>
    </tr>
    <tr>
      <th>217</th>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>White Striped Socks - Mens</td>
      <td>White Tee Shirt - Mens</td>
      <td>288</td>
    </tr>
    <tr>
      <th>218</th>
      <td>Indigo Rain Jacket - Womens</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>287</td>
    </tr>
    <tr>
      <th>219</th>
      <td>Black Straight Jeans - Womens</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>283</td>
    </tr>
  </tbody>
</table>
<p>220 rows × 4 columns</p>
</div>



**Result**</br>
The following output is the most common combination of 3 distinct items bougth in a single transaction.
- Grey Fashion Jacket - Womens
- Teal Button Up Shirt - Mens
- White Tee Shirt - Mens


```python
pd.read_sql(query10 + 'LIMIT 1', conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product1</th>
      <th>product2</th>
      <th>product3</th>
      <th>nb_occurences</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Grey Fashion Jacket - Womens</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>White Tee Shirt - Mens</td>
      <td>352</td>
    </tr>
  </tbody>
</table>
</div>




<a id = 'D'></a>
## D. Bonus Challenge
Use a single SQL query to transform the `product_hierarchy` and `product_prices` datasets to the `product_details` table.

Hint: you may want to consider using a recursive CTE to solve this problem!

Let's recall the `product_details` table.
<details>
<summary>View table</summary>

```python
pd.read_sql("""
SELECT *
FROM balanced_tree.product_details
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>price</th>
      <th>product_name</th>
      <th>category_id</th>
      <th>segment_id</th>
      <th>style_id</th>
      <th>category_name</th>
      <th>segment_name</th>
      <th>style_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c4a632</td>
      <td>13</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>7</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Navy Oversized</td>
    </tr>
    <tr>
      <th>1</th>
      <td>e83aa3</td>
      <td>32</td>
      <td>Black Straight Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>8</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Black Straight</td>
    </tr>
    <tr>
      <th>2</th>
      <td>e31d39</td>
      <td>10</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>9</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Cream Relaxed</td>
    </tr>
    <tr>
      <th>3</th>
      <td>d5e9a6</td>
      <td>23</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>10</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Khaki Suit</td>
    </tr>
    <tr>
      <th>4</th>
      <td>72f5d4</td>
      <td>19</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>11</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Indigo Rain</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9ec847</td>
      <td>54</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>12</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Grey Fashion</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5d267b</td>
      <td>40</td>
      <td>White Tee Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>13</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>White Tee</td>
    </tr>
    <tr>
      <th>7</th>
      <td>c8d436</td>
      <td>10</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>14</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Teal Button Up</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2a2353</td>
      <td>57</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>15</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Blue Polo</td>
    </tr>
    <tr>
      <th>9</th>
      <td>f084eb</td>
      <td>36</td>
      <td>Navy Solid Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>16</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Navy Solid</td>
    </tr>
    <tr>
      <th>10</th>
      <td>b9a74d</td>
      <td>17</td>
      <td>White Striped Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>17</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>White Striped</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2feb6b</td>
      <td>29</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>18</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Pink Fluro Polkadot</td>
    </tr>
  </tbody>
</table>
</div>

</details>

</br>
</br>

**Result**</br>
I used 2 CTEs to address this question:
- `category_cte`: This CTE retrieves the initial data for category items with the renamed columns. 
- `segment_cte`: This CTE retrieves the initial data for segment items with the renamed columns.


```python
pd.read_sql("""
WITH category_cte AS
(
    -- Retrieve data for category items
    
    SELECT 
        id AS category_id, 
        level_text AS category_name
    FROM balanced_tree.product_hierarchy 
    WHERE level_name = 'Category'
), 

segment_cte AS
(
    -- Retrieve data for segment items
    
    SELECT 
        cat.category_id,
        ph.id AS segment_id, 
        cat.category_name,
        ph.level_text AS segment_name
    FROM balanced_tree.product_hierarchy ph
    JOIN category_cte cat ON ph.parent_id = cat.category_id
    WHERE level_name = 'Segment'
)

SELECT
    pp.product_id,
    pp.price,
    CONCAT(ph.level_text, ' ', segment_name, ' - ', category_name) AS product_name,
    category_id,
    segment_id, 
    ph.id AS style_id,
    category_name,
    segment_name,
    ph.level_text AS style_name

FROM balanced_tree.product_hierarchy ph
JOIN balanced_tree.product_prices pp ON ph.id = pp.id
LEFT JOIN segment_cte seg ON ph.parent_id = seg.segment_id
WHERE level_name = 'Style'
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>price</th>
      <th>product_name</th>
      <th>category_id</th>
      <th>segment_id</th>
      <th>style_id</th>
      <th>category_name</th>
      <th>segment_name</th>
      <th>style_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c4a632</td>
      <td>13</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>7</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Navy Oversized</td>
    </tr>
    <tr>
      <th>1</th>
      <td>e83aa3</td>
      <td>32</td>
      <td>Black Straight Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>8</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Black Straight</td>
    </tr>
    <tr>
      <th>2</th>
      <td>e31d39</td>
      <td>10</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>9</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Cream Relaxed</td>
    </tr>
    <tr>
      <th>3</th>
      <td>d5e9a6</td>
      <td>23</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>10</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Khaki Suit</td>
    </tr>
    <tr>
      <th>4</th>
      <td>72f5d4</td>
      <td>19</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>11</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Indigo Rain</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9ec847</td>
      <td>54</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>12</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Grey Fashion</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5d267b</td>
      <td>40</td>
      <td>White Tee Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>13</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>White Tee</td>
    </tr>
    <tr>
      <th>7</th>
      <td>c8d436</td>
      <td>10</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>14</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Teal Button Up</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2a2353</td>
      <td>57</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>15</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Blue Polo</td>
    </tr>
    <tr>
      <th>9</th>
      <td>f084eb</td>
      <td>36</td>
      <td>Navy Solid Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>16</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Navy Solid</td>
    </tr>
    <tr>
      <th>10</th>
      <td>b9a74d</td>
      <td>17</td>
      <td>White Striped Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>17</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>White Striped</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2feb6b</td>
      <td>29</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>18</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Pink Fluro Polkadot</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, we can achieve the same result without using CTEs. Instead, we can obtain the desired outcome by utilizing `SELF JOINS` in the query.


```python
pd.read_sql("""
SELECT 
    pp.product_id,
    pp.price,
    CONCAT(ph2.level_text, ' ', ph1.level_text, ' - ', ph.level_text) AS product_name,
    ph.id AS category_id,
    ph1.id AS segment_id,
    ph2.id AS style_id,
    ph.level_text AS category_name,
    ph1.level_text AS segment_name,
    ph2.level_text AS style_name

FROM balanced_tree.product_hierarchy ph
JOIN balanced_tree.product_hierarchy ph1 ON ph.id = ph1.parent_id  -- SELF JOIN Table 1 to Table 2
JOIN balanced_tree.product_hierarchy ph2 ON ph1.id = ph2.parent_id -- SELF JOIN Table 2 to Table 3
LEFT JOIN balanced_tree.product_prices pp ON ph2.id = pp.id        -- LEFT JOIN Table to the "product_price" table
ORDER BY category_id, segment_id, style_id
""", conn) 
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>price</th>
      <th>product_name</th>
      <th>category_id</th>
      <th>segment_id</th>
      <th>style_id</th>
      <th>category_name</th>
      <th>segment_name</th>
      <th>style_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c4a632</td>
      <td>13</td>
      <td>Navy Oversized Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>7</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Navy Oversized</td>
    </tr>
    <tr>
      <th>1</th>
      <td>e83aa3</td>
      <td>32</td>
      <td>Black Straight Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>8</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Black Straight</td>
    </tr>
    <tr>
      <th>2</th>
      <td>e31d39</td>
      <td>10</td>
      <td>Cream Relaxed Jeans - Womens</td>
      <td>1</td>
      <td>3</td>
      <td>9</td>
      <td>Womens</td>
      <td>Jeans</td>
      <td>Cream Relaxed</td>
    </tr>
    <tr>
      <th>3</th>
      <td>d5e9a6</td>
      <td>23</td>
      <td>Khaki Suit Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>10</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Khaki Suit</td>
    </tr>
    <tr>
      <th>4</th>
      <td>72f5d4</td>
      <td>19</td>
      <td>Indigo Rain Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>11</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Indigo Rain</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9ec847</td>
      <td>54</td>
      <td>Grey Fashion Jacket - Womens</td>
      <td>1</td>
      <td>4</td>
      <td>12</td>
      <td>Womens</td>
      <td>Jacket</td>
      <td>Grey Fashion</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5d267b</td>
      <td>40</td>
      <td>White Tee Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>13</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>White Tee</td>
    </tr>
    <tr>
      <th>7</th>
      <td>c8d436</td>
      <td>10</td>
      <td>Teal Button Up Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>14</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Teal Button Up</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2a2353</td>
      <td>57</td>
      <td>Blue Polo Shirt - Mens</td>
      <td>2</td>
      <td>5</td>
      <td>15</td>
      <td>Mens</td>
      <td>Shirt</td>
      <td>Blue Polo</td>
    </tr>
    <tr>
      <th>9</th>
      <td>f084eb</td>
      <td>36</td>
      <td>Navy Solid Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>16</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Navy Solid</td>
    </tr>
    <tr>
      <th>10</th>
      <td>b9a74d</td>
      <td>17</td>
      <td>White Striped Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>17</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>White Striped</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2feb6b</td>
      <td>29</td>
      <td>Pink Fluro Polkadot Socks - Mens</td>
      <td>2</td>
      <td>6</td>
      <td>18</td>
      <td>Mens</td>
      <td>Socks</td>
      <td>Pink Fluro Polkadot</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```
