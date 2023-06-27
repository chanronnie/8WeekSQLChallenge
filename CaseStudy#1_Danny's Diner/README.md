# Data With Danny [#8WeekSQLChallenge](https://8weeksqlchallenge.com/)
# About `Case Study #1: Danny's Diner`

The case study questions presented here are brought to you by *Data With Danny*.\
My SQL queries are written in the `MySQL` language, integrated into Jupyter Notebook, which allows us to instantly view the query results.\
If you need further explanations about the tables (such as *Entity Relationship Diagram*) related to Case Study #1, please refer to the DataWithDanny website by clicking [here](https://8weeksqlchallenge.com/case-study-1/).
___

### Importing necessary librairies


```python
import os
import pymysql
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
```

### Connecting to MySQL database from Jupyter Notebook


```python
# Get MySQL password from the environment variables
mypassword = os.getenv('MYSQL_PASSWORD')

# Connecting to MySQL database from Jupyter Notebook
conn = pymysql.connect(host="localhost", user = 'root', passwd = mypassword, db = 'dannys_diner')
mycursor = conn.cursor()



# Here are the tables within the "dannys_diner" database.
mycursor.execute("SHOW TABLES")
print('--- Tables within "dannys_diner" database --- ')
for table in mycursor:
    print(table[0])
```

    --- Tables within "dannys_diner" database --- 
    members
    menu
    sales
    

Now, let's visualize each table to gain a general overview. 

**Table 1: sales**\
The `sales` table captures all the order information (`order_date` and `product_id`) of each customer (`customer_id`).


```python
pd.read_sql_query("SELECT * FROM dannys_diner.sales", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A</td>
      <td>2021-01-10</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>B</td>
      <td>2021-01-02</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>B</td>
      <td>2021-01-04</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>B</td>
      <td>2021-01-11</td>
      <td>1</td>
    </tr>
    <tr>
      <th>10</th>
      <td>B</td>
      <td>2021-01-16</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>B</td>
      <td>2021-02-01</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>3</td>
    </tr>
    <tr>
      <th>14</th>
      <td>C</td>
      <td>2021-01-07</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**Table 2: menu**\
The `menu` table lists the IDs, names and prices of each menu item.


```python
pd.read_sql_query("SELECT * FROM dannys_diner.menu", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>product_name</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>sushi</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>curry</td>
      <td>15</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>ramen</td>
      <td>12</td>
    </tr>
  </tbody>
</table>
</div>



**Table 3: members**\
The `members` table captures the dates (`join_date`) when each customer joined the member program.


```python
pd.read_sql_query("SELECT * FROM dannys_diner.members", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>join_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-07</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>2021-01-09</td>
    </tr>
  </tbody>
</table>
</div>



**Insights:**
- The `sales` table has `customer_id` as a PRIMARY KEY (PK) and `product_id` as a FOREIGN KEY (FK).
- The `menu` table has `product_id` as a PRIMARY KEY (PK).
- The `members` table has `customer_id` as a PRIMARY KEY (PK).
- Thus, the `sales` table and the `menu` table can be connected through the common key `product_id`.
- The `sales` table and the `members` table share the common key `customer_id`. 

____
## Case Study Questions

#### 1. What is the total amount each customer spent at the restaurant?


```python
pd.read_sql_query("""
SELECT 
    customer_id, 
    SUM(price) as total_amount_spent
FROM dannys_diner.sales s
JOIN dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY customer_id;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>total_amount_spent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>36.0</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER**:
- **Customers A** spent the most at Danny's Diner, with a total of `$76.00`.
- Followed by **Customers B**, who spent a total of `$74.00`.
- **Customers C** spent the least at Danny's Diner, with a total of `$36.00`.

___
#### 2. How many days has each customer visited the restaurant?


```python
pd.read_sql_query("""
SELECT 
    customer_id, 
    COUNT(DISTINCT(order_date)) as number_of_visits
FROM dannys_diner.sales
GROUP BY customer_id;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>number_of_visits</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- **Customers A** visited Danny's Diner 4 times.
- **Customers B** visited Danny's Diner 6 times.
- **Customers C** visited Danny's Diner 2 times.

___
#### 3. What was the first item from the menu purchased by each customer?


```python
pd.read_sql_query("""
WITH occurences AS
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as nth_item
    FROM dannys_diner.sales
)
SELECT o.customer_id, o.order_date, m.product_name
FROM occurences o 
JOIN dannys_diner.menu m ON o.product_id = m.product_id
WHERE nth_item = 1;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>sushi</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- **Customer A**'s first order is sushi.
- **Customer B**'s first order is curry.
- **Customer C**'s first order is ramen.

___
#### 4. What is the most purchased item on the menu and how many times was it purchased by all customers?


```python
query = """
SELECT 
    m.product_name, 
    COUNT(m.product_name) AS nb_purchases
FROM dannys_diner.sales s
JOIN dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY m.product_name
ORDER BY nb_purchases DESC;
"""

pd.read_sql_query(query, conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>nb_purchases</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ramen</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>curry</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>sushi</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- `Ramen` is the most frequently purchased item from Danny's Diner menu, with a total of 8 orders in January 2021.
- It is followed by `curry`, which has been ordered 4 times.
- The least popular item is `sushi`, with only 3 purchases.

Alternatively, to specifically answer the given question and retrieve the most purchased item on the menu, which is **ramen** ordered 8 times in total, you can add the statement `LIMIT 1` at the end of the query.


```python
pd.read_sql_query(query.replace(';', '') + "LIMIT 1;", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>nb_purchases</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ramen</td>
      <td>8</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. Which item was the most popular for each customer?


```python
pd.read_sql_query("""
SELECT Mp.customer_id, Mp.product_name, Mp.order_count
FROM
(
    SELECT 
        s.customer_id,
        m.product_name,
        COUNT(m.product_name) AS order_count,
        RANK() OVER (PARTITION BY s.customer_id ORDER BY COUNT(s.customer_id) DESC) AS ranking
    
    FROM dannys_diner.sales s
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
    GROUP BY s.customer_id, m.product_name
) Mp
WHERE Mp.ranking = 1;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>product_name</th>
      <th>order_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>ramen</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>curry</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>B</td>
      <td>sushi</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B</td>
      <td>ramen</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>C</td>
      <td>ramen</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- `Ramen` is the most popular item for both **customers A and C** at Danny's Diner.
- For **customer B**, it seems that all items from the Danny's Diner menu (ramen, curry, sushi) are equally liked.

___
#### 6. Which item was purchased first by the customer *AFTER* they became a member?


```python
pd.read_sql_query("""
WITH the_members AS
(
    SELECT 
        s.customer_id, 
        s.order_date, 
        mb.join_date, 
        m.product_name, 
        ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.order_date) AS row_nb
    FROM dannys_diner.sales s 
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
    JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id AND s.order_date >= mb.join_date
)
SELECT 
    customer_id, 
    join_date,
    order_date, 
    product_name
FROM the_members 
WHERE row_nb=1;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>join_date</th>
      <th>order_date</th>
      <th>product_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>2021-01-07</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>2021-01-09</td>
      <td>2021-01-11</td>
      <td>sushi</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- Customers A and B are the only clients who became members of Danny's Diner.
- **Customer A** ordered a `curry` on the same day he/she joined the member program, on January 7, 20201.
- The first item ordered by **customer B** after becoming a member was `sushi`.

___
#### 7. Which item was purchased just *BEFORE* the customer became a member?


```python
pd.read_sql_query("""
WITH before_member AS
(
    SELECT 
        s.customer_id, 
        s.order_date, 
        mb.join_date, 
        m.product_name, 
        DENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.order_date DESC) AS nth_item
    FROM dannys_diner.sales s
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
    JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id AND s.order_date < mb.join_date
)
SELECT 
    customer_id, 
    join_date, 
    order_date,  
    product_name
FROM before_member
WHERE nth_item = 1;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>join_date</th>
      <th>order_date</th>
      <th>product_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>2021-01-01</td>
      <td>sushi</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>2021-01-01</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>2</th>
      <td>B</td>
      <td>2021-01-09</td>
      <td>2021-01-04</td>
      <td>sushi</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- Just before becoming a member, **customer A** has ordered `sushi` and `curry`.
- Similarly, **customer B** has ordered `sushi` before joining the program.

___
#### 8. What is the total items and amount spent for each member *BEFORE* they became a member?


```python
pd.read_sql_query("""
SELECT 
    s.customer_id, 
    COUNT(m.product_name) AS total_items, 
    SUM(m.price) AS total_spent
FROM dannys_diner.sales s
JOIN dannys_diner.menu m ON s.product_id = m.product_id
JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id AND s.order_date < mb.join_date
GROUP BY s.customer_id
ORDER BY s.customer_id;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>total_items</th>
      <th>total_spent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2</td>
      <td>25.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>3</td>
      <td>40.0</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- Before becoming a member, **customer A** had ordered `2 items` from Danny's Diner, totaling `$25.00`.
- Similarly, **customer B** had ordered `3 items` from the restaurant, with a total cost of `$40.00`, prior to joining the membership program.

___
#### 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

Note that `MySQL aggregate functions` can sometimes return unexpected **decimal values** even if the variables are integers\
To address this and achieve the desired result, let's convert the `SUM(points)` column to an `UNSIGNED` integer using the `CAST` function to remove any decimal values.

`
CAST(SUM(points) AS UNSIGNED)
`


```python
pd.read_sql_query("""
WITH members_data AS
(
    SELECT 
        s.customer_id,
        m.price,
        CASE WHEN product_name = 'sushi' THEN price*20 ELSE price*10 END AS points
    FROM dannys_diner.sales s
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
)
SELECT 
    customer_id, 
    CAST(SUM(points) AS UNSIGNED) AS total_points
FROM members_data
GROUP BY customer_id;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>total_points</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>860</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>940</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>360</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- **Customer A** has accumulated a total of `860 points`.
- **Customer B** has earned the highest number of points, with a total of `940`.
- Among all customers, **Customer C** has the lowest number of points, with only `360 points`.

___
#### 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?


```python
pd.read_sql_query("""
WITH members_data AS
(
    SELECT 
        s.customer_id, 
        mb.join_date, 
        s.order_date, 
        m.product_name, 
        m.price,
        CASE WHEN 
            m.product_name = 'sushi' OR s.order_date BETWEEN mb.join_date AND mb.join_date + INTERVAL 6 DAY 
            THEN m.price*20 
            ELSE m.price*10 
        END AS points
        
    FROM dannys_diner.sales s
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
    JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id
    WHERE s.order_date < "2021-02-01"
)
SELECT 
    customer_id, 
    CAST(SUM(points) AS UNSIGNED) AS total_points
FROM members_data
GROUP BY customer_id
ORDER BY customer_id;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>total_points</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>1370</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>820</td>
    </tr>
  </tbody>
</table>
</div>



**ANSWER:**
- **Customer A** has accumulated the highest number of points, with a total of `1,370`.
- **Customer B** has a total of `820 points` with the member program.

___
## Bonus Questions

For this **Bonus Questions section**, we are required to recreate the tables displayed in [Data With Danny website](https://8weeksqlchallenge.com/case-study-1/)

#### Join All The Things


```python
pd.read_sql_query("""
SELECT 
    s.customer_id, 
    s.order_date, 
    m.product_name, 
    m.price, 
    CASE WHEN s.order_date >= mb.join_date THEN "Y" ELSE "N" END AS member
FROM dannys_diner.sales s
JOIN dannys_diner.menu m ON s.product_id = m.product_id
LEFT JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id
ORDER BY s.customer_id, s.order_date;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_name</th>
      <th>price</th>
      <th>member</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>curry</td>
      <td>15</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A</td>
      <td>2021-01-10</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>7</th>
      <td>B</td>
      <td>2021-01-02</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>8</th>
      <td>B</td>
      <td>2021-01-04</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
    </tr>
    <tr>
      <th>9</th>
      <td>B</td>
      <td>2021-01-11</td>
      <td>sushi</td>
      <td>10</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>10</th>
      <td>B</td>
      <td>2021-01-16</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>11</th>
      <td>B</td>
      <td>2021-02-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>12</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
    <tr>
      <th>13</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
    <tr>
      <th>14</th>
      <td>C</td>
      <td>2021-01-07</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
  </tbody>
</table>
</div>



#### Rank All The Things


```python
pd.read_sql("""
WITH ranking_cte AS
(
    SELECT 
        s.customer_id, 
        s.order_date, 
        m.product_name, 
        m.price, 
        CASE WHEN s.order_date >= mb.join_date THEN "Y" ELSE "N" END AS member
    FROM dannys_diner.sales s
    JOIN dannys_diner.menu m ON s.product_id = m.product_id
    LEFT JOIN dannys_diner.members mb ON s.customer_id = mb.customer_id
    ORDER BY s.customer_id, s.order_date
)
SELECT 
    *, 
    CASE WHEN member = "Y" THEN DENSE_RANK() OVER (PARTITION BY customer_id, member ORDER BY order_date) 
    ELSE "NULL"
    END AS ranking
FROM ranking_cte;
""", conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_name</th>
      <th>price</th>
      <th>member</th>
      <th>ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>curry</td>
      <td>15</td>
      <td>Y</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A</td>
      <td>2021-01-10</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>7</th>
      <td>B</td>
      <td>2021-01-02</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>8</th>
      <td>B</td>
      <td>2021-01-04</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>9</th>
      <td>B</td>
      <td>2021-01-11</td>
      <td>sushi</td>
      <td>10</td>
      <td>Y</td>
      <td>1</td>
    </tr>
    <tr>
      <th>10</th>
      <td>B</td>
      <td>2021-01-16</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>2</td>
    </tr>
    <tr>
      <th>11</th>
      <td>B</td>
      <td>2021-02-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>13</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>14</th>
      <td>C</td>
      <td>2021-01-07</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```


```python

```
