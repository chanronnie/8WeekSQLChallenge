# Case Study #3: Foodie-Fi
The case study questions presented here are created by [**Data With Danny**](https://linktr.ee/datawithdanny). They are part of the [**8 WeekSQL Challenge**](https://8weeksqlchallenge.com/).

My SQL queries are written in the `MySQL` language, integrated into `Jupyter Notebook`, which allows us to instantly view the query results and document the queries.

For more details about the **Case Study #3**, click [**here**](https://8weeksqlchallenge.com/case-study-3/).

## Table of Contents
### [1. Importing Libraries](#Import)
### [2. Tables of the Database](#Tables)
### [3. Case Study Questions](#CaseStudyQuestions)

- [A. Customer Journey](#A)
- [B. Data Analysis Questions](#B)
- [C. Challenge Payment Question](#C)


<a id="Import"></a>
## 1. Importing required libraries 


```python
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings

warnings.filterwarnings('ignore')
sns.set_style("whitegrid")
plt.rcParams.update({'text.color' : "Green",
                     'axes.labelcolor' : "Green"})
```


<a id="Tables"></a>
## 2. Tables of the Database

### Connecting MySQL database from Jupyter Notebook


```python
# Get the MySQL password 
password = os.getenv("MYSQL_PASSWORD")

# Connect MySQL database
conn = pymysql.connect(host = "localhost", user = "root", passwd = password, db = "foodie_fi")
mycursor = conn.cursor()

# display the tables within the 'foodie_fi' database
mycursor.execute("SHOW TABLES;")
print('--- Tables within "foodie_di" database --- ')
for table in mycursor:
    print(table[0])
```

    --- Tables within "foodie_di" database --- 
    plans
    subscriptions
    

The followings are the 2 tables within the `foodie_fi` database. Please click [here](https://8weeksqlchallenge.com/case-study-3/) to get more insights about the tables.


```python
mycursor.execute("SHOW TABLES;")
for table in mycursor:
    print("Table: ", table[0])
    query = "SELECT * FROM " + table[0]
    df = pd.read_sql(query, conn)
    display(df)
```

    Table:  plans

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>plan_id</th>
      <th>plan_name</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>trial</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>basic monthly</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>pro monthly</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>pro annual</td>
      <td>199.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>churn</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  subscriptions
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_id</th>
      <th>start_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0</td>
      <td>2020-08-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>2020-08-08</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>0</td>
      <td>2020-09-20</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>3</td>
      <td>2020-09-27</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>0</td>
      <td>2020-01-13</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2645</th>
      <td>999</td>
      <td>2</td>
      <td>2020-10-30</td>
    </tr>
    <tr>
      <th>2646</th>
      <td>999</td>
      <td>4</td>
      <td>2020-12-01</td>
    </tr>
    <tr>
      <th>2647</th>
      <td>1000</td>
      <td>0</td>
      <td>2020-03-19</td>
    </tr>
    <tr>
      <th>2648</th>
      <td>1000</td>
      <td>2</td>
      <td>2020-03-26</td>
    </tr>
    <tr>
      <th>2649</th>
      <td>1000</td>
      <td>4</td>
      <td>2020-06-04</td>
    </tr>
  </tbody>
</table>
<p>2650 rows × 3 columns</p>
</div>


<a id="CaseStudyQuestions"></a>
## 3. Case Study Questions

<a id="A"></a>
## A. Customer Journey

Based off the 8 sample customers provided in the sample from the subscriptions table, write a brief description about each customer’s onboarding journey.\
Try to keep it as short as possible - you may also want to run some sort of join to make your explanations a bit easier!


```python
pd.read_sql("""
SELECT 
    s.customer_id, 
    p.plan_name, 
    p.price,
    s.start_date
FROM foodie_fi.subscriptions s
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE s.customer_id IN (1, 2, 11, 13, 15, 16, 18, 19)
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_name</th>
      <th>price</th>
      <th>start_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-08-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>basic monthly</td>
      <td>9.9</td>
      <td>2020-08-08</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-09-20</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>pro annual</td>
      <td>199.0</td>
      <td>2020-09-27</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-11-19</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11</td>
      <td>churn</td>
      <td>NaN</td>
      <td>2020-11-26</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-12-15</td>
    </tr>
    <tr>
      <th>7</th>
      <td>13</td>
      <td>basic monthly</td>
      <td>9.9</td>
      <td>2020-12-22</td>
    </tr>
    <tr>
      <th>8</th>
      <td>13</td>
      <td>pro monthly</td>
      <td>19.9</td>
      <td>2021-03-29</td>
    </tr>
    <tr>
      <th>9</th>
      <td>15</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-03-17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>15</td>
      <td>pro monthly</td>
      <td>19.9</td>
      <td>2020-03-24</td>
    </tr>
    <tr>
      <th>11</th>
      <td>15</td>
      <td>churn</td>
      <td>NaN</td>
      <td>2020-04-29</td>
    </tr>
    <tr>
      <th>12</th>
      <td>16</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-05-31</td>
    </tr>
    <tr>
      <th>13</th>
      <td>16</td>
      <td>basic monthly</td>
      <td>9.9</td>
      <td>2020-06-07</td>
    </tr>
    <tr>
      <th>14</th>
      <td>16</td>
      <td>pro annual</td>
      <td>199.0</td>
      <td>2020-10-21</td>
    </tr>
    <tr>
      <th>15</th>
      <td>18</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-07-06</td>
    </tr>
    <tr>
      <th>16</th>
      <td>18</td>
      <td>pro monthly</td>
      <td>19.9</td>
      <td>2020-07-13</td>
    </tr>
    <tr>
      <th>17</th>
      <td>19</td>
      <td>trial</td>
      <td>0.0</td>
      <td>2020-06-22</td>
    </tr>
    <tr>
      <th>18</th>
      <td>19</td>
      <td>pro monthly</td>
      <td>19.9</td>
      <td>2020-06-29</td>
    </tr>
    <tr>
      <th>19</th>
      <td>19</td>
      <td>pro annual</td>
      <td>199.0</td>
      <td>2020-08-29</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- **`Customer 1`** joined Foodie-Fi on 2020-08-01 for a free trial and subscribed to the basic monthly plan on 2020-08-08, at the end of the 7-day free trial, with a fee of $9.90.


- **`Customer 2`** joined Foodie-Fi on 2020-09-20 for a free trial and subscribed to the pro annual plan on 2020-09-27, at the end of the 7-day free trial, with a fee of $199.00.


- **`Customer 11`** joined Foodie-Fi on 2020-11-19 for a free trial and cancelled the service on 2020-11-26 rigth after the end of the 7-day free trial.


- **`Customer 13`** joined Foodie-Fi on 2020-12-15 for a free trial, subscribed to the basic monthly plan on 2020-12-22, at the end of the 7-day free trial, with a fee of $9.90, and upgraded to a pro monthly plan on 2021-03-29 for a price of $19.90.


- **`Customer 15`** joined Foodie-Fi on 2020-03-17 for a free trial, subscribed to the pro monthly plan on 2020-03-24, at the end of the 7-day free trial, with a fee of $19.90, and then cancelled the service on 2020-04-29 (after a month).


- **`Customer 16`** joined Foodie-Fi on 2020-05-31 for a free trial, subscribed to the basic monthly plan on 2020-06-07, at the end of the 7-day free trial, with a fee of $9.90, and then upgraded to a pro annual plan on 2020-10-21 (after 4 months).


- **`Customer 18`** joined Foodie-Fi on 2020-07-06 for a free trial and subscribed to the pro monthly plan on 2020-07-13, at the end of the 7-day free trial, with a fee of $19.90.


- **`Customer 19`** joined Foodie-Fi on 2020-06-22 for a free trial, subscribed to the pro monthly plan on 2020-06-29, at the end of the 7-day free trial, with a fee of $19.90, and then upgraded to a pro annual plan on 2020-08-29 for a price of $199.00 (after 2 months).

___
<a id="B"></a>
## B. Data Analysis Questions 

#### 1. How many customers has Foodie-Fi ever had?


```python
pd.read_sql("""
SELECT COUNT(DISTINCT customer_id) AS nb_customers
FROM foodie_fi.subscriptions
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_customers</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
Foodie-Fi had 1000 customers.

___
#### 2.What is the monthly distribution of trial plan `start_date` values for our dataset - use the start of the month as the group by value.


```python
df2 = pd.read_sql("""
SELECT 
    MONTH(s.start_date) AS month, 
    MONTHNAME(s.start_date) AS month_name,
    COUNT(*) AS nb_trial
FROM foodie_fi.subscriptions s
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE p.plan_name = "trial"
GROUP BY month, month_name
ORDER BY month
""", conn)
df2
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>month_name</th>
      <th>nb_trial</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>January</td>
      <td>88</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>February</td>
      <td>68</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>March</td>
      <td>94</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>April</td>
      <td>81</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>May</td>
      <td>88</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>June</td>
      <td>79</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>July</td>
      <td>89</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>August</td>
      <td>88</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>September</td>
      <td>87</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>October</td>
      <td>79</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>November</td>
      <td>75</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>December</td>
      <td>84</td>
    </tr>
  </tbody>
</table>
</div>



**Result**


```python
df2.sort_values(by = "nb_trial", ascending= True, inplace = True)

fig, ax = plt.subplots(figsize = (8,4))
ax.barh(df2.month_name, df2.nb_trial, color = 'lightgreen')
ax.set(title = 'Trial Plan Monthly Distribution at Foodie-Fi', xlabel = 'Count', ylabel = 'Month');
```


    
![png](images/question2.png)
    


**Result**\
Based on the plotted bar graph of the SQL query results, the following observations can be made:
- The month with the highest number of new customers subscribing to the trial plan is `March`, with a total of 94 new subscriptions.
- Following March, the months of `July`, `August`, `May`, `January`, `September`, `December`, and `April` have descending order averages ranging from 81 to 89 subscriptions.
- The months of `October`, `June`, and `November` have an average number of new customers between 75 and 79.
- The month of `February` is the least popular for new customers to try the Foodie-Fi service.

___
#### 3.What plan `start_date` values occur *after the year 2020* for our dataset? Show the breakdown by count of events for each `plan_name`.


```python
df3 = pd.read_sql("""
SELECT 
    p.plan_name, 
    COUNT(p.plan_name) AS count
FROM foodie_fi.subscriptions s
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE YEAR(s.start_date) >= 2021
GROUP BY p.plan_name;
""", conn)
df3
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>plan_name</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>churn</td>
      <td>71</td>
    </tr>
    <tr>
      <th>1</th>
      <td>pro monthly</td>
      <td>60</td>
    </tr>
    <tr>
      <th>2</th>
      <td>pro annual</td>
      <td>63</td>
    </tr>
    <tr>
      <th>3</th>
      <td>basic monthly</td>
      <td>8</td>
    </tr>
  </tbody>
</table>
</div>



**Result**


```python
df3.sort_values(by = "count", ascending= True, inplace = True)

fig, ax = plt.subplots(figsize = (6,2))
ax.barh(df3.plan_name,df3['count'], color = 'lightgreen')
ax.set(title = 'Plan Distribution at Foodie-Fi after the year 2020', xlabel = 'Count', ylabel = 'Plan Name');
```


    
![png](images/question3.png)
    


Based on the plotted bar graph of the SQL query results, the following observations can be made:

- The number of customers who have `churned` the Foodie-Fi service is higher, with a total of 71 customers after the year 2020.
- Out of the customers, 63 have a `Pro Annual` plan, while 60 have a `Pro Monthly` plan.
- In contrast, only 8 customers have subscribed to the `Basic Monthly` plan.

___
#### 4.What is the customer count and percentage of customers who have churned rounded to 1 decimal place?


```python
pd.read_sql("""
WITH churn_cte AS
(
    SELECT 
        COUNT(s.customer_id) AS churn_count
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name = "churn"
)
SELECT 
    churn_count, 
    COUNT(DISTINCT s.customer_id) AS total_customers,
    CONCAT(ROUND(churn_count/COUNT(DISTINCT s.customer_id) * 100, 1), " %") AS percentage
FROM churn_cte, foodie_fi.subscriptions s
GROUP BY 1;
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>churn_count</th>
      <th>total_customers</th>
      <th>percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>307</td>
      <td>1000</td>
      <td>30.7 %</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
There are 307 customers (or 30.7 % of the Foodie-Fi customers) who have churned.

___
#### 5.How many customers have churned straight after their initial free trial - what percentage is this rounded to the nearest whole number?


```python
pd.read_sql("""
WITH postplans_cte AS
(
    SELECT 
        s.customer_id, 
        p.plan_name,  
        LEAD(p.plan_name) OVER (PARTITION BY s.customer_id ORDER BY s.plan_id) AS post_trial_plan
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
)
SELECT 
    COUNT(DISTINCT pp.customer_id) AS posttrial_churned_count, 
    COUNT(DISTINCT s.customer_id) AS total_customers, 
    CONCAT(ROUND(COUNT(DISTINCT pp.customer_id)/COUNT(DISTINCT s.customer_id) * 100, 0), " %") AS percentage
FROM postplans_cte pp, foodie_fi.subscriptions s
WHERE pp.plan_name = "trial" AND pp.post_trial_plan = "churn";
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>posttrial_churned_count</th>
      <th>total_customers</th>
      <th>percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>92</td>
      <td>1000</td>
      <td>9 %</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
There are 92 customers (or 9 % of the Foodie-Fi customers) who have churned straight after their initial free trial.

___
#### 6.What is the number and percentage of customer plans after their initial free trial?


```python
query6 = """
WITH postplans_cte AS
(
    SELECT 
        s.customer_id, 
        p.plan_name,
        LEAD(p.plan_name) OVER (PARTITION BY s.customer_id ORDER BY s.plan_id) AS post_trial_plan
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
),
total_customers_cte AS
(
    SELECT 
        COUNT(DISTINCT customer_id) AS total_customers
    FROM foodie_fi.subscriptions
)
SELECT 
    post_trial_plan, 
    COUNT(post_trial_plan) AS plan_count,
    CONCAT(ROUND(COUNT(post_trial_plan)/MAX(total_customers)*100, 1), ' %') AS percentage
FROM postplans_cte, total_customers_cte
WHERE plan_name = 'trial'
GROUP BY post_trial_plan
ORDER BY plan_count DESC;
"""

df6 = pd.read_sql(query6, conn)
df6
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>post_trial_plan</th>
      <th>plan_count</th>
      <th>percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>basic monthly</td>
      <td>546</td>
      <td>54.6 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>pro monthly</td>
      <td>325</td>
      <td>32.5 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>churn</td>
      <td>92</td>
      <td>9.2 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>pro annual</td>
      <td>37</td>
      <td>3.7 %</td>
    </tr>
  </tbody>
</table>
</div>



**Result**


```python
fig, ax = plt.subplots(figsize = (6,2))
ax.barh(df6.post_trial_plan, df6.plan_count, color = 'lightgreen')
ax.set(title = 'Foodie-Fi Plans Distribution After Initial Free Trial Plan', xlabel = 'Count', ylabel = 'Plan');
```


    
![png](images/question6.png)
    


- The `Basic Monthly` plan is the most popular one at Foodie-Fi after the 7-day free `trial` plan, with a total of 546 subscriptions, representing 54.6% of the Foodie-Fi customers (half of the customers).
- The `Pro Monthly` plan is the second most popular choice among customers, with a total of 325 subscriptions, accounting for 32.5% of the customers.
- There are 9.2% of customers who have cancelled the service (see `Churn` plan)
- As for the `Pro Annual` plan, only 37 customers, or 3.7% of the Foodie-Fi customers, have opted for this plan.

___
#### 7.What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31?


```python
query7 = """ 
WITH latest_plans_cte AS
(
    SELECT 
        s.customer_id, 
        s.plan_id, 
        p.plan_name, 
        s.start_date, 
        DENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.start_date DESC) AS rk
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE s.start_date <= '2020-12-31'
),
total_customers_cte AS
(
    SELECT 
        COUNT(DISTINCT customer_id) AS total_customers
    FROM foodie_fi.subscriptions
)
SELECT 
    plan_name, 
    COUNT(plan_name) AS count,
    CONCAT(ROUND(COUNT(plan_name) / MAX(total_customers) * 100, 1), ' %') AS percentage
FROM latest_plans_cte, total_customers_cte
WHERE rk = 1
GROUP BY plan_name
ORDER BY count DESC;
"""

df7 = pd.read_sql(query7, conn)
df7
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>plan_name</th>
      <th>count</th>
      <th>percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>pro monthly</td>
      <td>326</td>
      <td>32.6 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>churn</td>
      <td>236</td>
      <td>23.6 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>basic monthly</td>
      <td>224</td>
      <td>22.4 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>pro annual</td>
      <td>195</td>
      <td>19.5 %</td>
    </tr>
    <tr>
      <th>4</th>
      <td>trial</td>
      <td>19</td>
      <td>1.9 %</td>
    </tr>
  </tbody>
</table>
</div>



**Result**


```python
fig, ax = plt.subplots(figsize = (6,2))
ax.barh(df7['plan_name'], df7['count'], color = 'lightgreen')
ax.set(title = 'Foodie-Fi Plans Distribution After Initial Free Trial Plan', xlabel = 'Count', ylabel = 'Plan');
```


    
![png](images/question7.png)
    


Here is the subscription distribution report for the plans at Foodie-Fi as of December 31, 2020:
- The `Pro Monthly` plan is the most popular one at Foodie-Fi, with a total of 326 subscriptions, representing 32.6% of the customers.
- It is followed by the `Churn` and `Basic Monthly` plans, with a total of 236 and 224 subscriptions, respectively. This corresponds to 23.6% and 22.4% of Foodie-Fi customers, respectively.
- There are 195 customers, which accounts for 19.5% of the total customers, who upgraded to the `Pro Annual` plan.
- As for the `Trial` plan, only 19 customers, or 1.9% of the total customers, opted for this plan.

___
#### 8.How many customers have upgraded to an annual plan in 2020?


```python
pd.read_sql("""
SELECT 
    p.plan_name, 
    COUNT(*) AS count
FROM foodie_fi.subscriptions s
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE YEAR(s.start_date) = 2020 AND p.plan_name = "pro annual"
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>plan_name</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>pro annual</td>
      <td>195</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
In 2020, there were 195 Foodie-Fi customers who upgraded to `annual plan`.

___
#### 9.How many days on average does it take for a customer to an `annual plan` from the day they join Foodie-Fi?


```python
pd.read_sql("""
WITH customers_annualplan_cte AS
(
    -- retrieve customer IDs of those subscribed to the "pro annual" plan
    
    SELECT s.customer_id
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name = 'pro annual' 
)

 -- retrieve start dates for 'trial' and 'pro annual' for each customer 

SELECT 
    cte.customer_id, 
    p.plan_name, 
    s.start_date AS join_date, 
    LEAD(s.start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS updated_date
FROM customers_annualplan_cte cte
JOIN foodie_fi.subscriptions s ON cte.customer_id = s.customer_id
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE p.plan_name IN ('trial','pro annual') 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_name</th>
      <th>join_date</th>
      <th>updated_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>trial</td>
      <td>2020-09-20</td>
      <td>2020-09-27</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>pro annual</td>
      <td>2020-09-27</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9</td>
      <td>trial</td>
      <td>2020-12-07</td>
      <td>2020-12-14</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9</td>
      <td>pro annual</td>
      <td>2020-12-14</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>16</td>
      <td>trial</td>
      <td>2020-05-31</td>
      <td>2020-10-21</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>511</th>
      <td>976</td>
      <td>pro annual</td>
      <td>2021-02-13</td>
      <td>None</td>
    </tr>
    <tr>
      <th>512</th>
      <td>978</td>
      <td>trial</td>
      <td>2020-08-27</td>
      <td>2020-11-03</td>
    </tr>
    <tr>
      <th>513</th>
      <td>978</td>
      <td>pro annual</td>
      <td>2020-11-03</td>
      <td>None</td>
    </tr>
    <tr>
      <th>514</th>
      <td>989</td>
      <td>trial</td>
      <td>2020-09-03</td>
      <td>2021-01-10</td>
    </tr>
    <tr>
      <th>515</th>
      <td>989</td>
      <td>pro annual</td>
      <td>2021-01-10</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>516 rows × 4 columns</p>
</div>




```python
pd.read_sql("""
WITH customers_annualplan_cte AS
(
    -- retrieve customer IDs of those subscribed to the "pro annual" plan
    
    SELECT s.customer_id
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name = 'pro annual' 
),
subscription_dates_cte AS
(
    -- retrieve start dates for 'trial' and 'pro annual' for each customer 
    
    SELECT 
        cte.customer_id, 
        p.plan_name, 
        s.start_date AS join_date, 
        LEAD(s.start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS updated_date
    FROM customers_annualplan_cte cte
    JOIN foodie_fi.subscriptions s ON cte.customer_id = s.customer_id
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name IN ('trial','pro annual') 
)
SELECT 
    ROUND(AVG(TIMESTAMPDIFF(DAY, join_date, updated_date)), 1) AS avg_days
FROM subscription_dates_cte
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>104.6</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
The average number of days that it took for Foodie-Fi customers to upgrade to an `annual plan` is 104.5 days.

___
#### 10.Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)


```python
# Drop the Temporary Table (if it exists)
mycursor.execute("DROP TEMPORARY TABLE IF EXISTS NbDays_to_ProAnnual;")

# Write the query
query10 = """
WITH customers_annualplan_cte AS
(
    -- retrieve customer IDs of those subscribed to the "pro annual" plan
    
    SELECT s.customer_id
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name = 'pro annual' 
),

subscription_dates_cte AS
(
    -- retrieve start dates for 'trial' and 'pro annual' for each customer 
    
    SELECT 
        cte.customer_id, 
        s.start_date AS join_date, 
        LEAD(s.start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS updated_date,
        TIMESTAMPDIFF(DAY, s.start_date, LEAD(s.start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date)) AS num_days
    FROM customers_annualplan_cte cte
    JOIN foodie_fi.subscriptions s ON cte.customer_id = s.customer_id
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE p.plan_name IN ('trial','pro annual') 
)
SELECT *
FROM subscription_dates_cte;
"""

# Create and Save Temporary Table
mycursor.execute("CREATE TEMPORARY TABLE NbDays_to_ProAnnual AS " + query10.replace(';', ''))
conn.commit()

# Display the table
pd.read_sql(query10, conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>join_date</th>
      <th>updated_date</th>
      <th>num_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>2020-09-20</td>
      <td>2020-09-27</td>
      <td>7.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2020-09-27</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9</td>
      <td>2020-12-07</td>
      <td>2020-12-14</td>
      <td>7.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9</td>
      <td>2020-12-14</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>16</td>
      <td>2020-05-31</td>
      <td>2020-10-21</td>
      <td>143.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>511</th>
      <td>976</td>
      <td>2021-02-13</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>512</th>
      <td>978</td>
      <td>2020-08-27</td>
      <td>2020-11-03</td>
      <td>68.0</td>
    </tr>
    <tr>
      <th>513</th>
      <td>978</td>
      <td>2020-11-03</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>514</th>
      <td>989</td>
      <td>2020-09-03</td>
      <td>2021-01-10</td>
      <td>129.0</td>
    </tr>
    <tr>
      <th>515</th>
      <td>989</td>
      <td>2021-01-10</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>516 rows × 4 columns</p>
</div>



**Result**


```python
query10 = """
WITH periods_cte AS 
(
    -- create a column for the 30 day periods 
    SELECT 
        customer_id, 
        num_days,
        CONCAT(
            CAST(FLOOR((num_days - 1)/30) * 30 + 1 AS CHAR), '-',
            CAST(FLOOR((num_days - 1)/30 + 1) * 30 AS CHAR), ' days'
        ) AS periods
    FROM NbDays_to_ProAnnual
    HAVING num_days IS NOT NULL
)
SELECT 
    periods, 
    COUNT(customer_id) AS nb_customers, 
    ROUND(AVG(num_days), 2)AS avg_days
FROM periods_cte
GROUP BY periods
ORDER BY CAST(SUBSTRING_INDEX(periods, '-', 1) AS UNSIGNED);
"""

df10 = pd.read_sql(query10, conn)
df10
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>periods</th>
      <th>nb_customers</th>
      <th>avg_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1-30 days</td>
      <td>49</td>
      <td>9.96</td>
    </tr>
    <tr>
      <th>1</th>
      <td>31-60 days</td>
      <td>24</td>
      <td>42.33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>61-90 days</td>
      <td>34</td>
      <td>71.44</td>
    </tr>
    <tr>
      <th>3</th>
      <td>91-120 days</td>
      <td>35</td>
      <td>100.69</td>
    </tr>
    <tr>
      <th>4</th>
      <td>121-150 days</td>
      <td>42</td>
      <td>133.36</td>
    </tr>
    <tr>
      <th>5</th>
      <td>151-180 days</td>
      <td>36</td>
      <td>162.06</td>
    </tr>
    <tr>
      <th>6</th>
      <td>181-210 days</td>
      <td>26</td>
      <td>190.73</td>
    </tr>
    <tr>
      <th>7</th>
      <td>211-240 days</td>
      <td>4</td>
      <td>224.25</td>
    </tr>
    <tr>
      <th>8</th>
      <td>241-270 days</td>
      <td>5</td>
      <td>257.20</td>
    </tr>
    <tr>
      <th>9</th>
      <td>271-300 days</td>
      <td>1</td>
      <td>285.00</td>
    </tr>
    <tr>
      <th>10</th>
      <td>301-330 days</td>
      <td>1</td>
      <td>327.00</td>
    </tr>
    <tr>
      <th>11</th>
      <td>331-360 days</td>
      <td>1</td>
      <td>346.00</td>
    </tr>
  </tbody>
</table>
</div>




```python
fig, ax = plt.subplots(figsize = (6,3))
ax.barh(df10.periods,df10['nb_customers'], color = 'lightgreen')
ax.set(title = 'Average Number of Days Taken for Customers to Upgrade to an Annual Plan', 
       xlabel = 'Count', 
       ylabel = 'Time Periods');
```


    
![png](images/question10.png)
    



```python
mycursor.execute("DROP TEMPORARY TABLE IF EXISTS NbDays_to_ProAnnual;")
conn.commit()
```

___
#### 11.How many customers downgraded from a `pro monthly` to a `basic monthly` plan in 2020?


```python
pd.read_sql("""
WITH cte AS (
    SELECT 
        s.customer_id, 
        s.start_date, 
        p.plan_name, 
        LEAD(p.plan_name) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS updated_plan
    FROM foodie_fi.subscriptions s
    JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
    WHERE YEAR(s.start_date) = 2020
)
SELECT COUNT(*) AS downgraded_count
FROM cte
WHERE plan_name = 'pro monthly' AND updated_plan = 'basic monthly';
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>downgraded_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



**Result**\
There is no customer that downgraded from a `pro monthly` to a `basic monthly` plan in 2020 at Foodie-Fi.

___
<a id="C"></a>
## C. Challenge Payment Question 

The Foodie-Fi team wants you to create a new payments table for the **year 2020** that includes amounts paid by each customer in the subscriptions table with the following requirements:
- monthly payments always occur on the **same day of month as the original `start_date`** of any monthly paid plan
- upgrades from `basic to monthly` or `pro plans` are reduced by the current paid amount in that month and start immediately
- upgrades from `pro monthly` to `pro annual` are paid at the end of the current billing period and also starts at the end of the month period
- once a customer churns they will no longer make payments

### Strategies:
- In **PostgreSQL**, the `generate_series` function can be utilized to create a series of numbers or dates within a specified range.
- Therefore, in order to achieve this result with **MySQL**, we will create subqueries with `CROSS JOIN` and `UNION`.
1. Generate a series of monthly payment dates for each plan and customer.
2. Retrieve the payments until the end of year 2020 OR until the start date of the `pro annual` plan.
3. Sort the payments by customer IDs in ascending order.
4. Adjust the payment amounts for customers who have upgraded their plans from basic to pro.
5. Create a table for the new payment system.

### STEP 1: Generate a series of monthly payment dates for each customer.
As we observe in the initial datasets (see query below), the `start_date` of each plan is displayed *only once* for each respective month. However, our desired outcome is to generate a sequence of monthly payment dates for each plan and customer until the end of 2020.<br>
For instance, if **customer 1** subscribed to the `basic monthly` plan on 2020-08-08, we should display the same plan name for each month until 2020-12-08 (from august until december).


```python
pd.read_sql("""
SELECT 
    s.customer_id, 
    s.plan_id, 
    p.plan_name, 
    s.start_date, p.price
FROM foodie_fi.subscriptions s
JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
WHERE p.plan_name != 'trial' 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_id</th>
      <th>plan_name</th>
      <th>start_date</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-08</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-09-27</td>
      <td>199.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-01-20</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-01-24</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>4</td>
      <td>churn</td>
      <td>2020-04-21</td>
      <td>NaN</td>
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
      <th>1645</th>
      <td>998</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-19</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>1646</th>
      <td>999</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-30</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>1647</th>
      <td>999</td>
      <td>4</td>
      <td>churn</td>
      <td>2020-12-01</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1648</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-03-26</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>1649</th>
      <td>1000</td>
      <td>4</td>
      <td>churn</td>
      <td>2020-06-04</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>1650 rows × 5 columns</p>
</div>



Now, in the following query, let's generate a **serie of monthly dates** for each `customer_id` starting from their respective `start_date` and continuing until the end of the year 2020.


```python
query_generate_dates = """
SELECT 
    customer_id, 
    plan_id, 
    plan_name, 
    DATE_ADD(start_date, INTERVAL t.n MONTH) AS start_date,
    price
FROM 
    (
        -- Join Initial Datasets
        SELECT s.customer_id, s.plan_id, p.plan_name, s.start_date, p.price
        FROM foodie_fi.subscriptions s
        JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
        WHERE p.plan_name != 'trial' 
    ) i
CROSS JOIN (
        SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
        SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL
        SELECT 10 UNION ALL SELECT 11
    ) t
WHERE (plan_name != 'churn' AND YEAR(DATE_ADD(start_date, INTERVAL t.n MONTH)) < 2021) 
ORDER BY customer_id, start_date

"""
pd.read_sql(query_generate_dates, conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_id</th>
      <th>plan_name</th>
      <th>start_date</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-08</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-09-08</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-10-08</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-11-08</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-12-08</td>
      <td>9.9</td>
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
      <th>7239</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-08-26</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>7240</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-09-26</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>7241</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-26</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>7242</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-11-26</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>7243</th>
      <td>1000</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-12-26</td>
      <td>19.9</td>
    </tr>
  </tbody>
</table>
<p>7244 rows × 5 columns</p>
</div>



### STEP 2: Retrieve the payments until the end of year 2020 OR until the start date of the pro annual plan.
Please note that in the previous step, the series of monthly payment dates was automatically generated for each subscribed plan and customer until the end of the year 2020. However, it is important to understand that once a customer upgrades to the `pro annual` plan, only one fee will be charged for the remainder of the year, starting from the respective start date of the changed plan. In other words, in the final table, there should not be any instances of multiple monthly payments beyond the start date of the pro annual. The table presented below is a potential output from the previous step.

**Table 1**: Query Result Obtained in Step 1 with Pro Annual Upgrade

customer_id | plan_id | plan_name | start_date | price
-- | -- | -- | -- | -- |
A | 1 | basic monthly | 2020-01-03 | 9.90
A | 1 | basic monthly | 2020-02-03 | 9.90
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**
A | 1 | basic monthly | 2020-04-03 | 9.90
A | 1 | basic monthly | 2020-05-03 | 9.90
A | 1 | basic monthly | 2020-06-03 | 9.90

OR 


customer_id | plan_id | plan_name | start_date | price
-- | -- | -- | -- | -- |
A | 1 | basic monthly | 2020-01-03 | 9.90
A | 1 | basic monthly | 2020-02-03 | 9.90
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**


**Table 2**: Desired Query Result with Pro Annual Upgrade to achieve in STEP 2

customer_id | plan_id | plan_name | start_date | price
-- | -- | -- | -- | -- |
A | 1 | basic monthly | 2020-01-03 | 9.90
A | 1 | basic monthly | 2020-02-03 | 9.90
**A** | **3** | **pro annual** | **2020-03-03** | **199.00**


**Query explanation for STEP 2**

1. The **`addHelperVariables_cte`** has been introduced with 3 additional columns using **Window Functions** (LEAD, LAG, and DENSE_RANK) to facilitate the analysis:

    - `prev_price` column retrieves the plan price of the previous month from the previous row using the `LAG` function. This column allows for later adjustment of the amount for customers who have upgraded their plans from basic to pro.

    - `next_payment_date` column retrieves the next payment date from the row below using the `LEAD` function. This column allows us to remove multiple monthly payments beyond the start date of the pro annual plan.

    - `pro_annual_rk` column ranks the occurrence of the pro annual plan using the `DENSE_RANK` function. This column allows us to select the initial start date of the upgrade plan to an annual plan.


2. The **`joinAllPlans_cte`** is used to remove all instances of multiple monthly payments beyond the start date of the pro annual plan using `UNION ALL`.


```python
query_ranking = """
WITH addHelperVariables_cte AS
(
    -- This CTE introduces additional columns to facilitate the analysis
    SELECT 
        customer_id, plan_id, plan_name, start_date, price,
        LAG(price) OVER (PARTITION BY customer_id ORDER BY start_date) AS prev_price,
        LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS next_payment_date,
        CASE WHEN plan_name = 'pro annual'
            THEN DENSE_RANK() OVER (PARTITION BY customer_id, plan_name ORDER BY start_date)
        END AS pro_annual_rk
    FROM ({}) AS query_generate_dates
),
joinAllPlans_cte AS
(
    -- This CTE combines all plans until the end of year 2020 or until the start date of the "pro annual" plan.
    
    
    -- This first SELECT query retrives all plans except "pro annual" plan
    -- until the end of year 2020 or until the start date of the "pro annual" plan.
    SELECT customer_id, plan_id, plan_name, start_date, price, prev_price
    FROM 
    (
        SELECT customer_id, plan_id, plan_name, start_date, price, prev_price, next_payment_date,
        TIMESTAMPDIFF(MONTH, start_date, next_payment_date) AS diff_date
        FROM addHelperVariables_cte
        WHERE plan_name != 'pro annual'
    ) t
    WHERE diff_date = 1 OR diff_date IS NULL 


    UNION ALL


    -- This second query retrieves the initial occurence of "pro annual" plan for each customer
    SELECT customer_id, plan_id, plan_name, start_date, price, prev_price
    FROM addHelperVariables_cte
    WHERE pro_annual_rk = 1
)
SELECT * FROM joinAllPlans_cte """.format(query_generate_dates)

pd.read_sql(query_ranking + ' WHERE customer_id IN (1, 2, 13, 15, 16, 18, 19)' , conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_id</th>
      <th>plan_name</th>
      <th>start_date</th>
      <th>price</th>
      <th>prev_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-08</td>
      <td>9.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-09-08</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-10-08</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-11-08</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-12-08</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>13</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-12-22</td>
      <td>9.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-03-24</td>
      <td>19.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>7</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-04-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>8</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-05-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>9</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-06-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>10</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>11</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-08-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>12</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-09-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>13</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>14</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-11-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>15</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-12-24</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>16</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-06-07</td>
      <td>9.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>17</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-07-07</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>18</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-07</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>19</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-09-07</td>
      <td>9.9</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>20</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-13</td>
      <td>19.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>21</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-08-13</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>22</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-09-13</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>23</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-13</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>24</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-11-13</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>25</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-12-13</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>26</th>
      <td>19</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-06-29</td>
      <td>19.9</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>27</th>
      <td>19</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-29</td>
      <td>19.9</td>
      <td>19.9</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-09-27</td>
      <td>199.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>29</th>
      <td>16</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-10-21</td>
      <td>199.0</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>30</th>
      <td>19</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-08-29</td>
      <td>199.0</td>
      <td>19.9</td>
    </tr>
  </tbody>
</table>
</div>



### Result

Now, let's combine all the above steps into the following final lengthy query, utilizing a total of 4 CTEs:

- **`generateMonthlyPaymentDates_cte`**: generates a series of monthly payment dates for each customer (created in STEP 1).
- **`addHelperVariables_cte`**: introduces additional columns to facilitate the analysis (created in STEP 2).
- **`joinAllPlans_cte`**: combines all plans until the end of 2020 or until the start date of the "pro annual" plan (created in STEP 2).
- **`sortPaymentsByCustomerID_cte`**: sorts all the monthly plan payments by customer IDs and payment dates in ascending order. Once the data is sorted, I will introduce another column called `prev_plan` in the same CTE using the `LAG` function for later adjustment of the payment amount.
- Finally, the **CTE-based query** renames the column names, adjusts the payment amount, and adds the payment order.


```python
query_payments = """
WITH generateMonthlyPaymentDates_cte AS
(
    -- This CTE generates a serie of monthly payment dates for each customer
    SELECT 
        customer_id, 
        plan_id, 
        plan_name, 
        DATE_ADD(start_date, INTERVAL t.n MONTH) AS start_date,
        price

    FROM 
    (
        -- Join Initial Datasets 
        SELECT s.customer_id, s.plan_id, p.plan_name, s.start_date, p.price
        FROM foodie_fi.subscriptions s
        JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
        WHERE p.plan_name != 'trial'
    ) s
    CROSS JOIN (
        SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
        SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL
        SELECT 10 UNION ALL SELECT 11
    ) t
    WHERE (plan_name != 'churn' AND YEAR(DATE_ADD(start_date, INTERVAL t.n MONTH)) < 2021) 
    ORDER BY customer_id, start_date
),


addHelperVariables_cte AS
(
    -- This CTE introduces additional columns to facilitate the analysis
    SELECT 
        customer_id, plan_id, plan_name, start_date, price,
        LAG(price) OVER (PARTITION BY customer_id ORDER BY start_date) AS prev_price,
        LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS next_payment_date,
        CASE WHEN plan_name = 'pro annual'
            THEN DENSE_RANK() OVER (PARTITION BY customer_id, plan_name ORDER BY start_date)
        END AS pro_annual_rk
    FROM generateMonthlyPaymentDates_cte 
), 


joinAllPlans_cte AS
(
    -- This CTE combines all plans until the end of year 2020 or until the start date of the "pro annual" plan.
    
    
    -- This first SELECT query retrives all plans except "pro annual" plan
    -- until the end of year 2020 or until the start date of the "pro annual" plan.
    SELECT customer_id, plan_id, plan_name, start_date, price, prev_price
    FROM 
    (
        SELECT 
            customer_id, plan_id, plan_name, start_date, price, prev_price, next_payment_date,
            TIMESTAMPDIFF(MONTH, start_date, next_payment_date) AS diff_date
        FROM addHelperVariables_cte
        WHERE plan_name != 'pro annual'
    ) t
    WHERE diff_date = 1 OR diff_date IS NULL 


    UNION ALL


    -- This second query retrieves the initial occurence of "pro annual" plan for each customer
    SELECT customer_id, plan_id, plan_name, start_date, price, prev_price
    FROM addHelperVariables_cte
    WHERE pro_annual_rk = 1
),


sortPaymentsByCustomerID_cte AS
(
    SELECT *, LAG(plan_name) OVER (PARTITION BY customer_id ORDER BY start_date) AS prev_plan
    FROM joinAllPlans_cte
    ORDER BY customer_id, start_date
)

SELECT 
    customer_id, 
    plan_id, plan_name, 
    start_date AS payment_date,
    IF(plan_name LIKE '%pro%' AND prev_plan NOT LIKE '%pro%', FORMAT(price - prev_price, 2), FORMAT(price, 2)) AS amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date) AS payment_order
FROM sortPaymentsByCustomerID_cte 
"""


# Create table
mycursor.execute('DROP TABLE IF EXISTS monthly_payments;')
mycursor.execute('CREATE TABLE monthly_payments AS ' + query_payments)
conn.commit()
```

Now, let's display the table with the 7 sample customers provided in the example (customers 1, 2, 13, 15, 16, 18 and 19).


```python
pd.read_sql("""
SELECT *
FROM foodie_fi.monthly_payments
WHERE customer_id IN (1, 2, 13, 15, 16, 18, 19);
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>plan_id</th>
      <th>plan_name</th>
      <th>payment_date</th>
      <th>amount</th>
      <th>payment_order</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-08</td>
      <td>9.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-09-08</td>
      <td>9.90</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-10-08</td>
      <td>9.90</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-11-08</td>
      <td>9.90</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-12-08</td>
      <td>9.90</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-09-27</td>
      <td>199.00</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-12-22</td>
      <td>9.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-03-24</td>
      <td>19.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-04-24</td>
      <td>19.90</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-05-24</td>
      <td>19.90</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-06-24</td>
      <td>19.90</td>
      <td>4</td>
    </tr>
    <tr>
      <th>11</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-24</td>
      <td>19.90</td>
      <td>5</td>
    </tr>
    <tr>
      <th>12</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-08-24</td>
      <td>19.90</td>
      <td>6</td>
    </tr>
    <tr>
      <th>13</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-09-24</td>
      <td>19.90</td>
      <td>7</td>
    </tr>
    <tr>
      <th>14</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-24</td>
      <td>19.90</td>
      <td>8</td>
    </tr>
    <tr>
      <th>15</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-11-24</td>
      <td>19.90</td>
      <td>9</td>
    </tr>
    <tr>
      <th>16</th>
      <td>15</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-12-24</td>
      <td>19.90</td>
      <td>10</td>
    </tr>
    <tr>
      <th>17</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-06-07</td>
      <td>9.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>18</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-07-07</td>
      <td>9.90</td>
      <td>2</td>
    </tr>
    <tr>
      <th>19</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-08-07</td>
      <td>9.90</td>
      <td>3</td>
    </tr>
    <tr>
      <th>20</th>
      <td>16</td>
      <td>1</td>
      <td>basic monthly</td>
      <td>2020-09-07</td>
      <td>9.90</td>
      <td>4</td>
    </tr>
    <tr>
      <th>21</th>
      <td>16</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-10-21</td>
      <td>189.10</td>
      <td>5</td>
    </tr>
    <tr>
      <th>22</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-13</td>
      <td>19.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>23</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-08-13</td>
      <td>19.90</td>
      <td>2</td>
    </tr>
    <tr>
      <th>24</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-09-13</td>
      <td>19.90</td>
      <td>3</td>
    </tr>
    <tr>
      <th>25</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-10-13</td>
      <td>19.90</td>
      <td>4</td>
    </tr>
    <tr>
      <th>26</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-11-13</td>
      <td>19.90</td>
      <td>5</td>
    </tr>
    <tr>
      <th>27</th>
      <td>18</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-12-13</td>
      <td>19.90</td>
      <td>6</td>
    </tr>
    <tr>
      <th>28</th>
      <td>19</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-06-29</td>
      <td>19.90</td>
      <td>1</td>
    </tr>
    <tr>
      <th>29</th>
      <td>19</td>
      <td>2</td>
      <td>pro monthly</td>
      <td>2020-07-29</td>
      <td>19.90</td>
      <td>2</td>
    </tr>
    <tr>
      <th>30</th>
      <td>19</td>
      <td>3</td>
      <td>pro annual</td>
      <td>2020-08-29</td>
      <td>199.00</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.close()
```
