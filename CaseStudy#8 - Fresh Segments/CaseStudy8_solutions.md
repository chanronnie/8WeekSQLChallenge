# Case Study #8: Fresh Segments
The case study questions presented here are created by [**Data With Danny**](https://linktr.ee/datawithdanny). They are part of the [**8 Week SQL Challenge**](https://8weeksqlchallenge.com/).

My SQL queries are written in the `PostgreSQL 15` dialect, integrated into `Jupyter Notebook`, which allows us to instantly view the query results and document the queries.

For more details about the **Case Study #8**, click [**here**](https://8weeksqlchallenge.com/case-study-8/).

## Table of Contents
### [1. Importing Libraries](#Import)
### [2. Tables of the Database](#Tables)
### [3. Case Study Questions](#CaseStudyQuestions)
- [A. Data Exploration and Cleansing](#A)
- [B. Interest Analysis](#B)
- [C. Segment Analysis](#C)
- [D. Index Analysis](#D)


<a id = 'Import'></a>
## 1. Import Libraries


```python
import os
import pandas as pd
import psycopg2 as pg2
import warnings

warnings.filterwarnings("ignore")
```

### Connecting the database from Jupyter Notebook


```python
# Get PostgreSQL password
mypassword = os.getenv("POSTGRESQL_PASSWORD")

# Connect the database
try:
    conn = pg2.connect(user = 'postgres', password = mypassword, database = 'fresh_segments')
    cursor = conn.cursor()
    print("Database connection successful")
except mysql.connector.Error as err:
   print(f"Error: '{err}'") 
```

    Database connection successful
    


<a id = 'Tables'></a>
## 2. Tables of the database

First, let's verify if the connected database contains the correct dataset names.

```python
cursor.execute("""
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'fresh_segments'
""")

table_names = []
print('--- Tables within "fresh_segments" database --- ')
for table in cursor:
    print(table[1])
    table_names.append(table[1])
```

    --- Tables within "fresh_segments" database --- 
    json_data
    interest_map
    interest_metrics
    


```python
for table in table_names:
    print('Table: ', table)
    display(pd.read_sql("SELECT * FROM fresh_segments." + table, conn))
```

    Table:  json_data
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>raw_data</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{'month': 7, 'year': 2018, 'month_year': '07-2...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>{'month': 7, 'year': 2018, 'month_year': '07-2...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{'month': 7, 'year': 2018, 'month_year': '07-2...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>{'month': 7, 'year': 2018, 'month_year': '07-2...</td>
    </tr>
  </tbody>
</table>
</div>


    Table:  interest_map

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>interest_name</th>
      <th>interest_summary</th>
      <th>created_at</th>
      <th>last_modified</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Gamers</td>
      <td>Consumers researching game reviews and cheat c...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Car Enthusiasts</td>
      <td>Readers of automotive news and car reviews.</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Luxury Retail Researchers</td>
      <td>Consumers researching luxury product reviews a...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Brides &amp; Wedding Planners</td>
      <td>People researching wedding ideas and vendors.</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
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
      <th>1204</th>
      <td>6391</td>
      <td>HVAC Service Researchers</td>
      <td>None</td>
      <td>2017-06-08 12:21:04</td>
      <td>2017-06-08 12:21:04</td>
    </tr>
    <tr>
      <th>1205</th>
      <td>7483</td>
      <td>Economy Grocery Shoppers</td>
      <td>None</td>
      <td>2017-07-06 15:45:18</td>
      <td>2017-07-06 15:45:18</td>
    </tr>
    <tr>
      <th>1206</th>
      <td>7527</td>
      <td>Democratic Donors</td>
      <td>None</td>
      <td>2017-07-17 17:24:48</td>
      <td>2017-07-17 17:24:48</td>
    </tr>
    <tr>
      <th>1207</th>
      <td>7557</td>
      <td>Tailgaters</td>
      <td>None</td>
      <td>2017-07-20 17:31:31</td>
      <td>2019-01-16 09:11:30</td>
    </tr>
    <tr>
      <th>1208</th>
      <td>7605</td>
      <td>Organic Food Buyers</td>
      <td>None</td>
      <td>2017-08-07 10:00:03</td>
      <td>2017-08-07 10:00:03</td>
    </tr>
  </tbody>
</table>
<p>1209 rows × 5 columns</p>
</div>


    Table:  interest_metrics
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>_month</th>
      <th>_year</th>
      <th>month_year</th>
      <th>interest_id</th>
      <th>composition</th>
      <th>index_value</th>
      <th>ranking</th>
      <th>percentile_ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>2018</td>
      <td>07-2018</td>
      <td>32486</td>
      <td>11.89</td>
      <td>6.19</td>
      <td>1</td>
      <td>99.86</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7</td>
      <td>2018</td>
      <td>07-2018</td>
      <td>6106</td>
      <td>9.93</td>
      <td>5.31</td>
      <td>2</td>
      <td>99.73</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7</td>
      <td>2018</td>
      <td>07-2018</td>
      <td>18923</td>
      <td>10.85</td>
      <td>5.29</td>
      <td>3</td>
      <td>99.59</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7</td>
      <td>2018</td>
      <td>07-2018</td>
      <td>6344</td>
      <td>10.32</td>
      <td>5.10</td>
      <td>4</td>
      <td>99.45</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>2018</td>
      <td>07-2018</td>
      <td>100</td>
      <td>10.77</td>
      <td>5.04</td>
      <td>5</td>
      <td>99.31</td>
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
      <td>...</td>
    </tr>
    <tr>
      <th>14268</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1.60</td>
      <td>0.72</td>
      <td>1189</td>
      <td>0.42</td>
    </tr>
    <tr>
      <th>14269</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1.62</td>
      <td>0.71</td>
      <td>1190</td>
      <td>0.34</td>
    </tr>
    <tr>
      <th>14270</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1.62</td>
      <td>0.68</td>
      <td>1191</td>
      <td>0.25</td>
    </tr>
    <tr>
      <th>14271</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1.51</td>
      <td>0.63</td>
      <td>1193</td>
      <td>0.08</td>
    </tr>
    <tr>
      <th>14272</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1.64</td>
      <td>0.62</td>
      <td>1194</td>
      <td>0.00</td>
    </tr>
  </tbody>
</table>
<p>14273 rows × 8 columns</p>
</div>


<a id = 'CaseStudyQuestions'></a>
## 3. Case Study Questions

<a id = 'A'></a>
## A. Data Exploration and Cleansing

#### 1. Update the `fresh_segments.interest_metrics` table by modifying the `month_year` column to be a date data type with the start of the month

### Checking the datatypes

The query reveals that the data type of `month_year` is incorrect, as confirmed by using the function `pg_typeof()`.


```python
pd.read_sql("""
SELECT
    pg_typeof(month_year) AS month_year
FROM fresh_segments.interest_metrics
LIMIT 1
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>character varying</td>
    </tr>
  </tbody>
</table>
</div>



Alternatively, by checking the datatype of each column of the dataset, we can observe that `_month`, `_year`, `month_year` and `interest_id` have incorrect datatypes.


```python
pd.read_sql("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fresh_segments' AND table_name = 'interest_metrics' 
AND column_name IN ('_month', '_year', 'month_year', 'interest_id', 'composition', 'index_value', 'ranking', 'percentile_ranking');
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>column_name</th>
      <th>data_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>percentile_ranking</td>
      <td>double precision</td>
    </tr>
    <tr>
      <th>1</th>
      <td>composition</td>
      <td>double precision</td>
    </tr>
    <tr>
      <th>2</th>
      <td>index_value</td>
      <td>double precision</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ranking</td>
      <td>integer</td>
    </tr>
    <tr>
      <th>4</th>
      <td>_month</td>
      <td>character varying</td>
    </tr>
    <tr>
      <th>5</th>
      <td>_year</td>
      <td>character varying</td>
    </tr>
    <tr>
      <th>6</th>
      <td>month_year</td>
      <td>character varying</td>
    </tr>
    <tr>
      <th>7</th>
      <td>interest_id</td>
      <td>character varying</td>
    </tr>
  </tbody>
</table>
</div>



When performing data cleaning in SQL, it is essential to adopt certain **best coding practices**. The most recommended practice in this case is to prevent the loss of the original dataset. Regardless of any mistakes made during the process, it is crucial to retain access to the initial dataset. To achieve this, we should create a **backup table** and copy the entire original `interest_metrics` table to this backup table, which we'll name `interest_metrics_backup`. By doing so, we can proceed with the data cleaning directly on `interest_metrics` without any concerns about losing the original data.

### Creating Backup Table


```python
# Create the empty backup table
cursor.execute("DROP TABLE IF EXISTS fresh_segments.interest_metrics_backup;")
cursor.execute("""
CREATE TABLE fresh_segments.interest_metrics_backup
(
  "_month" VARCHAR(4),
  "_year" VARCHAR(4),
  "month_year" VARCHAR(7),
  "interest_id" VARCHAR(5),
  "composition" FLOAT,
  "index_value" FLOAT,
  "ranking" INTEGER,
  "percentile_ranking" FLOAT
);""")


# Copy/Paste the original "interest_metrics" into the "interest_metrics_backup"
cursor.execute("""
INSERT INTO fresh_segments.interest_metrics_backup
SELECT *
FROM fresh_segments.interest_metrics
""")

# Save the updates
conn.commit()
```

### Data Cleaning on `interest_metrics` table


```python
# Correct the datatypes of the columns
cursor.execute("""
ALTER TABLE fresh_segments.interest_metrics
    ALTER COLUMN _month TYPE INTEGER USING _month::INTEGER,
    ALTER COLUMN _year TYPE INTEGER USING _year::INTEGER,
    ALTER COLUMN interest_id TYPE INTEGER USING interest_id::INTEGER,
    ALTER COLUMN month_year TYPE DATE USING TO_DATE(month_year, 'mm-yyyy');
""")


# Rename the columns "_month" and "_year"
cursor.execute("""
ALTER TABLE fresh_segments.interest_metrics
    RENAME COLUMN _month TO month;
""")

cursor.execute("""
ALTER TABLE fresh_segments.interest_metrics
    RENAME COLUMN _year TO year;
""")


# Save the updates
conn.commit()
```

Let's perform another check on the datatypes of the columns once more.


```python
pd.read_sql("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fresh_segments' AND table_name = 'interest_metrics' 
AND column_name IN ('month', 'year', 'month_year', 'interest_id', 'composition', 'index_value', 'ranking', 'percentile_ranking');
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>column_name</th>
      <th>data_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>month</td>
      <td>integer</td>
    </tr>
    <tr>
      <th>1</th>
      <td>year</td>
      <td>integer</td>
    </tr>
    <tr>
      <th>2</th>
      <td>month_year</td>
      <td>date</td>
    </tr>
    <tr>
      <th>3</th>
      <td>interest_id</td>
      <td>integer</td>
    </tr>
    <tr>
      <th>4</th>
      <td>composition</td>
      <td>double precision</td>
    </tr>
    <tr>
      <th>5</th>
      <td>index_value</td>
      <td>double precision</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ranking</td>
      <td>integer</td>
    </tr>
    <tr>
      <th>7</th>
      <td>percentile_ranking</td>
      <td>double precision</td>
    </tr>
  </tbody>
</table>
</div>



Here is the processed table:


```python
pd.read_sql("SELECT * FROM fresh_segments.interest_metrics", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>year</th>
      <th>month_year</th>
      <th>interest_id</th>
      <th>composition</th>
      <th>index_value</th>
      <th>ranking</th>
      <th>percentile_ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>32486.0</td>
      <td>11.89</td>
      <td>6.19</td>
      <td>1</td>
      <td>99.86</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>6106.0</td>
      <td>9.93</td>
      <td>5.31</td>
      <td>2</td>
      <td>99.73</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>18923.0</td>
      <td>10.85</td>
      <td>5.29</td>
      <td>3</td>
      <td>99.59</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>6344.0</td>
      <td>10.32</td>
      <td>5.10</td>
      <td>4</td>
      <td>99.45</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>100.0</td>
      <td>10.77</td>
      <td>5.04</td>
      <td>5</td>
      <td>99.31</td>
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
      <td>...</td>
    </tr>
    <tr>
      <th>14268</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>1.60</td>
      <td>0.72</td>
      <td>1189</td>
      <td>0.42</td>
    </tr>
    <tr>
      <th>14269</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>1.62</td>
      <td>0.71</td>
      <td>1190</td>
      <td>0.34</td>
    </tr>
    <tr>
      <th>14270</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>1.62</td>
      <td>0.68</td>
      <td>1191</td>
      <td>0.25</td>
    </tr>
    <tr>
      <th>14271</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>1.51</td>
      <td>0.63</td>
      <td>1193</td>
      <td>0.08</td>
    </tr>
    <tr>
      <th>14272</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>1.64</td>
      <td>0.62</td>
      <td>1194</td>
      <td>0.00</td>
    </tr>
  </tbody>
</table>
<p>14273 rows × 8 columns</p>
</div>



___
#### 2. What is count of records in the `fresh_segments.interest_metrics` for each `month_year` value sorted in chronological order (earliest to latest) with the null values appearing first?


```python
pd.read_sql("""
SELECT 
    month_year, 
    COUNT(*) AS nb_records
FROM fresh_segments.interest_metrics
GROUP BY month_year
ORDER BY (month_year IS NOT NULL), month_year
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>nb_records</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>1194</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-07-01</td>
      <td>729</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-08-01</td>
      <td>767</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-09-01</td>
      <td>780</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-10-01</td>
      <td>857</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018-11-01</td>
      <td>928</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2018-12-01</td>
      <td>995</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-01-01</td>
      <td>973</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-02-01</td>
      <td>1121</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-03-01</td>
      <td>1136</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2019-04-01</td>
      <td>1099</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2019-05-01</td>
      <td>857</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2019-06-01</td>
      <td>824</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2019-07-01</td>
      <td>864</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2019-08-01</td>
      <td>1149</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 3. What do you think we should do with these null values in the `fresh_segments.interest_metrics`?

**Result**</br>
- Retaining the `NULL` values in the `month`, `year`, and `month_year` columns would result in less accurate analysis since the date of the activity cannot be determined.
- Additionally, keeping the `NULL` values in the `interest_id` column provides no information, as we cannot identify the specific interest it refers to.
- Therefore, for our analysis, we will exclude all rows with `NULL` values by filtering them out instead of deleting them entirely.

___
#### 4. How many `interest_id` values exist in the `fresh_segments.interest_metrics` table but not in the `fresh_segments.interest_map` table? What about the other way around?

There is no `interest_id` that exists in the `interest_metrics` table but not in the `interest_map` table.


```python
pd.read_sql("""
SELECT COUNT(interest_id) AS count_not_in_map
FROM fresh_segments.interest_metrics me
LEFT JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
WHERE interest_id IS NOT NULL AND ma.id IS NULL
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count_not_in_map</th>
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



There are 7 `interest_id` values that exist in the `interest_map` table but not in the `interest_metrics` table.


```python
pd.read_sql("""
SELECT COUNT(ma.id) AS count_not_in_metrics
FROM fresh_segments.interest_metrics me
RIGHT JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
WHERE me.interest_id IS NULL
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count_not_in_metrics</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
    </tr>
  </tbody>
</table>
</div>



The 7 interest_id values are the following:


```python
pd.read_sql("""
SELECT ma.id AS ids_not_in_metrics
FROM fresh_segments.interest_metrics me
RIGHT JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
WHERE me.interest_id IS NULL
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ids_not_in_metrics</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>19598</td>
    </tr>
    <tr>
      <th>1</th>
      <td>35964</td>
    </tr>
    <tr>
      <th>2</th>
      <td>40185</td>
    </tr>
    <tr>
      <th>3</th>
      <td>40186</td>
    </tr>
    <tr>
      <th>4</th>
      <td>42010</td>
    </tr>
    <tr>
      <th>5</th>
      <td>42400</td>
    </tr>
    <tr>
      <th>6</th>
      <td>47789</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. Summarise the id values in the `fresh_segments.interest_map` by its total record count in this table


```python
pd.read_sql("""
SELECT 
    me.interest_id AS id,
    ma.interest_name,
    COUNT(*) AS nb_records
FROM fresh_segments.interest_metrics me
JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
WHERE interest_id IS NOT NULL
GROUP BY me.interest_id, ma.interest_name
ORDER BY me.interest_id 
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>interest_name</th>
      <th>nb_records</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Fitness Enthusiasts</td>
      <td>12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Gamers</td>
      <td>11</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Car Enthusiasts</td>
      <td>10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Luxury Retail Researchers</td>
      <td>14</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Brides &amp; Wedding Planners</td>
      <td>14</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1197</th>
      <td>49979</td>
      <td>Cape Cod News Readers</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1198</th>
      <td>50860</td>
      <td>Food Delivery Service Users</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1199</th>
      <td>51119</td>
      <td>Skin Disorder Researchers</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1200</th>
      <td>51120</td>
      <td>Foot Health Researchers</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1201</th>
      <td>51678</td>
      <td>Plumbers</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>1202 rows × 3 columns</p>
</div>



___
#### 6. What sort of table join should we perform for our analysis and why? Check your logic by checking the rows where interest_id = 21246 in your joined output and include all columns from `fresh_segments.interest_metrics` and all columns from `fresh_segments.interest_map` except from the id column.


```python
pd.read_sql("""
SELECT 
    me.*, 
    ma.interest_name, 
    ma.interest_summary, 
    ma.created_at, 
    ma.last_modified
FROM fresh_segments.interest_metrics me
INNER JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
-- WHERE me.interest_id = 21246
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>year</th>
      <th>month_year</th>
      <th>interest_id</th>
      <th>composition</th>
      <th>index_value</th>
      <th>ranking</th>
      <th>percentile_ranking</th>
      <th>interest_name</th>
      <th>interest_summary</th>
      <th>created_at</th>
      <th>last_modified</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7.0</td>
      <td>2018.0</td>
      <td>2018-07-01</td>
      <td>1</td>
      <td>7.02</td>
      <td>3.44</td>
      <td>46</td>
      <td>93.69</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10.0</td>
      <td>2018.0</td>
      <td>2018-10-01</td>
      <td>1</td>
      <td>3.71</td>
      <td>1.84</td>
      <td>118</td>
      <td>86.23</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3.0</td>
      <td>2019.0</td>
      <td>2019-03-01</td>
      <td>1</td>
      <td>2.76</td>
      <td>1.54</td>
      <td>244</td>
      <td>78.52</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>8.0</td>
      <td>2019.0</td>
      <td>2019-08-01</td>
      <td>1</td>
      <td>2.64</td>
      <td>1.87</td>
      <td>394</td>
      <td>65.71</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12.0</td>
      <td>2018.0</td>
      <td>2018-12-01</td>
      <td>1</td>
      <td>2.94</td>
      <td>1.83</td>
      <td>140</td>
      <td>85.93</td>
      <td>Fitness Enthusiasts</td>
      <td>Consumers using fitness tracking apps and webs...</td>
      <td>2016-05-26 14:57:59</td>
      <td>2018-05-23 11:30:12</td>
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
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>13075</th>
      <td>5.0</td>
      <td>2019.0</td>
      <td>2019-05-01</td>
      <td>51120</td>
      <td>2.03</td>
      <td>1.62</td>
      <td>377</td>
      <td>56.01</td>
      <td>Foot Health Researchers</td>
      <td>People reading news and advice on preventing a...</td>
      <td>2019-04-26 18:00:00</td>
      <td>2019-04-29 14:20:04</td>
    </tr>
    <tr>
      <th>13076</th>
      <td>7.0</td>
      <td>2019.0</td>
      <td>2019-07-01</td>
      <td>51120</td>
      <td>2.20</td>
      <td>1.72</td>
      <td>428</td>
      <td>50.46</td>
      <td>Foot Health Researchers</td>
      <td>People reading news and advice on preventing a...</td>
      <td>2019-04-26 18:00:00</td>
      <td>2019-04-29 14:20:04</td>
    </tr>
    <tr>
      <th>13077</th>
      <td>8.0</td>
      <td>2019.0</td>
      <td>2019-08-01</td>
      <td>51678</td>
      <td>2.26</td>
      <td>1.38</td>
      <td>904</td>
      <td>21.32</td>
      <td>Plumbers</td>
      <td>Professionals reading industry news and resear...</td>
      <td>2019-05-06 22:00:00</td>
      <td>2019-05-07 18:50:04</td>
    </tr>
    <tr>
      <th>13078</th>
      <td>7.0</td>
      <td>2019.0</td>
      <td>2019-07-01</td>
      <td>51678</td>
      <td>1.97</td>
      <td>1.42</td>
      <td>718</td>
      <td>16.90</td>
      <td>Plumbers</td>
      <td>Professionals reading industry news and resear...</td>
      <td>2019-05-06 22:00:00</td>
      <td>2019-05-07 18:50:04</td>
    </tr>
    <tr>
      <th>13079</th>
      <td>5.0</td>
      <td>2019.0</td>
      <td>2019-05-01</td>
      <td>51678</td>
      <td>1.71</td>
      <td>1.53</td>
      <td>475</td>
      <td>44.57</td>
      <td>Plumbers</td>
      <td>Professionals reading industry news and resear...</td>
      <td>2019-05-06 22:00:00</td>
      <td>2019-05-07 18:50:04</td>
    </tr>
  </tbody>
</table>
<p>13080 rows × 12 columns</p>
</div>



**Result**</br>
For our analysis, we are using an `INNER JOIN`. The objective of joining two tables is to link all interest_id values from the `interest_metrics` table to their corresponding interest_name values provided by the `interest_map` table.

___
#### 7. Are there any records in your joined table where the `month_year` value is before the `created_at value` from the `fresh_segments.interest_map` table? Do you think these values are valid and why?


```python
pd.read_sql("""
SELECT 
    me.*, 
    ma.interest_name, 
    ma.interest_summary, 
    ma.created_at, 
    ma.last_modified
FROM fresh_segments.interest_metrics me
INNER JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
WHERE me.month_year < ma.created_at
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>year</th>
      <th>month_year</th>
      <th>interest_id</th>
      <th>composition</th>
      <th>index_value</th>
      <th>ranking</th>
      <th>percentile_ranking</th>
      <th>interest_name</th>
      <th>interest_summary</th>
      <th>created_at</th>
      <th>last_modified</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>2018</td>
      <td>2018-07-01</td>
      <td>32701</td>
      <td>4.23</td>
      <td>1.41</td>
      <td>483</td>
      <td>33.74</td>
      <td>Womens Equality Advocates</td>
      <td>People visiting sites advocating for womens eq...</td>
      <td>2018-07-06 14:35:03</td>
      <td>2018-07-06 14:35:03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7</td>
      <td>2018</td>
      <td>2018-07-01</td>
      <td>32702</td>
      <td>3.56</td>
      <td>1.18</td>
      <td>580</td>
      <td>20.44</td>
      <td>Romantics</td>
      <td>People reading about romance and researching i...</td>
      <td>2018-07-06 14:35:04</td>
      <td>2018-07-06 14:35:04</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7</td>
      <td>2018</td>
      <td>2018-07-01</td>
      <td>32703</td>
      <td>5.53</td>
      <td>1.80</td>
      <td>375</td>
      <td>48.56</td>
      <td>School Supply Shoppers</td>
      <td>Consumers shopping for classroom supplies for ...</td>
      <td>2018-07-06 14:35:04</td>
      <td>2018-07-06 14:35:04</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7</td>
      <td>2018</td>
      <td>2018-07-01</td>
      <td>32704</td>
      <td>8.04</td>
      <td>2.27</td>
      <td>225</td>
      <td>69.14</td>
      <td>Major Airline Customers</td>
      <td>People visiting sites for major airline brands...</td>
      <td>2018-07-06 14:35:04</td>
      <td>2018-07-06 14:35:04</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>2018</td>
      <td>2018-07-01</td>
      <td>32705</td>
      <td>4.38</td>
      <td>1.34</td>
      <td>505</td>
      <td>30.73</td>
      <td>Certified Events Professionals</td>
      <td>Professionals reading industry news and resear...</td>
      <td>2018-07-06 14:35:04</td>
      <td>2018-07-06 14:35:04</td>
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
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>183</th>
      <td>4</td>
      <td>2019</td>
      <td>2019-04-01</td>
      <td>49976</td>
      <td>2.35</td>
      <td>1.27</td>
      <td>530</td>
      <td>51.77</td>
      <td>Agriculture and Climate Advocates</td>
      <td>People supporting organizations for agricultur...</td>
      <td>2019-04-15 18:00:00</td>
      <td>2019-04-24 17:40:04</td>
    </tr>
    <tr>
      <th>184</th>
      <td>4</td>
      <td>2019</td>
      <td>2019-04-01</td>
      <td>49977</td>
      <td>2.17</td>
      <td>1.15</td>
      <td>722</td>
      <td>34.30</td>
      <td>DIY Upcycle Home Project Planners</td>
      <td>People researching and planning home DIY and u...</td>
      <td>2019-04-15 18:00:00</td>
      <td>2019-04-24 17:40:04</td>
    </tr>
    <tr>
      <th>185</th>
      <td>4</td>
      <td>2019</td>
      <td>2019-04-01</td>
      <td>49978</td>
      <td>2.19</td>
      <td>1.17</td>
      <td>695</td>
      <td>36.76</td>
      <td>Homeschooling Parents</td>
      <td>People researching academic projects and progr...</td>
      <td>2019-04-15 18:00:00</td>
      <td>2019-04-24 17:40:04</td>
    </tr>
    <tr>
      <th>186</th>
      <td>4</td>
      <td>2019</td>
      <td>2019-04-01</td>
      <td>49979</td>
      <td>2.56</td>
      <td>1.70</td>
      <td>145</td>
      <td>86.81</td>
      <td>Cape Cod News Readers</td>
      <td>People interested in reading about local news ...</td>
      <td>2019-04-15 18:00:00</td>
      <td>2019-04-18 09:00:05</td>
    </tr>
    <tr>
      <th>187</th>
      <td>5</td>
      <td>2019</td>
      <td>2019-05-01</td>
      <td>51678</td>
      <td>1.71</td>
      <td>1.53</td>
      <td>475</td>
      <td>44.57</td>
      <td>Plumbers</td>
      <td>Professionals reading industry news and resear...</td>
      <td>2019-05-06 22:00:00</td>
      <td>2019-05-07 18:50:04</td>
    </tr>
  </tbody>
</table>
<p>188 rows × 12 columns</p>
</div>



**Result**</br>
The records in the joined table where the `month_year` value is before the `created_at` value from the `fresh_segments.interest_map` table are indeed valid. This is because the `created_at` date values fall within the same month and year as the `month_year` values, indicating that the data is consistent and correctly aligned.


<a id = 'B'></a>
## B. Interest Analysis

#### 1. Which interests have been present in all `month_year` dates in our dataset?

In the Fresh Segment data records, data has been recorded for a total of 14 months.


```python
pd.read_sql("""
SELECT COUNT(DISTINCT month_year) AS nb_month_year_values
FROM fresh_segments.interest_metrics
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>nb_month_year_values</th>
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



By combining the above information, we can utilize a subquery in the `WHERE` statement to filter interests that appeared in all 14 months.


```python
pd.read_sql("""
WITH count_month_year_values_cte AS
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY interest_id) AS nb_month_year_values
    FROM fresh_segments.interest_metrics
    WHERE month_year IS NOT NULL
)
SELECT 
    cte.interest_id, 
    ma.interest_name, 
    nb_month_year_values
FROM count_month_year_values_cte cte
JOIN fresh_segments.interest_map ma ON cte.interest_id = ma.id
WHERE nb_month_year_values = (SELECT COUNT(DISTINCT month_year) FROM fresh_segments.interest_metrics)
ORDER BY ma.interest_name
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_id</th>
      <th>interest_name</th>
      <th>nb_month_year_values</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6183</td>
      <td>Accounting &amp; CPA Continuing Education Researchers</td>
      <td>14</td>
    </tr>
    <tr>
      <th>1</th>
      <td>18347</td>
      <td>Affordable Hotel Bookers</td>
      <td>14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>129</td>
      <td>Aftermarket Accessories Shoppers</td>
      <td>14</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7541</td>
      <td>Alabama Trip Planners</td>
      <td>14</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10284</td>
      <td>Alaskan Cruise Planners</td>
      <td>14</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>475</th>
      <td>19250</td>
      <td>World Cup Enthusiasts</td>
      <td>14</td>
    </tr>
    <tr>
      <th>476</th>
      <td>6234</td>
      <td>Yachting Enthusiasts</td>
      <td>14</td>
    </tr>
    <tr>
      <th>477</th>
      <td>22427</td>
      <td>Yale University Fans</td>
      <td>14</td>
    </tr>
    <tr>
      <th>478</th>
      <td>4902</td>
      <td>Yogis</td>
      <td>14</td>
    </tr>
    <tr>
      <th>479</th>
      <td>22091</td>
      <td>Zoo Visitors</td>
      <td>14</td>
    </tr>
  </tbody>
</table>
<p>480 rows × 3 columns</p>
</div>



___
#### 2. Using this same `total_months` measure - calculate the cumulative percentage of all records starting at 14 months - which `total_months` value passes the 90% cumulative percentage value?


```python
pd.read_sql("""
WITH nb_interests_per_month_cte AS
(
    -- Count the number of interests per month
    
    SELECT 
        total_months, 
        COUNT(interest_id) AS nb_interests
    FROM
    (
        -- Count the total number of month_year values for each interest_id
        
        SELECT 
            interest_id, 
            COUNT(DISTINCT month_year) AS total_months
        FROM fresh_segments.interest_metrics
        WHERE interest_id IS NOT NULL
        GROUP BY interest_id
    ) total_months
    GROUP BY total_months

), cumulative_percent_cte AS
(
    -- Compute the cumulative percentage for each month
    
    SELECT 
        total_months, 
        nb_interests, 
        ROUND(SUM(nb_interests) OVER (ORDER BY total_months DESC)/SUM(nb_interests) OVER () * 100,1) AS cumulative_percent
    FROM nb_interests_per_month_cte
)
SELECT *
FROM cumulative_percent_cte
-- WHERE cumulative_percent >= 90
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_months</th>
      <th>nb_interests</th>
      <th>cumulative_percent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
      <td>480</td>
      <td>39.9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13</td>
      <td>82</td>
      <td>46.8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>12</td>
      <td>65</td>
      <td>52.2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11</td>
      <td>94</td>
      <td>60.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10</td>
      <td>86</td>
      <td>67.1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9</td>
      <td>95</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>8</td>
      <td>67</td>
      <td>80.6</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>90</td>
      <td>88.1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>33</td>
      <td>90.8</td>
    </tr>
    <tr>
      <th>9</th>
      <td>5</td>
      <td>38</td>
      <td>94.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>4</td>
      <td>32</td>
      <td>96.7</td>
    </tr>
    <tr>
      <th>11</th>
      <td>3</td>
      <td>15</td>
      <td>97.9</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2</td>
      <td>12</td>
      <td>98.9</td>
    </tr>
    <tr>
      <th>13</th>
      <td>1</td>
      <td>13</td>
      <td>100.0</td>
    </tr>
  </tbody>
</table>
</div>



**Result**</br>
Based on the calculation of the cumulative percentage of all records starting at 14 months, the `total_months` value that passes the 90% cumulative percentage is 6.

___
#### 3. If we were to remove all `interest_id` values which are lower than the `total_months` value we found in the previous question - how many total data points would we be removing?


```python
pd.read_sql("""
SELECT COUNT(interest_id) AS "Number of interest IDs where month_year value is less than 6"
FROM
(
    SELECT interest_id, COUNT(DISTINCT month_year) AS total_months
    FROM fresh_segments.interest_metrics
    WHERE interest_id IS NOT NULL 
    GROUP BY interest_id
) total_months
WHERE total_months < 6
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Number of interest IDs where month_year value is less than 6</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>110</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql("""
SELECT COUNT(*) AS "Number of records (data points) where month_year value is less than 6"
FROM
(
    SELECT interest_id, COUNT(DISTINCT month_year) AS total_months
    FROM fresh_segments.interest_metrics
    WHERE interest_id IS NOT NULL 
    GROUP BY interest_id
) total_months
JOIN fresh_segments.interest_metrics me ON total_months.interest_id = me.interest_id
WHERE total_months < 6
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Number of records (data points) where month_year value is less</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>400</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 4. Does this decision make sense to remove these data points from a business perspective? Use an example where there are all 14 months present to a removed interest example for your arguments - think about what it means to have less months present from a segment perspective.


```python
pd.read_sql("""
WITH total_months_cte AS
(
    SELECT interest_id, COUNT(DISTINCT month_year) AS total_months
    FROM fresh_segments.interest_metrics
    GROUP BY interest_id
),
full_table_cte AS
(
    SELECT me.interest_id, me.month_year, c.total_months
    FROM fresh_segments.interest_metrics me
    JOIN total_months_cte c ON me.interest_id = c.interest_id
)
SELECT 
    c.month_year, 
    COUNT(c.interest_id) AS nb_present_interest, 
    less.nb_removed_interest,
    CONCAT(ROUND(less.nb_removed_interest/COUNT(c.interest_id)::NUMERIC * 100, 1), ' %') AS "Removed Interest Percentage"
FROM full_table_cte c
JOIN
(
    SELECT month_year, COUNT(interest_id) AS nb_removed_interest
    FROM full_table_cte
    WHERE total_months < 6
    GROUP BY month_year
) less ON less.month_year = c.month_year

GROUP BY c.month_year, less.nb_removed_interest
ORDER BY c.month_year
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>nb_present_interest</th>
      <th>nb_removed_interest</th>
      <th>Removed Interest Percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-07-01</td>
      <td>729</td>
      <td>20</td>
      <td>2.7 %</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-08-01</td>
      <td>767</td>
      <td>15</td>
      <td>2.0 %</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-09-01</td>
      <td>780</td>
      <td>6</td>
      <td>0.8 %</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>857</td>
      <td>4</td>
      <td>0.5 %</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-01</td>
      <td>928</td>
      <td>3</td>
      <td>0.3 %</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018-12-01</td>
      <td>995</td>
      <td>9</td>
      <td>0.9 %</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019-01-01</td>
      <td>973</td>
      <td>7</td>
      <td>0.7 %</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-02-01</td>
      <td>1121</td>
      <td>49</td>
      <td>4.4 %</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-03-01</td>
      <td>1136</td>
      <td>58</td>
      <td>5.1 %</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-04-01</td>
      <td>1099</td>
      <td>64</td>
      <td>5.8 %</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2019-05-01</td>
      <td>857</td>
      <td>30</td>
      <td>3.5 %</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2019-06-01</td>
      <td>824</td>
      <td>20</td>
      <td>2.4 %</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2019-07-01</td>
      <td>864</td>
      <td>28</td>
      <td>3.2 %</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2019-08-01</td>
      <td>1149</td>
      <td>87</td>
      <td>7.6 %</td>
    </tr>
  </tbody>
</table>
</div>

**Result**</br>
When removing the interests with a total_months value lower than 6, we observe that these interests account for only 1% to 7% of the total number of interests in their corresponding months. Therefore, it is logical and valid to drop those interests, as they have minimal impact on the overall analysis.

___
#### 5. After removing these interests - how many unique interests are there for each month?


```python
# Remove the interest IDs with less than 6 months worth of data
cursor.execute("""
DELETE FROM fresh_segments.interest_metrics
WHERE interest_id IN 
(
    SELECT interest_id
    FROM fresh_segments.interest_metrics
    GROUP BY interest_id
    HAVING COUNT(month_year) < 6
);
""")

# Save the updates
conn.commit()
```


```python
pd.read_sql("""
SELECT month_year, COUNT(DISTINCT interest_id) AS "Number of Interests"
FROM fresh_segments.interest_metrics
WHERE month_year IS NOT NULL
GROUP BY month_year
ORDER BY month_year
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>Number of Interests</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-07-01</td>
      <td>709</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-08-01</td>
      <td>752</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-09-01</td>
      <td>774</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>853</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-01</td>
      <td>925</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018-12-01</td>
      <td>986</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019-01-01</td>
      <td>966</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-02-01</td>
      <td>1072</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-03-01</td>
      <td>1078</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-04-01</td>
      <td>1035</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2019-05-01</td>
      <td>827</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2019-06-01</td>
      <td>804</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2019-07-01</td>
      <td>836</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2019-08-01</td>
      <td>1062</td>
    </tr>
  </tbody>
</table>
</div>



<a id = 'C'></a>
## C. Segment Analysis

#### 1. Using our filtered dataset by removing the interests with less than 6 months worth of data, which are the top 10 and bottom 10 interests which have the largest composition values in any month_year? Only use the maximum composition value for each interest but you must keep the corresponding month_year

### Result: Top 10 interests


```python
pd.read_sql("""
WITH max_composition_cte AS
(
    SELECT 
        me.month_year, 
        me.interest_id, 
        ma.interest_name, 
        MAX(me.composition) AS max_composition
    FROM fresh_segments.interest_metrics me
    JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    GROUP BY me.month_year, me.interest_id, ma.interest_name 
)
SELECT 
    month_year, 
    interest_name AS "Top 10 interests", 
    max_composition
FROM max_composition_cte 
ORDER BY max_composition DESC
LIMIT 10
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>Top 10 interests</th>
      <th>max_composition</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-12-01</td>
      <td>Work Comes First Travelers</td>
      <td>21.20</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>Work Comes First Travelers</td>
      <td>20.28</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-11-01</td>
      <td>Work Comes First Travelers</td>
      <td>19.45</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2019-01-01</td>
      <td>Work Comes First Travelers</td>
      <td>18.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-07-01</td>
      <td>Gym Equipment Owners</td>
      <td>18.82</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2019-02-01</td>
      <td>Work Comes First Travelers</td>
      <td>18.39</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2018-09-01</td>
      <td>Work Comes First Travelers</td>
      <td>18.18</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2018-07-01</td>
      <td>Furniture Shoppers</td>
      <td>17.44</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2018-07-01</td>
      <td>Luxury Retail Shoppers</td>
      <td>17.19</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2018-10-01</td>
      <td>Luxury Boutique Hotel Researchers</td>
      <td>15.15</td>
    </tr>
  </tbody>
</table>
</div>



### Result: Bottom 10 interests


```python
pd.read_sql("""
WITH max_composition_cte AS
(
    -- Filter the maximum composition for each interest and each corresponding month_year
    
    SELECT 
        me.month_year, 
        me.interest_id, 
        ma.interest_name, 
        MAX(me.composition) AS max_composition
    FROM fresh_segments.interest_metrics me
    JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    GROUP BY me.month_year, me.interest_id, ma.interest_name 
)
SELECT 
    month_year, 
    interest_name AS "Bottom 10 interests", 
    max_composition  
FROM max_composition_cte 
ORDER BY max_composition ASC
LIMIT 10
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>Bottom 10 interests</th>
      <th>max_composition</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2019-05-01</td>
      <td>Mowing Equipment Shoppers</td>
      <td>1.51</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2019-05-01</td>
      <td>Philadelphia 76ers Fans</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2019-06-01</td>
      <td>Disney Fans</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2019-06-01</td>
      <td>New York Giants Fans</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2019-05-01</td>
      <td>Beer Aficionados</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2019-04-01</td>
      <td>United Nations Donors</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019-05-01</td>
      <td>Gastrointestinal Researchers</td>
      <td>1.52</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-05-01</td>
      <td>LED Lighting Shoppers</td>
      <td>1.53</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-05-01</td>
      <td>Crochet Enthusiasts</td>
      <td>1.53</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-06-01</td>
      <td>Online Directory Searchers</td>
      <td>1.53</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 2. Which 5 interests had the lowest average ranking value?

The ranking value ranges from 1 to 1194.


```python
pd.read_sql("""
SELECT DISTINCT ranking
FROM fresh_segments.interest_metrics
ORDER BY ranking ASC
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>986</th>
      <td>1189</td>
    </tr>
    <tr>
      <th>987</th>
      <td>1190</td>
    </tr>
    <tr>
      <th>988</th>
      <td>1191</td>
    </tr>
    <tr>
      <th>989</th>
      <td>1193</td>
    </tr>
    <tr>
      <th>990</th>
      <td>1194</td>
    </tr>
  </tbody>
</table>
<p>991 rows × 1 columns</p>
</div>



**Result**


```python
pd.read_sql("""
SELECT 
    me.interest_id, 
    ma.interest_name, 
    ROUND(AVG(me.ranking),1) AS avg_ranking
FROM fresh_segments.interest_metrics me
JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
GROUP BY me.interest_id, ma.interest_name
ORDER BY avg_ranking ASC
LIMIT 5
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_id</th>
      <th>interest_name</th>
      <th>avg_ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>41548</td>
      <td>Winter Apparel Shoppers</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>42203</td>
      <td>Fitness Activity Tracker Users</td>
      <td>4.1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>115</td>
      <td>Mens Shoe Shoppers</td>
      <td>5.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>171</td>
      <td>Shoe Shoppers</td>
      <td>9.4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6206</td>
      <td>Preppy Clothing Shoppers</td>
      <td>11.9</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 3. Which 5 interests had the largest standard deviation in their `percentile_ranking` value?


```python
pd.read_sql("""
SELECT 
    me.interest_id, 
    ma.interest_name, 
    ROUND(STDDEV(percentile_ranking)::NUMERIC, 2) AS standard_deviation
FROM fresh_segments.interest_metrics me
JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
GROUP BY me.interest_id, ma.interest_name
ORDER BY standard_deviation DESC
LIMIT 5
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_id</th>
      <th>interest_name</th>
      <th>standard_deviation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>23</td>
      <td>Techies</td>
      <td>30.18</td>
    </tr>
    <tr>
      <th>1</th>
      <td>20764</td>
      <td>Entertainment Industry Decision Makers</td>
      <td>28.97</td>
    </tr>
    <tr>
      <th>2</th>
      <td>38992</td>
      <td>Oregon Trip Planners</td>
      <td>28.32</td>
    </tr>
    <tr>
      <th>3</th>
      <td>43546</td>
      <td>Personalized Gift Shoppers</td>
      <td>26.24</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10839</td>
      <td>Tampa and St Petersburg Trip Planners</td>
      <td>25.61</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 4. For the 5 interests found in the previous question - what was minimum and maximum percentile_ranking values for each interest and its corresponding year_month value? Can you describe what is happening for these 5 interests?


```python
pd.read_sql("""
WITH ranking_percentile_cte AS
(
    SELECT 
        me.month_year, 
        me.interest_id, 
        ma.interest_name, 
        me.percentile_ranking, 
        RANK() OVER (PARTITION BY me.interest_id ORDER BY me.percentile_ranking) AS asc_rk,
        RANK() OVER (PARTITION BY me.interest_id ORDER BY me.percentile_ranking DESC) AS desc_rk
    FROM fresh_segments.interest_metrics me
    JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    WHERE interest_id IN
    (
        -- Retrieve the 5 interests with the largest standard deviation in their percentile_ranking value
        
        SELECT interest_id
        FROM fresh_segments.interest_metrics
        WHERE interest_id IS NOT NULL
        GROUP BY interest_id
        ORDER BY STDDEV(percentile_ranking) DESC
        LIMIT 5
    ) 
)
SELECT 
    interest_name, 
    interest_id,
    month_year, 
    percentile_ranking, 
    CASE WHEN asc_rk = 1 THEN 'Min' ELSE 'Max' END AS percentile_rank_value_type
FROM ranking_percentile_cte
WHERE asc_rk = 1 OR desc_rk = 1
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_name</th>
      <th>interest_id</th>
      <th>month_year</th>
      <th>percentile_ranking</th>
      <th>percentile_rank_value_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Techies</td>
      <td>23</td>
      <td>2019-08-01</td>
      <td>7.92</td>
      <td>Min</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Techies</td>
      <td>23</td>
      <td>2018-07-01</td>
      <td>86.69</td>
      <td>Max</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Tampa and St Petersburg Trip Planners</td>
      <td>10839</td>
      <td>2019-03-01</td>
      <td>4.84</td>
      <td>Min</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Tampa and St Petersburg Trip Planners</td>
      <td>10839</td>
      <td>2018-07-01</td>
      <td>75.03</td>
      <td>Max</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Entertainment Industry Decision Makers</td>
      <td>20764</td>
      <td>2019-08-01</td>
      <td>11.23</td>
      <td>Min</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Entertainment Industry Decision Makers</td>
      <td>20764</td>
      <td>2018-07-01</td>
      <td>86.15</td>
      <td>Max</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Oregon Trip Planners</td>
      <td>38992</td>
      <td>2019-07-01</td>
      <td>2.20</td>
      <td>Min</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Oregon Trip Planners</td>
      <td>38992</td>
      <td>2018-11-01</td>
      <td>82.44</td>
      <td>Max</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Personalized Gift Shoppers</td>
      <td>43546</td>
      <td>2019-06-01</td>
      <td>5.70</td>
      <td>Min</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Personalized Gift Shoppers</td>
      <td>43546</td>
      <td>2019-03-01</td>
      <td>73.15</td>
      <td>Max</td>
    </tr>
  </tbody>
</table>
</div>



Here is an alternative output achieved by putting columns side by side using a JOIN statement.


```python
pd.read_sql("""
WITH ranking_percentile_cte AS
(
    SELECT 
        me.month_year, 
        me.interest_id, 
        ma.interest_name, 
        me.percentile_ranking, 
        RANK() OVER (PARTITION BY me.interest_id ORDER BY me.percentile_ranking) AS asc_rk,
        RANK() OVER (PARTITION BY me.interest_id ORDER BY me.percentile_ranking DESC) AS desc_rk
    FROM fresh_segments.interest_metrics me
    JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    WHERE interest_id IN
    (
        -- Retrieve the 5 interests with the largest standard deviation in their percentile_ranking value
        
        SELECT interest_id
        FROM fresh_segments.interest_metrics
        WHERE interest_id IS NOT NULL
        GROUP BY interest_id
        ORDER BY STDDEV(percentile_ranking) DESC
        LIMIT 5
    ) 
)
SELECT 
    min.interest_name, 
    min.interest_id,
    min.month_year AS month_year_minval,
    min.percentile_ranking AS percentile_ranking_minval,
    max.month_year_maxval,
    max.percentile_ranking_maxval
FROM ranking_percentile_cte min
JOIN 
(
    SELECT 
        interest_id,
        month_year AS month_year_maxval, 
        percentile_ranking AS percentile_ranking_maxval
    FROM ranking_percentile_cte
    WHERE desc_rk = 1
    
) max ON min.interest_id = max.interest_id
WHERE asc_rk = 1
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_name</th>
      <th>interest_id</th>
      <th>month_year_minval</th>
      <th>percentile_ranking_minval</th>
      <th>month_year_maxval</th>
      <th>percentile_ranking_maxval</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Techies</td>
      <td>23</td>
      <td>2019-08-01</td>
      <td>7.92</td>
      <td>2018-07-01</td>
      <td>86.69</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Tampa and St Petersburg Trip Planners</td>
      <td>10839</td>
      <td>2019-03-01</td>
      <td>4.84</td>
      <td>2018-07-01</td>
      <td>75.03</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Entertainment Industry Decision Makers</td>
      <td>20764</td>
      <td>2019-08-01</td>
      <td>11.23</td>
      <td>2018-07-01</td>
      <td>86.15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Oregon Trip Planners</td>
      <td>38992</td>
      <td>2019-07-01</td>
      <td>2.20</td>
      <td>2018-11-01</td>
      <td>82.44</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Personalized Gift Shoppers</td>
      <td>43546</td>
      <td>2019-06-01</td>
      <td>5.70</td>
      <td>2019-03-01</td>
      <td>73.15</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. How would you describe our customers in this segment based off their composition and ranking values? What sort of products or services should we show to these customers and what should we avoid?

**Result**
- Topics related to travels, fitness, furniture, and luxury lifestyle are the top 10 interests among our customers. Therefore, Fresh Segments should prioritize displaying more products or services related to travel, fitness, furniture, and luxury lifestyle.

- Products or services to avoid showing on Fresh Segments include anything related to Mowing Equipment, Crochet (Arts and Crafts), Social and Charitable Causes, and LED products.


<a id = 'D'></a>
## D. Index Analysis
The `index_value` is a measure which can be used to reverse calculate the average composition for Fresh Segments’ clients.

Average composition can be calculated by dividing the composition column by the index_value column rounded to 2 decimal places.

#### 1. What is the top 10 interests by the average composition for each month?


```python
pd.read_sql("""
WITH avg_comp_cte AS
(
    SELECT *, DENSE_RANK() OVER (PARTITION BY month_year ORDER BY avg_composition DESC) AS rank
    FROM
    (
        SELECT 
            me.month_year, 
            me.interest_id,             
            ma.interest_name, 
            ROUND(composition::NUMERIC/index_value::NUMERIC, 2) AS avg_composition
        FROM fresh_segments.interest_metrics me
        JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    ) avg
)
SELECT *
FROM avg_comp_cte
WHERE month_year IS NOT NULL AND interest_id IS NOT NULL AND rank <= 10
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>interest_id</th>
      <th>interest_name</th>
      <th>avg_composition</th>
      <th>rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-07-01</td>
      <td>6324</td>
      <td>Las Vegas Trip Planners</td>
      <td>7.36</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-07-01</td>
      <td>6284</td>
      <td>Gym Equipment Owners</td>
      <td>6.94</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-07-01</td>
      <td>4898</td>
      <td>Cosmetics and Beauty Shoppers</td>
      <td>6.78</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-07-01</td>
      <td>77</td>
      <td>Luxury Retail Shoppers</td>
      <td>6.61</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-07-01</td>
      <td>39</td>
      <td>Furniture Shoppers</td>
      <td>6.51</td>
      <td>5</td>
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
      <th>145</th>
      <td>2019-08-01</td>
      <td>77</td>
      <td>Luxury Retail Shoppers</td>
      <td>2.59</td>
      <td>6</td>
    </tr>
    <tr>
      <th>146</th>
      <td>2019-08-01</td>
      <td>4931</td>
      <td>Marijuana Legalization Advocates</td>
      <td>2.56</td>
      <td>7</td>
    </tr>
    <tr>
      <th>147</th>
      <td>2019-08-01</td>
      <td>6253</td>
      <td>Medicare Researchers</td>
      <td>2.55</td>
      <td>8</td>
    </tr>
    <tr>
      <th>148</th>
      <td>2019-08-01</td>
      <td>6208</td>
      <td>Recently Retired Individuals</td>
      <td>2.53</td>
      <td>9</td>
    </tr>
    <tr>
      <th>149</th>
      <td>2019-08-01</td>
      <td>6225</td>
      <td>Piercing Fans</td>
      <td>2.50</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 5 columns</p>
</div>



___
#### 2. For all of these top 10 interests - which interest appears the most often?


```python
pd.read_sql("""
WITH avg_comp_cte AS
(
    SELECT *, DENSE_RANK() OVER (PARTITION BY month_year ORDER BY avg_composition DESC) AS rank
    FROM
    (
        SELECT 
            me.month_year, 
            me.interest_id,             
            ma.interest_name, 
            ROUND(composition::NUMERIC/index_value::NUMERIC, 2) AS avg_composition
        FROM fresh_segments.interest_metrics me
        JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
    ) avg
)
SELECT 
    interest_name, 
    COUNT(interest_name) AS nb_occurrences
FROM avg_comp_cte
WHERE month_year IS NOT NULL AND interest_id IS NOT NULL AND rank <= 10
GROUP BY interest_name
ORDER BY nb_occurrences DESC
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>interest_name</th>
      <th>nb_occurrences</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Alabama Trip Planners</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Solar Energy Researchers</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Luxury Bedding Shoppers</td>
      <td>10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Nursing and Physicians Assistant Journal Resea...</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>New Years Eve Party Ticket Purchasers</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Readers of Honduran Content</td>
      <td>9</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Work Comes First Travelers</td>
      <td>8</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Teen Girl Clothing Shoppers</td>
      <td>8</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Christmas Celebration Researchers</td>
      <td>7</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Las Vegas Trip Planners</td>
      <td>5</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Gym Equipment Owners</td>
      <td>5</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Luxury Retail Shoppers</td>
      <td>5</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Asian Food Enthusiasts</td>
      <td>5</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Recently Retired Individuals</td>
      <td>5</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Furniture Shoppers</td>
      <td>5</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Cosmetics and Beauty Shoppers</td>
      <td>5</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Readers of Catholic News</td>
      <td>4</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Restaurant Supply Shoppers</td>
      <td>4</td>
    </tr>
    <tr>
      <th>18</th>
      <td>PlayStation Enthusiasts</td>
      <td>4</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Medicare Researchers</td>
      <td>3</td>
    </tr>
    <tr>
      <th>20</th>
      <td>HDTV Researchers</td>
      <td>3</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Medicare Provider Researchers</td>
      <td>2</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Cruise Travel Intenders</td>
      <td>2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Chelsea Fans</td>
      <td>2</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Piercing Fans</td>
      <td>1</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Family Adventures Travelers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Patio Furniture Shoppers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Teen Content Readers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Portugal Trip Planners</td>
      <td>1</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Freelancers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Marijuana Legalization Advocates</td>
      <td>1</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Video Gamers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Gamers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Luxury Boutique Hotel Researchers</td>
      <td>1</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Medicare Price Shoppers</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**Result**</br>
The interests that appeared most often in the Fresh Segment are the ones that have the highest number of occurrences among all the `month_year` values. Hence,
- **Alabama Trip Planners**, **Luxury Bedding Shoppers**, and **Solar Energy Researchers** appear most frequently, with a total count of 10 occurrences.
- **Readers of Honduran Content**, **Nursing and Physician Assistant Journal Researchers**, and **New Year's Eve Party Ticket Purchasers** are the second most appearing interests, with a total of 9 occurrences.
- **Teen Girl Clothing Shoppers** and **Work Comes First Travelers** are the third most appearing interests, with a total of 8 occurrences.

___
#### 3. What is the average of the average composition for the top 10 interests for each month?


```python
pd.read_sql("""
WITH avg_comp_cte AS
(
    SELECT *, DENSE_RANK() OVER (PARTITION BY month_year ORDER BY avg_composition DESC) AS rank
    FROM
    (
        SELECT 
            month_year,
            interest_id,
            ROUND(composition::NUMERIC/index_value::NUMERIC, 2) AS avg_composition
        FROM fresh_segments.interest_metrics
    ) avg
)
SELECT 
    month_year, 
    ROUND(AVG(avg_composition),2) AS avg_composition
FROM avg_comp_cte
WHERE month_year IS NOT NULL AND rank <= 10
GROUP BY month_year
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>avg_composition</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-07-01</td>
      <td>6.04</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-08-01</td>
      <td>5.95</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-09-01</td>
      <td>6.90</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>7.01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-01</td>
      <td>6.62</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018-12-01</td>
      <td>6.65</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019-01-01</td>
      <td>6.24</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-02-01</td>
      <td>6.58</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-03-01</td>
      <td>6.07</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-04-01</td>
      <td>5.75</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2019-05-01</td>
      <td>3.50</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2019-06-01</td>
      <td>2.39</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2019-07-01</td>
      <td>2.72</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2019-08-01</td>
      <td>2.62</td>
    </tr>
  </tbody>
</table>
</div>



**Result**
- October 2018 has the highest average composition of 7.01 among the top 10 interests.
- The month with the lowest average composition is June 2019, with only 2.39.

___
#### 4. What is the 3 month rolling average of the max average composition value from September 2018 to August 2019 and include the previous top ranking interests in the same output shown below.

<details>
<summary>Required output for question 4</summary>

month_year | interest_name | max_index_composition | 3_month_moving_avg | 1_month_ago | 2_months_ago  
--- | --- | --- | --- | --- | --- 
2018-09-01 | Work Comes First Travelers | 8.26 | 7.61 | Las Vegas Trip Planners: 7.21 | Las Vegas Trip Planners: 7.36 
2018-10-01 | Work Comes First Travelers | 9.14 | 8.20 | Work Comes First Travelers: 8.26 | Las Vegas Trip Planners: 7.21 
2018-11-01 | Work Comes First Travelers | 8.28 | 8.56 | Work Comes First Travelers: 9.14 | Work Comes First Travelers: 8.26  
2018-12-01 | Work Comes First Travelers | 8.31 | 8.58 | Work Comes First Travelers: 8.28 | Work Comes First Travelers: 9.14  
2019-01-01 | Work Comes First Travelers | 7.66 | 8.08 | Work Comes First Travelers: 8.31 | Work Comes First Travelers: 8.28  
2019-02-01 | Work Comes First Travelers | 7.66 | 7.88 | Work Comes First Travelers: 7.66 | Work Comes First Travelers: 8.31 
2019-03-01 | Alabama Trip Planners | 6.54 | 7.29 | Work Comes First Travelers: 7.66 | Work Comes First Travelers: 7.66 
2019-04-01 | Solar Energy Researchers | 6.28 | 6.83 | Alabama Trip Planners: 6.54 | Work Comes First Travelers: 7.66 
2019-05-01 | Readers of Honduran Content | 4.41 | 5.74 | Solar Energy Researchers: 6.28 | Alabama Trip Planners: 6.54  
2019-06-01 | Las Vegas Trip Planners | 2.77 | 4.49 | Readers of Honduran Content: 4.41 | Solar Energy Researchers: 6.28 
2019-07-01 | Las Vegas Trip Planners | 2.82 | 3.33 | Las Vegas Trip Planners: 2.77 | Readers of Honduran Content: 4.41 
2019-08-01 | Cosmetics and Beauty Shoppers | 2.73 | 2.77 | Las Vegas Trip Planners: 2.82 | Las Vegas Trip Planners: 2.77 

</details>

**Result**


```python
pd.read_sql("""
WITH ranking_interests_cte AS
(
    -- Rank each interest by the index_composition value for each month_year 
    
    SELECT 
        me.month_year, 
        ma.interest_name, 
        ROUND(me.composition::NUMERIC/me.index_value::NUMERIC, 2) AS index_composition,
        DENSE_RANK() OVER (PARTITION BY me.month_year ORDER BY me.composition::NUMERIC/me.index_value::NUMERIC DESC) AS rank
    FROM fresh_segments.interest_metrics me
    JOIN fresh_segments.interest_map ma ON me.interest_id = ma.id
)
SELECT *
FROM
(
    -- Calculate the 3-month rolling average of the max_index_composition.
    -- Determine the interests and their corresponding index_composition from 1 month and 2 months ago.
    
    SELECT 
        month_year, 
        interest_name, 
        index_composition AS max_index_composition, 
        ROUND(AVG(index_composition) OVER (ORDER BY month_year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) AS "3_month_moving_avg",
        LAG(interest_name) OVER () || ': ' || LAG(index_composition) OVER () AS "1_months_ago",
        LAG(interest_name, 2) OVER () || ': ' || LAG(index_composition, 2) OVER () AS "2_months_ago"
    FROM ranking_interests_cte
    WHERE rank = 1
    
) computing
WHERE month_year BETWEEN '2018-09-01' AND '2019-08-01'
""", conn)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month_year</th>
      <th>interest_name</th>
      <th>max_index_composition</th>
      <th>3_month_moving_avg</th>
      <th>1_months_ago</th>
      <th>2_months_ago</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-09-01</td>
      <td>Work Comes First Travelers</td>
      <td>8.26</td>
      <td>7.61</td>
      <td>Las Vegas Trip Planners: 7.21</td>
      <td>Las Vegas Trip Planners: 7.36</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>Work Comes First Travelers</td>
      <td>9.14</td>
      <td>8.20</td>
      <td>Work Comes First Travelers: 8.26</td>
      <td>Las Vegas Trip Planners: 7.21</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-11-01</td>
      <td>Work Comes First Travelers</td>
      <td>8.28</td>
      <td>8.56</td>
      <td>Work Comes First Travelers: 9.14</td>
      <td>Work Comes First Travelers: 8.26</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-12-01</td>
      <td>Work Comes First Travelers</td>
      <td>8.31</td>
      <td>8.58</td>
      <td>Work Comes First Travelers: 8.28</td>
      <td>Work Comes First Travelers: 9.14</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2019-01-01</td>
      <td>Work Comes First Travelers</td>
      <td>7.66</td>
      <td>8.08</td>
      <td>Work Comes First Travelers: 8.31</td>
      <td>Work Comes First Travelers: 8.28</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2019-02-01</td>
      <td>Work Comes First Travelers</td>
      <td>7.66</td>
      <td>7.88</td>
      <td>Work Comes First Travelers: 7.66</td>
      <td>Work Comes First Travelers: 8.31</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019-03-01</td>
      <td>Alabama Trip Planners</td>
      <td>6.54</td>
      <td>7.29</td>
      <td>Work Comes First Travelers: 7.66</td>
      <td>Work Comes First Travelers: 7.66</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019-04-01</td>
      <td>Solar Energy Researchers</td>
      <td>6.28</td>
      <td>6.83</td>
      <td>Alabama Trip Planners: 6.54</td>
      <td>Work Comes First Travelers: 7.66</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019-05-01</td>
      <td>Readers of Honduran Content</td>
      <td>4.41</td>
      <td>5.74</td>
      <td>Solar Energy Researchers: 6.28</td>
      <td>Alabama Trip Planners: 6.54</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019-06-01</td>
      <td>Las Vegas Trip Planners</td>
      <td>2.77</td>
      <td>4.49</td>
      <td>Readers of Honduran Content: 4.41</td>
      <td>Solar Energy Researchers: 6.28</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2019-07-01</td>
      <td>Las Vegas Trip Planners</td>
      <td>2.82</td>
      <td>3.33</td>
      <td>Las Vegas Trip Planners: 2.77</td>
      <td>Readers of Honduran Content: 4.41</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2019-08-01</td>
      <td>Cosmetics and Beauty Shoppers</td>
      <td>2.73</td>
      <td>2.77</td>
      <td>Las Vegas Trip Planners: 2.82</td>
      <td>Las Vegas Trip Planners: 2.77</td>
    </tr>
  </tbody>
</table>
</div>



___
#### 5. Provide a possible reason why the max average composition might change from month to month? Could it signal something is not quite right with the overall business model for Fresh Segments?

**Result**</br>
One possible explanation for the maximum average composition variation from month to month is that customers may change their interests and switch to other topics. The analysis shows that the curiosity of humans/customers is not consistent.
- However, we can observe that `Work Comes First Travelers` has been the top interest for 6 consecutive months, from September 2018 to February 2019.
- Similarly, `Las Vegas Trip Planners` is also the top interest on the Fresh Segments website for four specific months: July 2018, August 2018, June 2019, and July 2019.


```python
conn.close()
```


```python

```
