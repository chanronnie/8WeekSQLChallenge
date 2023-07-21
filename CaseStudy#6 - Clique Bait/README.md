# Case Study #6: Clique Bait 🪝</br>
[![View Main Folder](https://img.shields.io/badge/View-Main_Folder-F5788D.svg?logo=GitHub)](https://github.com/chanronnie/8WeekSQLChallenge)
[![View Case Study 6](https://img.shields.io/badge/View-Case_Study_6-06816E)](https://8weeksqlchallenge.com/case-study-6/)</br>
![6](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/364b11b1-7a4b-47d9-b7a6-6fd87075d140)


The case study presented here is part of the **8 Week SQL Challenge**.\
It is kindly brought to us by [**Data With Danny**](https://8weeksqlchallenge.com).

This time, I am using `PostgreSQL queries` (instead of MySQL) in `Jupyter Notebook` to quickly view results, which provides me with an opportunity 
  - to learn PostgreSQL
  - to utilize handy mathematical and string functions.



## Table of Contents
* [Entity Relationship Diagram](#entity-relationship-diagram)
* [Datasets](#datasets)
* [Case Study Questions](#case-study-questions)
* [Solutions](#solutions)
* [PostgreSQL Topics Covered](#postgresql-topics-covered)

## Entity Relationship Diagram
The question A will require the creation of an Entity-Relationship Diagram (ERD).


## Datasets
The Case Study #6 contains 5 dataset:


### `users`
<details>
<summary>
View table
</summary>
  
`users` : This table shows all the user_id along with their unique cookie_id (PK).
  
user_id | cookie_id | start_date
--- | --- | ---
397 | 759ff | 2020-03-30 00:00:00
215 | 863329 | 2020-01-26 00:00:00
191 | eefca9 | 2020-03-15 00:00:00
89 | 764796 | 2020-01-07 00:00:00
127 | 17ccc5 | 2020-01-22 00:00:00
81 | b0b666 | 2020-03-01 00:00:00
260 | a4f236 | 2020-01-08 00:00:00
203 | d1182f | 2020-04-18 00:00:00
23 | 12dbc8 | 2020-01-18 00:00:00
375 | f61d69 | 2020-01-03 00:00:00
  
</details>


### `events`
<details>
<summary>
View table
</summary>
  
`events` : The table lists all the website interactions by customers (cookie_id, webpage visited, etc.).

visit_id | cookie_id | page_id | event_type | sequence_number | event_time
--- | --- | --- | --- | --- | --- 
719fd3 | 3d83d3 | 5 | 1 | 4 | 2020-03-02 00:29:09.975502
fb1eb1 | c5ff25 | 5 | 2 | 8 | 2020-01-22 07:59:16.761931
23fe81 | 1e8c2d | 10 | 1 | 9 | 2020-03-21 13:14:11.745667
ad91aa | 648115 | 6 | 1 | 3 | 2020-04-27 16:28:09.824606
5576d7 | ac418c | 6 | 1 | 4 | 2020-01-18 04:55:10.149236
48308b | c686c1 | 8 | 1 | 5 | 2020-01-29 06:10:38.702163
46b17d | 78f9b3 | 7 | 1 | 12 | 2020-02-16 09:45:31.926407
9fd196 | ccf057 | 4 | 1 | 5 | 2020-02-14 08:29:12.922164
edf853 | f85454 | 1 | 1 | 1 | 2020-02-22 12:59:07.652207
3c6716 | 02e74f | 3 | 2 | 5 | 2020-01-31 17:56:20.777383

</details>


### `event_identifier`
<details>
<summary>
View table
</summary>
  
`event_identifier` : The table maps the event_type to its corresponding event_name.

event_type | event_name
--- | ---
1 | Page View
2 | Add to Cart
3 | Purchase
4 | Ad Impression
5 | Ad Click

</details>


### `campaign_identifier`
<details>
<summary>
View table
</summary>
  
`campaign_identifier`: The table lists the information of the three campaigns that ran on the Clique Bait website.

campaign_id | products | campaign_name | start_date | end_date
--- | --- | --- | --- | --- 
1 | 1-3 | BOGOF - Fishing For Compliments | 2020-01-01 00:00:00 | 2020-01-14 00:00:00
2 | 4-5 | 25% Off - Living The Lux Life | 2020-01-15 00:00:00 | 2020-01-28 00:00:00
3 | 6-8 | Half Off - Treat Your Shellf(ish) | 2020-02-01 00:00:00 | 2020-03-31 00:00:00

</details>


### `page_hierarchy`
<details>
<summary>
View table
</summary>
  
`page_hierarchy`: The table maps the page_id to its page_name.

page_id | page_name | product_category | product_id
--- | --- | --- | --- 
1 | Home Page | null | null
2 | All Products | null | null
3 | Salmon | Fish | 1
4 | Kingfish | Fish | 2
5 | Tuna | Fish | 3
6 | Russian Caviar | Luxury | 4
7 | Black Truffle | Luxury | 5
8 | Abalone | Shellfish | 6
9 | Lobster | Shellfish | 7
10 | Crab | Shellfish | 8
11 | Oyster | Shellfish | 9
12 | Checkout | null | null
13 | Confirmation | null | null

</details>


## Case Study Questions
Case Study #6 is categorized into 4 question groups\
To view the specific section, please open the link in a *`new tab`* or *`window`*.\
[A. Enterprise Relationship Diagram](CaseStudy6_solutions.md#A)\
[B. Digital Analysis](CaseStudy6_solutions.md#B)\
[C. Product Funnel Analysis](CaseStudy6_solutions.md#C)\
[D. Campaigns Analysis](CaseStudy6_solutions.md#D)

## Solutions
- View `clique_bait` database: [**CaseStudy6_schema.sql**](https://raw.githubusercontent.com/chanronnie/8WeekSQLChallenge/main/CaseStudy%236%20-%20Clique%20Bait/CaseStudy6_schema.sql)
- View Solution:
    - [**Markdown File**](CaseStudy6_solutions.md): offers a more fluid and responsive viewing experience
    - [**Jupyter Notebook**](CaseStudy6_solutions.ipynb): contains the original code

## PostgreSQL Topics Covered
- Establish relationships between datasets to create an Entity-Relationship Diagram (ERD)
- Data Cleaning
- Create Tables
- Pivot Table using CASE WHEN
- DATE and STRING Type Manipulation
- Common Table Expressions (CTE)
- Window Functions
- Subqueries
- JOIN, UNION ALL
