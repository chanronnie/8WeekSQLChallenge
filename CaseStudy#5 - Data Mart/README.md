# Case Study #5: Data Mart 🧺
[![View Main Folder](https://img.shields.io/badge/View-Main_Folder-F5788D.svg?logo=GitHub)](https://github.com/chanronnie/8WeekSQLChallenge)
[![View Case Study 5](https://img.shields.io/badge/View-Case_Study_5-red)](https://8weeksqlchallenge.com/case-study-5/)</br>
![5](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/a07627c4-27c5-47f5-a6f8-dc3379831287)


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
![DataMart](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/b0bb72b8-e579-41c6-99f0-ce73718f0d73)


## Datasets
The Case Study #5 contains 1 dataset:</br>
**`weekly_sales`**: This table shows all the sales records made at Data Mart.

<details>
  <summary>View dataset</summary>

Here are the 10 random rows of the `weekly_sales` dataset

week_date | region | platform | segment | customer_type | transactions | sales
--- | --- | --- | --- | --- | --- | ---
9/9/20 | OCEANIA | Shopify | C3 | New | 610 | 110033.89
29/7/20 | AFRICA | Retail | C1 | New | 110692 | 3053771.19
22/7/20 | EUROPE | Shopify | C4 | Existing | 24 | 8101.54
13/5/20 | AFRICA | Shopify | null | Guest | 5287 | 1003301.37
24/7/19 | ASIA | Retail | C1 | New | 127342 | 3151780.41
10/7/19 | CANADA | Shopify | F3 | New | 51 | 8844.93
26/6/19 | OCEANIA | Retail | C3 | New | 152921 | 5551385.36
29/5/19 | SOUTH AMERICA | Shopify | null | New | 53 | 10056.2
22/8/18 | AFRICA | Retail | null | Existing | 31721 | 1718863.58
25/7/18 | SOUTH AMERICA | Retail | null | New | 2136 | 81757.91

</details>

## Case Study Questions
Case Study #5 is categorized into 4 question groups\
To view the specific section, please open the link in a *`new tab`* or *`window`*.\
[A. Data Cleansing](CaseStudy5_solutions.md#A)\
[B. Data Exploration](CaseStudy5_solutions.md#B)\
[C. Before & After Analysis](CaseStudy5_solutions.md#C)\
[D. Bonus Question](CaseStudy5_solutions.md#D)

## Solutions
- View `data_mart` database: [**here**](CaseStudy5_schema.sql)
- View Solution:
    - [**Markdown File**](CaseStudy5_solutions.md): offers a more fluid and responsive viewing experience
    - [**Jupyter Notebook**](CaseStudy5_solutions.ipynb): contains the original code


## PostgreSQL Topics Covered
- Data Cleaning
- Pivot Table using CASE WHEN
- DATE Type Manipulation
- Common Table Expressions (CTE)
- Window Functions
- Subqueries
- JOIN, UNION ALL
