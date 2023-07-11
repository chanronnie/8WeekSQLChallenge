# Case Study #4: Data Bank 💱
![4](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/8d5eb6d0-c3d2-4e6c-998d-6c00fff3d516)


The case study presented here is part of the **8 Week SQL Challenge**.\
It is kindly brought to us by [**Data With Danny**](https://8weeksqlchallenge.com).

This time, I am using `PostgreSQL queries` (instead of MySQL) in `Jupyter Notebook` to quickly view results, which provides me with an opportunity 
  - to learn PostgreSQL
  - to utilize handy mathematical and string functions.



💱 View **Case Study #4**: [**here**](https://8weeksqlchallenge.com/case-study-4/).

## Table of Contents
* [Entity Relationship Diagram](#entity-relationship-diagram)
* [Datasets](#datasets)
* [Case Study Questions](#case-study-questions)
* [Solutions](#solutions)
* [PostgreSQL Topics Covered](#postgresql-topics-covered)

## Entity Relationship Diagram
![DataBank](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/5e232441-2712-4812-90a3-3a9ee5cd1ef0)




## Datasets
The Case Study #4 contains 3 tables:
- **regions**: This table maps the region_id to its respective region_name values.
- **customer_nodes**: This table lists the regions and node reallocation informations of all Data Bank customers.
- **customer_transactions**: This table lists all the transaction informations of all Data Bank customers.

## Case Study Questions
Case Study #4 is categorized into 3 question groups\
To view the specific section, please open the link in a *`new tab`* or *`window`*.\
[A. Customer Nodes Exploration](CaseStudy4_solutions.md#A)\
[B. Customer Transactions](CaseStudy4_solutions.md#B)\
[C. Data Allocation Challenge](CaseStudy4_solutions.md#C)

## Solutions
- View `data_bank` database: [**here**](CaseStudy4_schema.sql)
- View Solution:
    - [**Markdown File**](CaseStudy4_solutions.md): offers a more fluid and responsive viewing experience
    - [**Jupyter Notebook**](CaseStudy4_solutions.ipynb): contains the original code

## PostgreSQL Topics Covered
- Common Table Expressions (CTE)
- Window Functions
- Subqueries
- JOIN, UNION ALL
- MEDIAN, PERCENTILE
