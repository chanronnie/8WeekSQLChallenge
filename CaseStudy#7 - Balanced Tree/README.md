# Case Study #7: Balanced Tree Clothing Co.🌲</br>
[![View Main Folder](https://img.shields.io/badge/View-Main_Folder-F5788D.svg?logo=GitHub)](https://github.com/chanronnie/8WeekSQLChallenge)
[![View Case Study 7](https://img.shields.io/badge/View-Case_Study_7-06816E)](https://8weeksqlchallenge.com/case-study-7/)</br>
![7](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/d355752c-2e17-4706-a6cf-f92310e84fa1)



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
Again, I used the [DB Diagram tool](https://dbdiagram.io/home) to create the following ERD.
![balanced_tree](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/9867786d-6078-42f8-abf5-3565768d2832)



<details>
<summary>View code to create ERD</summary>
  
Here is the code that I used for creating the ERD for all the Balanced Tree datasets on DB Diagram tool

```markdown
TABLE product_hierarchy {
  "id" INTEGER
  "parent_id" INTEGER
  "level_text" VARCHAR(19)
  "level_name" VARCHAR(8)
}

TABLE product_prices {
  "id" INTEGER
  "product_id" VARCHAR(6)
  "price" INTEGER
}

TABLE product_details {
  "product_id" VARCHAR(6)
  "price" INTEGER
  "product_name" VARCHAR(32)
  "category_id" INTEGER
  "segment_id" INTEGER
  "style_id" INTEGER
  "category_name" VARCHAR(6)
  "segment_name" VARCHAR(6)
  "style_name" VARCHAR(19)
  }

TABLE sales {
  "prod_id" VARCHAR(6)
  "qty" INTEGER
  "price" INTEGER
  "discount" INTEGER
  "member" BOOLEAN
  "txn_id" VARCHAR(6)
  "start_txn_time" TIMESTAMP
}

# Establish relationships between datasets
REF: sales.prod_id > product_details.product_id
REF: product_hierarchy.id - product_prices.id
```

</details>


## Datasets
The Case Study #7 contains 4 dataset:

<details>
  <summary>View Table: product_details</summary>

The `product_details` dataset lists the product information.
  
product_id | price | product_name | category_id | segment_id | style_id | category_name | segment_name | style_name
--- | --- | --- | --- | --- | --- | --- | --- | ---  
c4a632 | 13 | Navy Oversized Jeans - Womens | 1 | 3 | 7	 | Womens | Jeans | Navy Oversized
e83aa3 | 32 | Black Straight Jeans - Womens | 1 | 3 | 8 | Womens | Jeans | Black Straight
e31d39 | 10 | Cream Relaxed Jeans - Womens | 1 | 3 | 9 | Womens | Jeans | Cream Relaxed
d5e9a6 | 23 | Khaki Suit Jacket - Womens | 1 | 4 | 10 | Womens | Jacket | Khaki Suit
72f5d4 | 19 | Indigo Rain Jacket - Womens | 1 | 4 | 11 | Womens | Jacket | Indigo Rain
9ec847 | 54 | Grey Fashion Jacket - Womens | 1 | 4 | 12 | Womens | Jacket | Grey Fashion
5d267b | 40 | White Tee Shirt - Mens | 2 | 5 | 13 | Mens | Shirt | White Tee
c8d436 | 10 | Teal Button Up Shirt - Mens | 2 | 5 | 14 | Mens | Shirt | Teal Button Up
2a2353 | 57 | Blue Polo Shirt - Mens | 2 | 5 | 15 | Mens | Shirt | Blue Polo
f084eb | 36 | Navy Solid Socks - Mens | 2 | 6 | 16 | Mens | Socks | Navy Solid
b9a74d | 17 | White Striped Socks - Mens | 2 | 6 | 17 | Mens | Socks | White Striped
2feb6b | 29 | Pink Fluro Polkadot Socks - Mens | 2 | 6 | 18 | Mens | Socks | Pink Fluro Polkadot

  
</details>

<details>
  <summary>View Table: sales</summary>

The `sales` dataset lists product level information (transactions made for Balanced Tree Co.)

prod_id | qty | price | discount | member | txn_id | start_txn_time
  --- | --- | --- | --- | --- | --- | --- 
c4a632 | 4 | 13 | 17 | t | 54f307 | 2021-02-13 01:59:43.296
5d267b | 4 | 40 | 17 | t | 54f307 | 2021-02-13 01:59:43.296
b9a74d | 4 | 17 | 17 | t | 54f307 | 2021-02-13 01:59:43.296
2feb6b | 2 | 29 | 17 | t | 54f307 | 2021-02-13 01:59:43.296
c4a632 | 5 | 13 | 21 | t | 26cc98 | 2021-01-19 01:39:00.3456
e31d39 | 2 | 10 | 21 | t | 26cc98 | 2021-01-19 01:39:00.3456
72f5d4 | 3 | 19 | 21 | t | 26cc98 | 2021-01-19 01:39:00.3456
2a2353 | 3 | 57 | 21 | t | 26cc98 | 2021-01-19 01:39:00.3456
f084eb | 3 | 36 | 21 | t | 26cc98 | 2021-01-19 01:39:00.3456
c4a632 | 1 | 13 | 21 | f | ef648d | 2021-01-27 02:18:17.1648
  
</details>


<details>
  <summary>View Table: product_hierarchy</summary>

The `product_hierarchy` dataset will be used to answer the **Bonus Question** to recreate the `product_details` dataset.

id | parent_id | level_text | level_name
--- | --- | --- | --- 
1 | | Womens | Category
2 | | Mens | Category
3 | 1 | Jeans | Segment
4 | 1 | Jacket | Segment
5 | 2 | Shirt | Segment
6 | 2 | Socks | Segment
7 | 3 | Navy Oversized | Style
8 | 3 | Black Straight | Style
9 | 3 | Cream Relaxed | Style
10 | 4 | Khaki Suit | Style
11 | 4 | Indigo Rain | Style
12 | 4 | Grey Fashion | Style
13 | 5 | White Tee | Style
14 | 5 | Teal Button Up | Style
15 | 5 | Blue Polo | Style
16 | 6 | Navy Solid | Style
17 | 6 | White Striped | Style
18 | 6 | Pink Fluro Polkadot | Style
  
</details>

<details>
  <summary>View Table: product_prices</summary>
  
The `product_prices` dataset will be used to answer the **Bonus Question** to recreate the `product_details` dataset.
  
id | product_id | price
--- | --- | ---
7 | c4a632 | 13
8 | e83aa3 | 32
9 | e31d39 | 10
10 | d5e9a6 | 23
11 | 72f5d4 | 19
12 | 9ec847 | 54
13 | 5d267b | 40
14 | c8d436 | 10
15 | 2a2353 | 57
16 | f084eb | 36
17 | b9a74d | 17
18 | 2feb6b | 29

</details>




## Case Study Questions
Case Study #7 is categorized into 4 question groups\
To view the specific section, please open the link in a *`new tab`* or *`window`*.\
[A. High Level Sales Analysis](CaseStudy7_solutions.md#A)\
[B. Transaction Analysis](CaseStudy7_solutions.md#B)\
[C. Product Analysis](CaseStudy7_solutions.md#C)\
[D. Bonus Challenge](CaseStudy7_solutions.md#D)


## Solutions
- View `balanced_tree` database: [**here**](CaseStudy7_schema.sql)
- View Solution:
    - [**Markdown File**](CaseStudy7_solutions.md): offers a more fluid and responsive viewing experience
    - [**Jupyter Notebook**](CaseStudy7_solutions.ipynb): contains the original code


## PostgreSQL Topics Covered
- Establish relationships between datasets to create an Entity-Relationship Diagram (ERD)
- Common Table Expressions (CTE)
- Window Functions
- Subqueries
- JOIN, SELF JOIN
