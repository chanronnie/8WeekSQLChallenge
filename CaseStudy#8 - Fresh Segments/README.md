# Case Study #8: Fresh Segments</br>
[![View Main Folder](https://img.shields.io/badge/View-Main_Folder-F5788D.svg?logo=GitHub)](https://github.com/chanronnie/8WeekSQLChallenge)
[![View Case Study 8](https://img.shields.io/badge/View-Case_Study_8-07A092)](https://8weeksqlchallenge.com/case-study-8/)</br>
![8](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/4f340132-a725-4702-a865-808b623c467c)


The case study presented here is part of the **8 Week SQL Challenge**.\
It is kindly brought to us by [**Data With Danny**](https://8weeksqlchallenge.com).\
This time, I am using `PostgreSQL queries` (instead of MySQL) in `Jupyter Notebook`.


## Table of Contents
* [Entity Relationship Diagram](#entity-relationship-diagram)
* [Datasets](#datasets)
* [Case Study Questions](#case-study-questions)
* [Solutions](#solutions)
* [PostgreSQL Topics Covered](#postgresql-topics-covered)


## Entity Relationship Diagram
Again, I used the [DB Diagram tool](https://dbdiagram.io/home) to create the following ERD.
![Fresh Segments](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/1a9d92ec-c04b-44ed-9600-e98220789525)


<details>
<summary>View code to create ERD</summary>
  
Here is the code that I used for creating the ERD for the Fresh Segments datasets on DB Diagram tool

```markdown
TABLE interest_metrics {
  "_month" VARCHAR(4)
  "_year" VARCHAR(4)
  "month_year" VARCHAR(7)
  "interest_id" VARCHAR(5)
  "composition" FLOAT
  "index_value" FLOAT
  "ranking" INTEGER
  "percentile_ranking" FLOAT
}

TABLE interest_map {
  "id" INTEGER
  "interest_name" TEXT
  "interest_summary" TEXT
  "created_at" TIMESTAMP
  "last_modified" TIMESTAMP
  }


# Establish relationships between datasets
  REF: interest_metrics.interest_id > interest_map.id
```

</details>


## Datasets
The Case Study #8 contains 2 dataset:

### `interest_metrics`

<details>
  <summary>View Table: interest_metrics</summary>

The `interest_metrics` table presents aggregated interest metrics for a major client of Fresh Segments, reflecting the performance of interest_id based on clicks and interactions with targeted advertising content among their customer base.

_month | _year | month_year | interest_id | composition | index_value | ranking | percentile_ranking
--- | --- | --- | --- | --- | --- | --- | --- 
7 | 2018 | 07-2018 | 32486 | 11.89 | 6.19 | 1 | 99.86
7 | 2018 | 07-2018 | 6106 | 9.93 | 5.31 | 2 | 99.73
7 | 2018 | 07-2018 | 18923 | 10.85 | 5.29 | 3 | 99.59
7 | 2018 | 07-2018 | 6344 | 10.32 | 5.1 | 4 | 99.45
7 | 2018 | 07-2018 | 100 | 10.77 | 5.04 | 5 | 99.31
7 | 2018 | 07-2018 | 69 | 10.82 | 5.03 | 6 | 99.18
7 | 2018 | 07-2018 | 79 | 11.21 | 4.97 | 7 | 99.04
7 | 2018 | 07-2018 | 6111 | 10.71 | 4.83 | 8 | 98.9
7 | 2018 | 07-2018 | 6214 | 9.71 | 4.83 | 8 | 98.9
7 | 2018 | 07-2018 | 19422 | 10.11 | 4.81 | 10 | 98.63

</details>

### `interest_map`

<details>
  <summary>View Table: interest_map</summary>

The **`interest_map`** links the interest_id to the corresponding interest_name.

id | interest_name | interest_summary | created_at | last_modified
--- | --- | --- | --- | ---
1 | Fitness Enthusiasts | Consumers using fitness tracking apps and websites. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
2 | Gamers | Consumers researching game reviews and cheat codes. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
3 | Car Enthusiasts | Readers of automotive news and car reviews. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
4 | Luxury Retail Researchers | Consumers researching luxury product reviews and gift ideas. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
5 | Brides & Wedding Planners | People researching wedding ideas and vendors. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
6 | Vacation Planners | Consumers reading reviews of vacation destinations and accommodations. | 2016-05-26 14:57:59 | 2018-05-23 11:30:13
7 | Motorcycle Enthusiasts | Readers of motorcycle news and reviews. | 2016-05-26 14:57:59 | 2018-05-23 11:30:13
8 | Business News Readers	 | Readers of online business news content. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12
12 | Thrift Store Shoppers | Consumers shopping online for clothing at thrift stores and researching locations. | 2016-05-26 14:57:59 | 2018-03-16 13:14:00
13 | Advertising Professionals | People who read advertising industry news. | 2016-05-26 14:57:59 | 2018-05-23 11:30:12

  
</details>


## Case Study Questions
Case Study #8 is categorized into 4 question groups\
To view the specific section, please open the link in a *`new tab`* or *`window`*.\
[A. Data Exploration and Cleansing](CaseStudy8_solutions.md#A)\
[B. Interest Analysis](CaseStudy8_solutions.md#B)\
[C. Segment Analysis](CaseStudy8_solutions.md#C)\
[D. Index Analysis](CaseStudy8_solutions.md#D)


## Solutions
- View `fresh_segments` database: [**here**](CaseStudy8_schema.sql)
- View Solution:
    - [**Markdown File**](CaseStudy8_solutions.md): offers a more fluid and responsive viewing experience
    - [**Jupyter Notebook**](CaseStudy8_solutions.ipynb): contains the original code


## PostgreSQL Topics Covered
- Data Cleaning 
- Common Table Expressions (CTE)
- Window Functions (rolling average)
- Subqueries
- JOIN
