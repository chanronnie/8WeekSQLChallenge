# Case Study #6: Clique Bait 🪝
![6](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/364b11b1-7a4b-47d9-b7a6-6fd87075d140)


The case study presented here is part of the **8 Week SQL Challenge**.\
It is kindly brought to us by [**Data With Danny**](https://8weeksqlchallenge.com).

This time, I am using `PostgreSQL queries` (instead of MySQL) in `Jupyter Notebook` to quickly view results, which provides me with an opportunity 
  - to learn PostgreSQL
  - to utilize handy mathematical and string functions.


🪝 View **Case Study #6**: [**here**](https://8weeksqlchallenge.com/case-study-6/).

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
- **users**: This table shows all the user_id along with their unique cookie_id (PK).
- **events**: The table lists all the website interactions by customers (cookie_id, webpage visited, number of visits, etc.).
- **event_identifier**: The table maps the event_type to its corresponding event_name.
- **campaign_identifier**: The table lists the information of the three campaigns that ran on the Clique Bait website.
- **page_hierarchy**: The table maps the page_id to its page_name.


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
