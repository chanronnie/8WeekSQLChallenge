# Case Study #1: Danny's Diner 🍥 
[![View Main Folder](https://img.shields.io/badge/View-Main_Folder-F5788D.svg?logo=GitHub)](https://github.com/chanronnie/8WeekSQLChallenge)
[![View Case Study 1](https://img.shields.io/badge/View-Case_Study_1-68032C)](https://8weeksqlchallenge.com/case-study-1/)</br>
![1](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/3ebb1080-b8d3-4381-850d-4a003cc9476d)

The case study presented here is part of the **8 Week SQL Challenge**.\
It is kindly brought to us by [**Data With Danny**](https://8weeksqlchallenge.com).\
I use `MySQL queries` in `Jupyter Notebook` to quickly view results.



## Table of Contents
* [Entity Relationship Diagram](#entity-relationship-diagram)
* [Datasets](#datasets)
* [Case Study Questions](#case-study-questions)
* [Solutions](#solutions)


## Entity Relationship Diagram
![Danny's Diner](https://github.com/chanronnie/8WeekSQLChallenge/assets/121308347/d71bffd1-6513-456c-9686-d95dbf1eeaaf)

## Datasets
The Case Study #1 contains 3 tables:
- **sales**: This table captures all the order information (`order_date` and `product_id`) of each customer (`customer_id`).
- **menu**: This table lists the IDs, names and prices of each menu item.
- **members**: This table captures the dates (`join_date`) when each customer joined the member program.

## Case Study Questions 
1. What is the total amount each customer spent at the restaurant?
2. How many days has each customer visited the restaurant?
3. What was the first item from the menu purchased by each customer?
4. What is the most purchased item on the menu and how many times was it purchased by all customers?
5. Which item was the most popular for each customer?
6. Which item was purchased first by the customer after they became a member?
7. Which item was purchased just before the customer became a member?
8. What is the total items and amount spent for each member before they became a member?
9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?


## Solutions
- View `dannys_diner` database: [**here**](CaseStudy1_schema.sql)
- View Solution: [**here**](CaseStudy1_solution.ipynb)

