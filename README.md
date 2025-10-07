# Netflix Data Engineering Pipeline

## Project Overview

This project implements an **end-to-end data pipeline** built with **PySpark** to process Netflix-like streaming data. It simulates a real-world data platform by ingesting multiple CSV datasets, transform into structured tables, and preparing them for analytic queries using **SQL**.

## Project Architecture

![Architecture](docs/pipeline.png)

## Data Model

![Gold Tables](docs/gold_tables.png)

## Aggregated Tables (Gold)

The **Gold layer** contains aggregated tables that are optimized for query tasks.

| Table Name | Description | Example Questions Answered |
|------------|-------------|----------------------------|
| `gold_content_performance` | Aggregates viewing and rating metrics for each movie or show | Which titles have the highest total views? What is the average rating per genre? Which content performs best over time? |
| `gold_subscription_revenue` | Tracks monthly subscription metrics and revenue trends derived from user sign-ups. | How much revenue was generated per month? What’s the net gain/loss in subscribers? How do subscription trends change over time? |
| `gold_user_engagement_profile` | Summarizes user-level engagement metrics such as total watch time, activity frequency, and content preferences. | Who are the most active users? How does engagement vary across regions or segments? What types of content drive the most user activity? |