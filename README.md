# Automated ETL and Visualization of Weather Data from NCHMF Using Airflow and Superset

<img src="https://nchmf.gov.vn/KttvsiteE//images/banner-hd-en.jpg" alt="NCHMF" width="700"/>

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Setup Instructions](#setup-instructions)
- [Data Pipeline](#data-pipeline)
- [Usage](#usage)
- [Visualizations](#visualizations)
- [Conclusion](#concly)
- [Future Direction](#Future_Direction)

## Introduction

This project aims to automate the process of extracting, transforming, and loading (ETL) weather data from NCHMF, storing it in a PostgreSQL database, and visualizing the data using Apache Superset. The entire ETL workflow is managed by Apache Airflow, ensuring a scalable and reliable data pipeline.
## Features
- **Web Srapy:** Using BeautifulSoup to crawl weather data from the NCHMF website and store it in MongoDB as a datalake
- **Automated ETL Pipeline**: Using Apache Airflow to automate the process of collecting data into MongoDB, extracting it from MongoDB, transforming into a usable format, and loading it into a PostgreSQL database.
- **Data Visualization**: Leveraging Apache Superset to create interactive dashboards and visualizations for analyzing weather data everyday.
- **Docker Compose Setup**: The project uses Docker Compose to streamline the deployment and management of the required services, including Apache Airflow, PostgreSQL, Redis, MongoDB, and Apache Superset.
## Technologies Used
- **BeautifulSoup** : For web scraping weather data
- **Apache Airflow**: For managing and scheduling ETL workflows.
- **PostgreSQL**: To store transformed weather data.
- **Redis**: Provides caching to enhance performance.
- **MongoDB**: Optional, used for additional data storage.
- **Apache Superset**: For creating and managing data visualizations.
- **Docker Compose**: To orchestrate the deployment of the above technologies.

   
