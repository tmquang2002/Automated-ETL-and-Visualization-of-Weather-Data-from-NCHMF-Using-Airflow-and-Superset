# Automated ETL and Visualization of Weather Data from NCHMF Using Airflow and Superset

<img src="https://nchmf.gov.vn/KttvsiteE//images/banner-hd-en.jpg" alt="NCHMF" width="900"/>

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

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/tmquang2002/Automated-ETL-and-Visualization-of-Weather-Data-from-NCHMF-Using-Airflow-and-Superset.git
   cd Automated-ETL-and-Visualization-of-Weather-Data-from-NCHMF-Using-Airflow-and-Superset.git
    ```bash
    - Install superset.
    ```bash
   git clone https://github.com/apache/superset.git

2. **Configuration**

- **Airflow Configuration**

  - Edit the file `airflow/dags/ETL.py` to configure your ETL tasks.

- **Superset Configuration**

  - Configure the connection to your PostgreSQL database in the Superset web interface after deployment.

- **Docker Compose Configuration**

  - Modify `docker-compose.yml` if necessary to suit your environment.

3.  **Start the Environment**

```bash
docker compose up -d
cd superset
docker compose -f docker-compose-non-dev.yml up #to run Apache Superset
```
## Data Pipeline
![image](https://i.ibb.co/7jd8Ykg/ETL-process.png)

- **Scrapy**: Collects weather data from the NCHMF website using BeautifulSoup and stores it in a MongoDB database as a data lake.
- **Extraction**: Airflow DAG fetches weather data from the MongoDB database.
- **Transformation**: Data is cleaned and transformed into a format suitable for analysis.
- **Loading**: Transformed data is loaded into a PostgreSQL database as Data WareHouse for further analysis.
- **Scheduling**: Airflow schedules regular updates of the data pipeline.
- **Email**: Sends an email notification to users with updated weather information specific to their local area.
- **Analysis**: Analyzes the weather data, providing detailed insights into daily temperature, humidity, wind direction, and other weather indicators.

## Usage
### Raw Data
- Latest weather information.
![image](https://i.ibb.co/wKPfrKn/wdinfo.png)

- Temperature information for the past 10 days.
![image](https://i.ibb.co/BB7DM6j/wdseries.png)

### Access Airflow and Run ETL task

- Open [http://localhost:8081](http://localhost:8081) in your browser.
- Default credentials: `airflow` / `airflow`.
- Run ETL
  
![image](https://i.ibb.co/YQ3t1p5/airflow.png)
![image](https://i.ibb.co/LCPkBC6/dag.png)

- After ETL is done, receive email about the latest weather information in your subscribed area.
  
  ![image](https://i.ibb.co/KFKCJ1H/sendmail.png)

### Access Superset

- Open [http://localhost:8088](http://localhost:8088) in your browser.
- Default credentials: `admin` / `admin`.

### Explore Data

- In Superset, connect to the PostgreSQL database and use the available datasets to create charts and dashboards.

![image](https://i.ibb.co/C97XzRx/superset.png)

## Visualizations

- Insert additional information about iso_codes to support map visualization.
  ![image](https://i.ibb.co/R6JL0pC/bd0.png)
  
- Temperature and humidity chart of the provinces.
  ![image](https://i.ibb.co/8Kk4bF9/bd1.png)
  
- Temperature chart for the past 10 days of some provinces.
  ![image](https://i.ibb.co/kxC2whN/bd2.png)

## Conclusion
- This project successfully automated the process of extracting, transforming, and loading (ETL) weather data from the NCHMF website, storing it in a MongoDB, tranform and load to PostgreSQL database, visualizing the data using Apache Superset. By leveraging Apache Airflow, the ETL pipeline ensures a scalable and reliable system that consistently updates weather information. The use of Docker Compose has streamlined the deployment of services such as Airflow, PostgreSQL, MongoDB, and Apache Superset, making the setup portable and easy to manage. Additionally, users receive timely email notifications with localized weather data, enhancing the project's practicality.
## Future Direction
- Future improvements include integrating more data sources, applying machine learning for weather predictions, enabling real-time data updates, and adding personalized notifications.
## Contact

For any questions or feedback, please reach out to [tmquang120202@gmail.com](mailto:tmquang120202@gmail.com).
   
