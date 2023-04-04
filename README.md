# STEDI Human Balance Analytics

## 1. Project description
In this project, I am going to build a data lakehouse solution for sensor data that trains a **machine learning model**.

### 1.1. About STEDI Step Trainer
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

### 1.2 Project objectives

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. **Privacy** will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

As a data engineer on the STEDI Step Trainer team, I will extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that *Data Scientists* can train the learning model by creating 05 **AWS Glue Jobs**.

## 2. Files

**`customer_landing.sql`** SQL queries for creating a Glue table for (S3) customer landing zone.

**`customer_landing.png`** A screenshot of the resulting data from querying **customer_landing** table using Athena.
 
**`accelerometer_landing.sql`** SQL queries for creating a Glue table for (S3) accelerometer landing zone.

**`accelerometer_landing.png`** A screenshot of the resulting data from querying **accelerometer_landing** table using Athena.

**`customer_landing_to_trusted.py`** A Python script using Spark that sanitizes the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **customer_trusted**.

**`customer_trusted.png`** A screenshot of the resulting data from querying **customer_trusted** table using Athena. The screenshot shows that Glue job is successful and only contains Customer Records from people who agreed to share their data.

**`accelerometer_landing_to_trusted.py`** A Python script using Spark that sanitizes the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **accelerometer_trusted**.

**`customer_trusted_to_curated.py`** A Python script using Spark that sanitizes the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called **customers_curated**.

**`step_trainer_landing_to_trusted.py`** A Python script using Spark that read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called **step_trainer_trusted** that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

**`machine_learning_curated.py`** A Python script using Spark that creates an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called **machine_learning_curated**.
