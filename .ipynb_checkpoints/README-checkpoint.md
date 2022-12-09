# Udacity: Data Warehouse Project

## Introduction

The aim of this project is to create an etl process that can help to migrate a music streaming service (Sparkify) to the cloud (AWS). This is a music streaming service who have user activity along with song data and wish to grow to become an evil international tech giant. This project is performed using Amazon Redshift.

## How to Run

1. Establish an Amazon Redshift cluster where public access is allowed and inbound and outbound traffic is possible from worldwide.
2. Run **create_tables.py**: Drops and creates tables.
3. Run **etl.py**: From song_data and log_data, read, process and load files/data.

**Notes:**  
The dwh.cfg file should be configured with your access and cluster details.
Always do steps in order to reset your tables.

## File Introduction
### Datasets

#### Song data  
A subset of the million song dataset.  
JSON files containing song and artist metadata.  
First 3 letters of track ID is used for partitioning the data
#### Log Data
JSON files contianing simulated user activity data.  
year and month used to partition data.

### Project Files
**dwh.cfg**  
A configuration file containing the data to establish a connection to Amazon Redshift  
**create_tables.py**  
Establishes a Redshift connect then drops and creates staging and target tables needed to run etl.py  
**etl.py**  
Establishes a Redshift connection then reads and processes data from song_data and log_data into staging tables.  
Extracts from staging tables, transforms the data and loads to the target tables.  
**sql_queries.py**  
A collection of SQL queries used in this project.  
**README.md**  
This interesting discussion.  
**er.png**  
ER diagram used in this project.  

## Schema and ETL
![ER Diagram for Sparkify Project](er.png)

The ER diagram corresponding to the Staging Tables and Star Schema used in the current project is show above.  
The songplays fact table acts as a convenient and logical node between the given dimension tables.  
The dimension tables are well defined and separated allowing for a high degree of flexibility.  

The raw data is loaded from the S3 bucket to the song_raw and event_raw staging tables.
Data required for each of the target tables is extracted, transformed as required and loaded to the appropriate table.
Songplay records are extracted and completed with the aid of the event_raw and song_raw tables.  



