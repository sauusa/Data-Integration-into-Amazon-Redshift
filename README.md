# Data-Integration-into-Amazon-Redshift
Sample project for Data Intergraion/Migration from On-premise to Amazon Cloud (Redshift)

## Getting Data into Amazon cloud, options:

-  Multi-part upload (in-parallel processing, using S3)

-  VPN (creating virtual private network for direct connection)

-  Direct Connect (Connect the On-premise and AWS directly for data transfer)

-  Import / Export (Using AWS tools to dump and load the data)
<img src = "https://user-images.githubusercontent.com/67065902/97900386-90ba3b80-1d08-11eb-8d2b-74b530f6aec2.png" />

Note: We will use the cloud native intergration approach for this sample project:

## Native Integration: Loading the Data directly from S3 to Redshift
<img src="https://user-images.githubusercontent.com/67065902/97900464-a7609280-1d08-11eb-9914-41caacac6eb4.png"/>

# Steps:
Note: This is a sample project, actual project includes a lot more additional small steps in process.

#### 1. Export the data from on-premis (postgres):

##### For the whole DB copy:
Export:
<font color="blue"> psql$ pg_dump --no-owner sample_db > dump.sql </font>

(pg_dump is a utility for backing up a PostgreSQL database. 
It makes consistent backups even if the database is being used concurrently. 
pg_dump does not block other users accessing the database (readers or writers). 
pg_dump only dumps a single database.)

dump.sql file has all the sql scripts to create schema and data from scratch.
this file can be used elsewhere to create a copy of your exisitng database.

Import:

<font color="blue"> psql$: CREATE DATABASE sample_db_new </font>
<font color="blue"> psql$: psql sample_db_new < dump.sql </font>

##### For exporting specific table into csv file:

- Use PGAdmin4

- Chose particular table > right click & select query tool

- Write the query for export, for example:
  select * from public.sample_table where public.sample_table.field >= 5000 and public.sample_table.field < 10000
  
  COPY public.sample_table TO 'D:\MyDBOutput.csv' DELIMITER ',' CSV HEADER;

 - If the file is too big, repartition the files using Python, Shell or Windows CMD script
   check split-csv-file.py
   
   or use command line:
 $ split -l 10000 MyDBOutput.csv

#### 2. Load the data to Amazon (S3):

 - Login to AWS, create a bucket in S3
 
 - Use the AWS CLI to connect to your account (using secret key)
 
 - Use the following script to copy your files to S3 bucket:
 
<font color="blue"> $ aws s3 cp --recursive <localfolderpath> s3://<bucketname>/<key>/ </font>

(you can also synchronize the input and output folders for better accessibility:
<font color="blue"> $ aws s3 sync . s3://my-bucket/path </font>

#### 3. Create AWS Redshift schema for the upload:

(Amazon Redshift is a data warehouse product which forms part of the larger cloud-computing platform Amazon Web Services.)

  - Create a Redshift cluster
  (follow the document for additional steps: https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html )
  
  - Connect to Redshift cluster
  
  - Create Tables for upload:
    With No distribution strategy (NoDist Schema)
    WIth Distribution strategy (Disp Schema)
    
    check create-tables.py
    
#### 4. Create ETL Pipelines for Data transfer from S3 to Redshift:
    
    check etl.py and sql_queries.py
    
#### 5. Use EMR Spark for aditional Data Transformation(if required):

    check emr-spark.py
    
Once the Data Integration is successful, you will be able to see the results on Redshift, by simpling using sql queries.

### Additional Step for BI / Data Analytics

    Extract the result output from Redshift, Import the file to Microsoft Power BI, and perform the DAX.
    
    or
    
    Open Tableau.
    
    Connect to Redshift cluster and database.
    
    Pull the tables.
    
    Perform the BI operations. Like below:
    
<img src="https://user-images.githubusercontent.com/67065902/98997505-9aab1e00-2502-11eb-95d5-356c0a1ad7d6.png" />
