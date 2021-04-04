# Project: Data Lake
##### _README file_
&nbsp;
### What it does?
This package uses Spark to read song and events data from AWS S3 buckets, process the data, and then upload it back to S3.


### How to use?

- Run _`etl.py`_ , which performs the ETL process. This will load data for the song and events data from S3, and create parquet out of these data, then upload them back to S3.
- It will generate below five parquet/tables 

  - songplays: stores users' music listening records which is extracted from log data
  - songs: stores song information which is extracted from song data
  - artists: stores artist information which is extracted from song data
  - users: stores user information which is extracted from log data
  - time: parsed time information which is extracted from timestamp from log data
