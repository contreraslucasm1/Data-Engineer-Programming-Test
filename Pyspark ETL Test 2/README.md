# Data Engineer Programming Test

The events.csv.gz dataset reflects the interactions between users of a communication service. Each record shows events that occurred during a whole hour between a pair of users (source and destination).

| Column | Description |
| --- | --- |
| id_source|ID of the source user (the one who initiates the events)
| id_destination|ID of the destination user
| region|Region of the country where the events were recorded
| date|Date (YYYYMMDD)
| hour|Hour of the day (0, ..., 23)
| calls|Number of calls between the users during that hour
| seconds|Total number of seconds that the calls made during that hour lasted (0 if calls=0)
| sms|Number of sms between the users during that hour

### Example: 

If on 01/20/2021 between 4:00 PM and 4:59 PM
ID A made 2 calls of 10 seconds each to ID B
ID B sent 3 sms to ID A
Then in the events dataset the following records will be observed:

id_source=A, id_destination=B, date=20210120, hour=16, calls=2, seconds=20,
sms=0, ...
id_source=B, id_destination=A, date=20210120, hour=16, calls=0, seconds=0,
sms=3, ...
Those records with null id_source or id_destination must be discarded.

The free_sms_destinations.csv.gz dataset contains the IDs of the users to whom sms can be sent for free (free destinations).
Sms are always billed to the source user as follows:

$0.0 if the destination is free
$1.5 if the event is recorded in regions 1 to 5
$2.0 if the event is recorded in regions 6 to 9

##Tasks:

Calculate the total amount that the service provider will bill for sms deliveries.

Generate a dataset that contains the IDs of the 100 users with the highest billing for sms delivery and the total amount to be billed to each one. 

In addition to the ID, include the ID hashed using the MD5 algorithm. Write the dataset in parquet format with gzip compression.

Plot a histogram of the number of calls made per hour of the day.

##Suggestion: 

Solve in a notebook (Zeppelin, Jupyter) using Spark or PySpark.
Attach the used notebook/code, the total amount from point 1, the dataset generated in point 2 and the histogram in PNG format from point 3.


##Run Solution
Hi,

Please implement the solution using the jupyter/pyspark-notebook Docker image.

To run it in the terminal, run the following command:


```docker run -it --rm -p 8888:8888 -v {REPLACE THIS WITH THE PATH TO THE FOLDER CONTAINING THE NOTEBOOK AND CSV FILES}:/home/jovyan/work jupyter/pyspark-notebook```

Access the link that appears in the terminal, open the notebook, and run.
