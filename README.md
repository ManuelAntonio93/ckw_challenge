## Objective
Build a near real-time data pipeline using PySpark to read, validate, 
and store measurement values from electricity meters.

## Brief
Welcome to the CKW AG coding challenge! Your task is to build a data 
pipeline that processes measurement values from electricity meters in
near real-time. The data will be read from a Kafka stream, processed, 
and stored in a table. Additionally, you will need to model and validate
this data.

Don't hesitate to contact us via Code Submit or Email, if something is
completely unclear for you. Read carefully through the hole README file
and don't be afraid to make some assumptions if something is not clearly
described in your opinion. 

Don't worry if you cannot solve all the tasks. Solve as much as you can
and don't forget to push regularly to the branch you just cloned. After
your final push, submit the code in the Browser UI of Code Submit by
clicking the submit button.

Happy coding! :-)



## Instructions for Setting Up Local Development Environment

### Install the python requirements
1. Install the required python libraries by running
  `pip install -r requirements.txt` in your desired python environment.



### Docker Set Up
In the following points, it is explained how you can start up two docker
containers **which are already preapared and ready to use for you (you
don't have to develop them).** The one container holds a simple kafka
broker that you going to read messages from later in your first task.
The second container holds a very simple python script that produces
every 10 seconnds some measurements and sends them to the kafka broker.

### Set up the docker container holding the Kafka broker

1. Make sure you have Docker version 24.0.5, build ced0996, installed
  in your environment (for Linux / WSL you can use 
  `sudo snap install docker` if you have the sudo service currently
  installed or check the internet for a suitable documentation based on
  your operating system). **It is important to use the specified version
  otherwise the Docker config used afterwards could have conflicts.**
1. Run `sudo docker-compose up -d` to build and run the docker container
  holding the kafka broker.
1. Run `sudo docker ps` to see all running containers. There should be the
    following two containers (NAMES):
    - electric-measurestream-<random_string>-kafka-1
    - electric-measurestream-<random_string>-zookeeper-1

### Set up the docker container holding the measurement producer

1. Check the IP Adress of the electric-measurestream-<random_string>-
  kafka-1 container by running
  `sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' electric-measurestream-<random_string>-kafka-1` **replacing the
  <random_string> place holder with the random string of your working
  directory.**
1. Make sure that in line 33 of the *producer.py* the
  *'bootstrap.servers'* property is set to *"<ip_of_broker>:9093"*
1. Start the *producer.py* in a separate docker container running the
  following commands:
    - `sudo docker build -t myproducer .`
    - `sudo docker run -d --network=host myproducer`

### Test you docker set up

1. Finally you can test your setup by running the
  *test_your_docker_setup/reader.py* in a python console. Before you
  run the file, make sure that in line 5 the *'bootstrap.servers'*
  property is set to *"<ip_of_broker>:9093"*.
1. **If you did not succeed in running this docker containers locally
  you can still coplete all the tasks. You can still complete task 1 by
  completing the file *exercises/consumer.py* but when running the file,
  it will fail since there is no Kafka stream to read from. But there is
  already some data from the stream stored at *datastore/consumer_data*
  in parquet format that you can use for the remaining tasks.**



### Install spark in your local environment
Before you can now finally start with solving your tasks, you have to
make sure that you have a local spark installation so that you can
use the pyspark library in the tasks

1. Check the internet for a suitable documentation based on your
  operating system
2. **If you do not succeed with installing spark locally, you can still
  complete the files in the exercise folder. The disadvantage is that
  you will not be able to run the files to test if your code works**
      


## Your tasks

### Task 1: Consuming the Kafka Stream
- Set up a Kafka consumer in the file *exercises/consumer.py* to read
  measurement values from the Kafka stream.
    - Usefull settings like the *bootstrap.servers* or the *topic* can
    be found in the file *test_your_docker_setup/reader.py*
    - Further set the option *startingOffsets* to *earliest* and
    *failOnDataLoss* to *false*.
    - The data you receive will be in the following form:

      | key | value | topic | partition | offset | timestamp | timestampType |
      | -------- | -------- | -------- | -------- | -------- | -------- | -------- |
      |NULL|[7B 22 74 5F 6D 6...|mytopic|        0|     0|2024-05-22 11:14:...|            0|
      |NULL|[7B 22 74 5F 6D 6...|mytopic|        0|     1|2024-05-22 11:14:...|            0|
      |NULL|[7B 22 74 5F 6D 6...|mytopic|        0|     2|2024-05-22 11:14:...|            0|
      |NULL|[7B 22 74 5F 6D 6...|mytopic|        0|     3|2024-05-22 11:14:...|            0|
      |NULL|[7B 22 74 5F 6D 6...|mytopic|        0|     4|2024-05-22 11:14:...|            0|
    - Note that all columns except *value* is generated by the Kafka
    Stream by default. The measurements itself are binary encoded in the
    *value* column. If you want to know how the single measurements
    within the *value* column look like after you parse them correctly,
    have a look at the file *example_data/measurement_values.json*.
    In every row, the value column will hold one of the json-structs
    you see in the list of structs in the mentioned example file.
- Write the data to the location *datastore/consumer_data* in parquet
  format.
    - The data should be stored with the same structure you receive it
    from the stream.
    - Use the location *datastore/checkpoint_consumer* for the 
    checkpoint meta data.
    - Further set the *outputMode* to *append* and make sure you pass
    the property *availableNow* set to *true* to the trigger of the 
    *writeStream* method.


### Task 2: Data Processing
- In this task you will work in the file *exercises/processor.py*. The
  goal is that you load the previously saved raw data  from the consumer
  and you process it.
  - You should only keep the *value* column of this table. The entries
  should be parsed to the correct struct format you see in the example
  file *example_data/measurement_values.json*.
- In a second step, you should flatten the table in a way, that all the
  initial struct properties (*t_measurement*, *t_report*, *device_id*,
  *signal_name*, *value*) are represented as single columns in the flat
  table.
- Store this flat table in parquet format at the location
  *datastore/processed_data*.
- Note: For simplicity you can load the whole cosumer_data table and use
  the mode overwrite when saving the flat table in order to not take
  care of incremental loads.

### Task 3: Data Cleaning
- In this task you will load the processed_data from the previous step
  and clean it. To do so, work in the *exercises/cleaner.py* file.
- Maybe you have already realized, that there are some "holes" in the
  data. This means that for all 4 timeseries (device1_export,
  device1_import, device2_export, device2_import) we expect a value
  every 10 seconds. From the source, we don't necessarily get a value
  every 10 seconds (this can have various reasons).
- Clean the processed_data so that you fill these holes for all 4
  timeseries in a reasonable way (you can decide with what strategy you
  do this interpolation per timeseries).
- Finally save the cleaned data under *datastore/cleaned_data*.
- Note: For simplicity, do again a full load of the data and store it
  with the overwrite mode.

### Bonus Task: Data Checks
- If you could solve all the previous tasks and still want to continue
  hacking, you can also solve this bonus task ;-) But it is also fine,
  if you skip this task, it will not influence your evaluation in a 
  negative way.
- Use the *exercises/checker.py* file to create some reasonable data 
  checks that you perform on the cleaned_data.
- Please decide on your own, what data checks could be valuable to 
  perform on the cleaned time series data.
      


## Evaluation Criteria

- **Correctness**: Does the pipeline correctly read, process, clean,
  and potentially check the data?
- **Efficiency**: Is the code written in a efficient way in terms of
  data processing?
- **Code Quality**: Is the code well-organized and documented?
- **Error Handling**: Does the pipeline gracefully handle errors and
  edge cases?




## CodeSubmit 

Please organize, design, test, and document your code as if it were
going into production - then push your changes to the master branch.

Have fun coding! 🚀

The CKW AG Team