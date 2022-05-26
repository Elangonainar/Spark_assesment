# Spark_assesment
This assesment is about a spark application to extract the data from the given URL and transform them accordingly to load the new data and updated data into Mysql Database.

**Repository contents**:
- Spark_assesment/spark_assesment/spark_network.py
- Spark_assesment/spark_assesment/Schema_Info.py
- Spark_assesment/spark_prerequest.sql
- Spark_assesment/sql_test.sql
- README.md

**Environment used for development and Pre-request**:
- spark-3.2.1-bin-hadoop3.2
- python 3.6.8
- Jdk 1.8
- Mysql 8.0.28
- pip install mysql-connector-python-rf
- pip install pandas
- Git pull the repository to local "https://github.com/Elangonainar/Spark_assesment.git"
- Run the spark_prerequest.sql file using Mysql developer tool(Mysql workbench) to create the Database and table objects.

**Steps to trigger the application**:
- The configuration parameters are in the file "Spark_assesment/spark_assesment/Schema_Info.py". Update the local machine Mysql user name and password. Host name has been configured as localhost and the Name of the database used is "DB1"
- Trigger the application by executing spark-submit command on the file "Spark_assesment/spark_assesment/spark_network.py"
           Sample command: > spark-submit D:\Spark_assesment\spark_assesment\spark_network.py
- The data should be loaded into the Mysql DB, once the job got completed. Check the data in database tables using the below queries.
          select * from users;
          select * from subscription;
          select * from message;
          
          
**About the application:**
This application extracts the Json data from the given URL and load that into a MySql Database. This data will be loaded into 3 different tables Users, Subscription and message. If there is any new data arrived while executing the application those data will be loaded into DB. The application also take care of existing data which came as a update, the updates are tracked using updateAt column in User table. Used md5 function to mask the PII data.


Also added a file "Spark_assesment/sql_test.sql". Open that file and run the query to get answers for the below question,

    •	How many total messages are being sent every day?
    •	Are there any users that did not receive any message?
    •	How many active subscriptions do we have today?
    •	Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
    •	Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?                                   



